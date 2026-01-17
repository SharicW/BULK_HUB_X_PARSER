// parser.js (ESM)
// Node 18+ (у тебя Node 24 ок)

import dotenv from "dotenv";
dotenv.config();

import { Pool } from "pg";

const BASE_URL = process.env.TWITTERAPI_BASE_URL || "https://api.twitterapi.io";

const API_KEY = process.env.TWITTERAPI_IO_KEY;
const COMMUNITY_ID = process.env.COMMUNITY_ID;
const DATABASE_URL = process.env.DATABASE_URL;

// free-tier часто 1 запрос / 5 секунд -> ставим 5200-6000мс
const MIN_REQUEST_INTERVAL_MS = Number(process.env.MIN_REQUEST_INTERVAL_MS || 5200);

// сколько страниц newest-листы смотреть (ingest-new)
const TOP_PAGES = Number(process.env.TOP_PAGES || 3);

// сколько часов считать “recent tweets” для обновления метрик
const RECENT_HOURS = Number(process.env.RECENT_HOURS || 48);

// сколько часов считать “active posters” для refresh-users
const ACTIVE_HOURS = Number(process.env.ACTIVE_HOURS || 24);

// пачка tweet_ids для /twitter/tweets
const BATCH_TWEET_IDS = Number(process.env.BATCH_TWEET_IDS || 80);

// backfill cutoff (0 = без cut)
const BACKFILL_CUTOFF_DAYS = Number(process.env.BACKFILL_CUTOFF_DAYS || 0);

// сколько страниц backfill делать за один запуск (чтобы не висеть вечность)
const BACKFILL_PAGES_PER_RUN = Number(process.env.BACKFILL_PAGES_PER_RUN || 50);

// SSL для Railway Postgres (обычно нужно)
const PGSSL = (process.env.PGSSL || "").toLowerCase() === "true";

if (!API_KEY) console.warn("WARN: TWITTERAPI_IO_KEY is missing");
if (!COMMUNITY_ID) console.warn("WARN: COMMUNITY_ID is missing");
if (!DATABASE_URL) console.warn("WARN: DATABASE_URL is missing");

function die(msg) {
  console.error("FATAL:", msg);
  process.exit(1);
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

// ---------- API client with strict pacing + retries ----------
class TwitterApiIO {
  constructor({ baseUrl, apiKey, minIntervalMs }) {
    this.baseUrl = baseUrl.replace(/\/$/, "");
    this.apiKey = apiKey;
    this.minIntervalMs = minIntervalMs;
    this._lastTs = 0;
  }

  async _pace() {
    const now = Date.now();
    const wait = this._lastTs + this.minIntervalMs - now;
    if (wait > 0) await sleep(wait);
    this._lastTs = Date.now();
  }

  async get(path, params = {}, { retries = 6 } = {}) {
    if (!this.apiKey) die("No TWITTERAPI_IO_KEY");
    await this._pace();

    const url = new URL(this.baseUrl + path);
    for (const [k, v] of Object.entries(params)) {
      if (v === undefined || v === null || v === "") continue;
      url.searchParams.set(k, String(v));
    }

    let attempt = 0;
    while (true) {
      attempt++;
      const res = await fetch(url, {
        method: "GET",
        headers: { "X-API-Key": this.apiKey },
      });

      // 429 handling
      if (res.status === 429 && attempt <= retries) {
        let waitMs = this.minIntervalMs + attempt * 1000; // растём
        try {
          const j = await res.json();
          // иногда там msg типа "one request every 5 seconds"
          console.warn("API 429:", j?.message || j?.msg || "Too Many Requests");
        } catch {}
        console.warn(`429 -> retry in ${waitMs}ms (attempt ${attempt}/${retries})`);
        await sleep(waitMs);
        await this._pace();
        continue;
      }

      let data;
      const text = await res.text();
      try {
        data = text ? JSON.parse(text) : {};
      } catch {
        data = { raw: text };
      }

      if (!res.ok) {
        const msg = data?.message || data?.msg || `HTTP ${res.status}`;
        throw new Error(`${msg} @ ${url.pathname}`);
      }

      // twitterapi.io обычно возвращает {status:"success"...}
      if (data?.status && data.status !== "success") {
        throw new Error(`${data?.msg || "API status error"} @ ${url.pathname}`);
      }

      return data;
    }
  }

  // docs:
  // GET /twitter/community/tweets?community_id=...&cursor=...
  getCommunityTweets({ community_id, cursor }) {
    return this.get("/twitter/community/tweets", { community_id, cursor });
  }

  // GET /twitter/tweets?tweet_ids=comma,separated
  getTweetsByIds(tweet_ids) {
    return this.get("/twitter/tweets", { tweet_ids: tweet_ids.join(",") });
  }

  // GET /twitter/user/info?userName=...
  getUserInfo(userName) {
    return this.get("/twitter/user/info", { userName });
  }

  // GET /twitter/community/members?community_id=...&cursor=...
  getCommunityMembers({ community_id, cursor }) {
    return this.get("/twitter/community/members", { community_id, cursor });
  }
}

// ---------- DB ----------
const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: PGSSL ? { rejectUnauthorized: false } : undefined,
});

async function q(sql, params) {
  return pool.query(sql, params);
}

async function ensureStateRow(communityId) {
  await q(
    `
    INSERT INTO ingest_state (community_id, backfill_cursor, last_seen_tweet_id)
    VALUES ($1, NULL, NULL)
    ON CONFLICT (community_id) DO NOTHING
  `,
    [communityId]
  );
}

async function getState(communityId) {
  await ensureStateRow(communityId);
  const r = await q(
    `SELECT community_id, backfill_cursor, last_seen_tweet_id, updated_at
     FROM ingest_state WHERE community_id=$1`,
    [communityId]
  );
  return r.rows[0];
}

async function setBackfillCursor(communityId, cursor) {
  await q(
    `UPDATE ingest_state
     SET backfill_cursor=$2, updated_at=now()
     WHERE community_id=$1`,
    [communityId, cursor]
  );
}

async function setLastSeenTweetId(communityId, tweetId) {
  await q(
    `UPDATE ingest_state
     SET last_seen_tweet_id=$2, updated_at=now()
     WHERE community_id=$1`,
    [communityId, tweetId]
  );
}

function parseCreatedAt(s) {
  if (!s) return null;
  const d = new Date(s);
  if (Number.isNaN(d.getTime())) return null;
  return d.toISOString(); // pg нормально примет ISO
}

function normalizeTweet(t) {
  const tweet_id = String(t.id);
  const url = t.url || null;
  const text = t.text || "";
  const created_at = parseCreatedAt(t.createdAt);
  const author_user_id = t?.author?.id ? String(t.author.id) : null;
  const author_username = t?.author?.userName || t?.author?.username || null;

  return {
    tweet_id,
    url,
    text,
    created_at,
    author_user_id,
    author_username,
  };
}

async function upsertCommunityTweet(communityId, tw) {
  await q(
    `
    INSERT INTO community_tweets
      (community_id, tweet_id, created_at, author_user_id, author_username, url, text)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (community_id, tweet_id) DO UPDATE SET
      created_at=COALESCE(EXCLUDED.created_at, community_tweets.created_at),
      author_user_id=COALESCE(EXCLUDED.author_user_id, community_tweets.author_user_id),
      author_username=COALESCE(EXCLUDED.author_username, community_tweets.author_username),
      url=COALESCE(EXCLUDED.url, community_tweets.url),
      text=COALESCE(EXCLUDED.text, community_tweets.text)
  `,
    [
      communityId,
      tw.tweet_id,
      tw.created_at,
      tw.author_user_id,
      tw.author_username,
      tw.url,
      tw.text,
    ]
  );
}

async function upsertMetrics(m) {
  await q(
    `
    INSERT INTO tweet_metrics_latest
      (tweet_id, view_count, like_count, retweet_count, reply_count, quote_count, bookmark_count, updated_at)
    VALUES
      ($1,$2,$3,$4,$5,$6,$7, now())
    ON CONFLICT (tweet_id) DO UPDATE SET
      view_count=EXCLUDED.view_count,
      like_count=EXCLUDED.like_count,
      retweet_count=EXCLUDED.retweet_count,
      reply_count=EXCLUDED.reply_count,
      quote_count=EXCLUDED.quote_count,
      bookmark_count=EXCLUDED.bookmark_count,
      updated_at=now()
  `,
    [
      String(m.id),
      Number(m.viewCount || 0),
      Number(m.likeCount || 0),
      Number(m.retweetCount || 0),
      Number(m.replyCount || 0),
      Number(m.quoteCount || 0),
      Number(m.bookmarkCount || 0),
    ]
  );
}

async function upsertUser(u) {
  const user_id = u?.id ? String(u.id) : null;
  const username = u?.userName || null;
  const name = u?.name || null;
  const followers = Number(u?.followers || 0);
  const following = Number(u?.following || 0);
  const profile_picture = u?.profilePicture || null;

  if (!user_id || !username) return;

  await q(
    `
    INSERT INTO users (user_id, username, name, followers, following, profile_picture, updated_at)
    VALUES ($1,$2,$3,$4,$5,$6, now())
    ON CONFLICT (user_id) DO UPDATE SET
      username=EXCLUDED.username,
      name=EXCLUDED.name,
      followers=EXCLUDED.followers,
      following=EXCLUDED.following,
      profile_picture=EXCLUDED.profile_picture,
      updated_at=now()
  `,
    [user_id, username, name, followers, following, profile_picture]
  );
}

// ---------- Commands ----------
const api = new TwitterApiIO({
  baseUrl: BASE_URL,
  apiKey: API_KEY,
  minIntervalMs: MIN_REQUEST_INTERVAL_MS,
});

async function doctor() {
  if (!API_KEY || !COMMUNITY_ID || !DATABASE_URL) die("Missing env vars (key/community_id/database_url)");

  await q("SELECT 1;");
  const data = await api.getCommunityTweets({ community_id: COMMUNITY_ID });
  const n = Array.isArray(data?.tweets) ? data.tweets.length : 0;
  console.log(`DB: OK`);
  console.log(`API: OK (/community/tweets returned ${n} tweets)`);
}

async function backfill() {
  const communityId = COMMUNITY_ID;
  if (!communityId) die("No COMMUNITY_ID");

  const state = await getState(communityId);
  let cursor = state.backfill_cursor || null;

  const cutoffTs =
    BACKFILL_CUTOFF_DAYS > 0 ? Date.now() - BACKFILL_CUTOFF_DAYS * 24 * 3600 * 1000 : null;

  let insertedTotal = 0;

  for (let page = 1; page <= BACKFILL_PAGES_PER_RUN; page++) {
    const data = await api.getCommunityTweets({ community_id: communityId, cursor });
    const tweets = Array.isArray(data?.tweets) ? data.tweets : [];

    let insertedThisPage = 0;
    let reachedCutoff = false;

    for (const t of tweets) {
      const tw = normalizeTweet(t);

      // cutoff check
      if (cutoffTs && tw.created_at) {
        const ts = new Date(tw.created_at).getTime();
        if (!Number.isNaN(ts) && ts < cutoffTs) {
          reachedCutoff = true;
          break;
        }
      }

      await upsertCommunityTweet(communityId, tw);
      insertedThisPage++;
      insertedTotal++;
    }

    console.log(
      `backfill page=${page} insertedTotal=${insertedTotal} insertedThisPage=${insertedThisPage} cursor=${cursor ? "..." : "<null>"}`
    );

    if (reachedCutoff) {
      console.log(`backfill stop: reached cutoff (${BACKFILL_CUTOFF_DAYS} days)`);
      await setBackfillCursor(communityId, null);
      break;
    }

    if (!data?.has_next_page || !data?.next_cursor) {
      console.log("backfill stop: no next page");
      await setBackfillCursor(communityId, null);
      break;
    }

    cursor = data.next_cursor;
    await setBackfillCursor(communityId, cursor);
  }
}

async function ingestNew() {
  const communityId = COMMUNITY_ID;
  if (!communityId) die("No COMMUNITY_ID");

  const state = await getState(communityId);
  const stopId = state.last_seen_tweet_id || null;

  let cursor = null;
  let newLastSeen = null;
  let inserted = 0;
  let stopped = false;

  for (let page = 1; page <= TOP_PAGES; page++) {
    const data = await api.getCommunityTweets({ community_id: communityId, cursor });
    const tweets = Array.isArray(data?.tweets) ? data.tweets : [];
    if (page === 1 && tweets[0]?.id) newLastSeen = String(tweets[0].id);

    for (const t of tweets) {
      const id = String(t.id);
      if (stopId && id === stopId) {
        stopped = true;
        break;
      }
      const tw = normalizeTweet(t);
      await upsertCommunityTweet(communityId, tw);
      inserted++;
    }

    console.log(`ingest-new page=${page} inserted=${inserted} stopped=${stopped}`);

    if (stopped) break;
    if (!data?.has_next_page || !data?.next_cursor) break;
    cursor = data.next_cursor;
  }

  if (newLastSeen) {
    await setLastSeenTweetId(communityId, newLastSeen);
  }
}

async function refreshMetrics({ hours = RECENT_HOURS, all = false } = {}) {
  const communityId = COMMUNITY_ID;
  if (!communityId) die("No COMMUNITY_ID");

  const where = all
    ? `ct.community_id = $1`
    : `ct.community_id = $1 AND ct.created_at >= now() - ($2 || ' hours')::interval`;

  const params = all ? [communityId] : [communityId, String(hours)];

  const r = await q(
    `
    SELECT ct.tweet_id
    FROM community_tweets ct
    LEFT JOIN tweet_metrics_latest tm ON tm.tweet_id = ct.tweet_id
    WHERE ${where}
      AND (tm.tweet_id IS NULL OR tm.updated_at < now() - interval '6 hours')
    ORDER BY ct.created_at DESC NULLS LAST
    LIMIT 5000
  `,
    params
  );

  const ids = r.rows.map((x) => String(x.tweet_id));
  console.log(`refresh-metrics: tweets to update = ${ids.length} (all=${all}, hours=${hours})`);
  if (ids.length === 0) return;

  for (const group of chunk(ids, BATCH_TWEET_IDS)) {
    const data = await api.getTweetsByIds(group);
    const tweets = Array.isArray(data?.tweets) ? data.tweets : [];

    for (const t of tweets) {
      await upsertMetrics(t);
    }

    console.log(`refresh-metrics: updated batch size=${group.length}`);
  }

  console.log("refresh-metrics done.");
}

async function refreshUsers({ hours = ACTIVE_HOURS } = {}) {
  const communityId = COMMUNITY_ID;
  if (!communityId) die("No COMMUNITY_ID");

  const r = await q(
    `
    SELECT DISTINCT lower(author_username) as username
    FROM community_tweets
    WHERE community_id=$1
      AND author_username IS NOT NULL
      AND created_at >= now() - ($2 || ' hours')::interval
    ORDER BY 1
  `,
    [communityId, String(hours)]
  );

  const users = r.rows.map((x) => x.username).filter(Boolean);
  console.log(`refresh-users: found ${users.length} active users (last ${hours}h)`);

  for (const username of users) {
    try {
      const data = await api.getUserInfo(username);
      if (data?.user) await upsertUser(data.user);
      else if (data?.data) await upsertUser(data.data);
      else if (data?.result) await upsertUser(data.result);
      console.log(`refresh-users ok @${username}`);
    } catch (e) {
      console.warn(`refresh-users fail @${username}: ${e.message}`);
    }
  }

  console.log("refresh-users done.");
}

async function syncMembers() {
  const communityId = COMMUNITY_ID;
  if (!communityId) die("No COMMUNITY_ID");

  let cursor = null;
  let total = 0;

  while (true) {
    const data = await api.getCommunityMembers({ community_id: communityId, cursor });
    const members = Array.isArray(data?.members) ? data.members : [];

    for (const m of members) {
      total++;
      await upsertUser(m);
      await q(
        `
        INSERT INTO community_members (community_id, user_id, username, updated_at)
        VALUES ($1,$2,$3, now())
        ON CONFLICT (community_id, user_id) DO UPDATE SET
          username=EXCLUDED.username,
          updated_at=now()
      `,
        [communityId, String(m.id), m.userName]
      );
    }

    console.log(`sync-members page added=${members.length} total=${total}`);

    if (!data?.has_next_page || !data?.next_cursor) break;
    cursor = data.next_cursor;
  }

  console.log("sync-members done.");
}

async function userStats(username) {
  const communityId = COMMUNITY_ID;
  if (!communityId) die("No COMMUNITY_ID");
  if (!username) die("Usage: node parser.js user-stats <username>");

  // all-time totals for user
  const r = await q(
    `
    SELECT
      lower(ct.author_username) as username,
      COUNT(*)::bigint as posts,
      COALESCE(SUM(tm.view_count),0)::bigint as views,
      COALESCE(SUM(tm.like_count),0)::bigint as likes,
      COALESCE(SUM(tm.retweet_count),0)::bigint as retweets,
      COALESCE(SUM(tm.reply_count),0)::bigint as replies,
      COALESCE(SUM(tm.quote_count),0)::bigint as quotes,
      COALESCE(SUM(tm.bookmark_count),0)::bigint as bookmarks
    FROM community_tweets ct
    LEFT JOIN tweet_metrics_latest tm ON tm.tweet_id = ct.tweet_id
    WHERE ct.community_id = $1
      AND lower(ct.author_username) = lower($2)
    GROUP BY lower(ct.author_username)
  `,
    [communityId, username]
  );

  console.log(r.rows[0] || { username: username.toLowerCase(), posts: 0, views: 0, likes: 0 });
}

async function main() {
  const cmd = process.argv[2];

  try {
    if (!cmd || cmd === "help" || cmd === "--help") {
      console.log(`
Commands:
  node parser.js doctor
  node parser.js backfill
  node parser.js ingest-new
  node parser.js refresh-metrics [--all] [--hours=48]
  node parser.js refresh-users [--hours=24]
  node parser.js sync-members
  node parser.js user-stats <username>

Env:
  TWITTERAPI_IO_KEY
  COMMUNITY_ID
  DATABASE_URL
  PGSSL=true (Railway обычно)
  MIN_REQUEST_INTERVAL_MS=5200
  TOP_PAGES=3
  RECENT_HOURS=48
  ACTIVE_HOURS=24
  BATCH_TWEET_IDS=80
  BACKFILL_CUTOFF_DAYS=0
  BACKFILL_PAGES_PER_RUN=50
      `.trim());
      return;
    }

    if (!DATABASE_URL) die("Missing DATABASE_URL");
    if (!COMMUNITY_ID) die("Missing COMMUNITY_ID");
    if (!API_KEY) die("Missing TWITTERAPI_IO_KEY");

    if (cmd === "doctor") await doctor();
    else if (cmd === "backfill") await backfill();
    else if (cmd === "ingest-new") await ingestNew();
    else if (cmd === "refresh-metrics") {
      const all = process.argv.includes("--all");
      const hoursArg = process.argv.find((x) => x.startsWith("--hours="));
      const hours = hoursArg ? Number(hoursArg.split("=")[1]) : RECENT_HOURS;
      await refreshMetrics({ all, hours });
    } else if (cmd === "refresh-users") {
      const hoursArg = process.argv.find((x) => x.startsWith("--hours="));
      const hours = hoursArg ? Number(hoursArg.split("=")[1]) : ACTIVE_HOURS;
      await refreshUsers({ hours });
    } else if (cmd === "sync-members") await syncMembers();
    else if (cmd === "user-stats") await userStats(process.argv[3]);
    else die(`Unknown command: ${cmd}`);
  } finally {
    // pool закрываем ТОЛЬКО здесь (иначе "Cannot use a pool after calling end")
    await pool.end();
  }
}

process.on("unhandledRejection", (e) => {
  console.error("UNHANDLED:", e);
  process.exit(1);
});

main();
