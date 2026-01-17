BEGIN;

CREATE TABLE IF NOT EXISTS communities (
  community_id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ingest_state (
  key TEXT PRIMARY KEY,
  value TEXT,
  updated_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS community_tweets (
  tweet_id TEXT PRIMARY KEY,
  community_id TEXT NOT NULL,
  created_at TIMESTAMPTZ,
  url TEXT,
  text TEXT,
  author_id TEXT,
  author_username TEXT,
  author_name TEXT,
  raw_json JSONB,
  inserted_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ct_community_created ON community_tweets (community_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ct_community_author ON community_tweets (community_id, lower(author_username));
CREATE INDEX IF NOT EXISTS idx_ct_created ON community_tweets (created_at DESC);

CREATE TABLE IF NOT EXISTS tweet_metrics_latest (
  tweet_id TEXT PRIMARY KEY,
  view_count BIGINT DEFAULT 0,
  like_count BIGINT DEFAULT 0,
  retweet_count BIGINT DEFAULT 0,
  reply_count BIGINT DEFAULT 0,
  quote_count BIGINT DEFAULT 0,
  bookmark_count BIGINT DEFAULT 0,
  updated_at TIMESTAMPTZ DEFAULT now(),
  raw_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_tml_updated ON tweet_metrics_latest (updated_at DESC);

CREATE TABLE IF NOT EXISTS users (
  user_id TEXT PRIMARY KEY,
  username TEXT UNIQUE,
  name TEXT,
  followers BIGINT DEFAULT 0,
  following BIGINT DEFAULT 0,
  profile_picture TEXT,
  verified_type TEXT,
  is_blue_verified BOOLEAN,
  updated_at TIMESTAMPTZ DEFAULT now(),
  raw_json JSONB
);

CREATE INDEX IF NOT EXISTS idx_users_username_lower ON users (lower(username));

CREATE TABLE IF NOT EXISTS community_members (
  community_id TEXT NOT NULL,
  user_id TEXT,
  username TEXT,
  name TEXT,
  followers BIGINT DEFAULT 0,
  following BIGINT DEFAULT 0,
  profile_picture TEXT,
  is_blue_verified BOOLEAN,
  updated_at TIMESTAMPTZ DEFAULT now(),
  raw_json JSONB,
  PRIMARY KEY (community_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_members_community_username_lower ON community_members (community_id, lower(username));

COMMIT;
