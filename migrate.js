// Migration script to fix schema issues
import dotenv from "dotenv";
dotenv.config();

import { Pool } from "pg";

const DATABASE_URL = process.env.DATABASE_URL;
const PGSSL = (process.env.PGSSL || "").toLowerCase() === "true";

if (!DATABASE_URL) {
  console.error("FATAL: DATABASE_URL is missing");
  process.exit(1);
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: PGSSL ? { rejectUnauthorized: false } : undefined,
});

async function q(sql, params) {
  return pool.query(sql, params);
}

async function migrate() {
  console.log("Running migrations...");

  // 1. Add author_user_id column if it doesn't exist
  try {
    await q(`
      DO $$
      BEGIN
        IF NOT EXISTS (
          SELECT 1 FROM information_schema.columns 
          WHERE table_name = 'community_tweets' 
          AND column_name = 'author_user_id'
        ) THEN
          ALTER TABLE community_tweets ADD COLUMN author_user_id text NULL;
          RAISE NOTICE 'Added author_user_id column';
        ELSE
          RAISE NOTICE 'author_user_id column already exists';
        END IF;
      END $$;
    `);
    console.log("✓ Checked author_user_id column");
  } catch (e) {
    console.error("Error checking author_user_id:", e.message);
  }

  // 2. Ensure PRIMARY KEY constraint exists on (community_id, tweet_id)
  try {
    // Check what PRIMARY KEY constraints exist
    const pkCheck = await q(`
      SELECT 
        tc.constraint_name,
        string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) as columns
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kcu 
        ON tc.constraint_name = kcu.constraint_name
        AND tc.table_schema = kcu.table_schema
      WHERE tc.table_name = 'community_tweets' 
        AND tc.constraint_type = 'PRIMARY KEY'
      GROUP BY tc.constraint_name
    `);

    const expectedColumns = 'community_id,tweet_id';
    let needsFix = true;

    if (pkCheck.rows.length > 0) {
      const existingPk = pkCheck.rows[0];
      console.log(`Found PRIMARY KEY: ${existingPk.constraint_name} on (${existingPk.columns})`);
      
      if (existingPk.columns === expectedColumns) {
        console.log("✓ PRIMARY KEY constraint is correct");
        needsFix = false;
      } else {
        console.log(`⚠ PRIMARY KEY exists but on wrong columns, will recreate`);
        
        // Check for dependent foreign keys
        const fkCheck = await q(`
          SELECT 
            tc.constraint_name,
            tc.table_name
          FROM information_schema.table_constraints tc
          WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
            AND EXISTS (
              SELECT 1 FROM information_schema.key_column_usage kcu
              WHERE kcu.constraint_name = tc.constraint_name
                AND kcu.table_name = 'tweet_metrics_latest'
                AND kcu.column_name = 'tweet_id'
            )
        `);
        
        // Drop dependent foreign keys first
        for (const fk of fkCheck.rows) {
          console.log(`  Dropping dependent FK: ${fk.constraint_name} on ${fk.table_name}`);
          await q(`ALTER TABLE ${fk.table_name} DROP CONSTRAINT IF EXISTS ${fk.constraint_name}`);
        }
        
        // Drop existing PK with CASCADE to be safe
        await q(`ALTER TABLE community_tweets DROP CONSTRAINT ${existingPk.constraint_name} CASCADE`);
        console.log(`  Dropped old PRIMARY KEY: ${existingPk.constraint_name}`);
      }
    }

    if (needsFix) {
      // Check if columns exist
      const columnsCheck = await q(`
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = 'community_tweets' 
        AND column_name IN ('community_id', 'tweet_id')
      `);

      if (columnsCheck.rows.length === 2) {
        await q(`
          ALTER TABLE community_tweets 
          ADD CONSTRAINT community_tweets_pkey 
          PRIMARY KEY (community_id, tweet_id)
        `);
        console.log("✓ Created PRIMARY KEY constraint on (community_id, tweet_id)");
        console.log("  Note: Foreign key from tweet_metrics_latest was not recreated (not in schema.sql)");
      } else {
        console.warn("⚠ Columns community_id or tweet_id missing, cannot add PRIMARY KEY");
        console.warn(`  Found columns: ${columnsCheck.rows.map(r => r.column_name).join(', ')}`);
      }
    }
  } catch (e) {
    console.error("Error fixing PRIMARY KEY:", e.message);
    throw e;
  }

  console.log("Migration complete!");
}

migrate()
  .then(() => {
    pool.end();
    process.exit(0);
  })
  .catch((e) => {
    console.error("Migration failed:", e);
    pool.end();
    process.exit(1);
  });

