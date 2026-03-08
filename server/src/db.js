import dotenv from "dotenv";
import pg from "pg";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const envCandidates = [
  process.env.DOTENV_CONFIG_PATH,
  path.resolve(process.cwd(), ".env"),
  path.resolve(process.cwd(), "../.env"),
  path.resolve(__dirname, "../../.env")
].filter(Boolean);

for (const candidate of envCandidates) {
  if (fs.existsSync(candidate)) {
    dotenv.config({ path: candidate });
    break;
  }
}

const { Pool } = pg;
const TX_ADVISORY_LOCK_KEY = 933301;
const MAX_TX_RETRIES = 5;
const RETRYABLE_TX_CODES = new Set(["40P01", "40001"]);

if (!process.env.DATABASE_URL) {
  throw new Error("Missing DATABASE_URL. Add it to .env (Neon connection string).");
}

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_SSL === "disable" ? false : { rejectUnauthorized: false }
});

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function withTx(fn) {
  let attempt = 0;

  while (true) {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");
      await client.query("SELECT pg_advisory_xact_lock($1)", [TX_ADVISORY_LOCK_KEY]);
      const result = await fn(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch {
        // Ignore rollback errors from already-aborted transactions.
      }

      if (RETRYABLE_TX_CODES.has(error?.code) && attempt < MAX_TX_RETRIES) {
        attempt += 1;
        await sleep(60 * attempt);
      } else {
        throw error;
      }
    } finally {
      client.release();
    }
  }
}
