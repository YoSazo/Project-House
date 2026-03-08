import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { pool } from "./db.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const seedPath = path.resolve(__dirname, "../../db/seed.sql");
const sql = await fs.readFile(seedPath, "utf8");

await pool.query(sql);
console.log("Seed applied.");
await pool.end();