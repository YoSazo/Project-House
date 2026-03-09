import "dotenv/config";
import express from "express";
import cors from "cors";
import { WebSocketServer } from "ws";
import { pool, withTx } from "./db.js";
import { schedulerTick, systemState, sampleMetrics, surveyHouse, verifyBlock, runWeatheringTest, completeSealingForHouse, triggerMaintenanceForHouse, getMaintenanceAlerts, logisticsLaneSummary } from "./scheduler.js";

const app = express();
const port = Number(process.env.PORT || 8787);
const tickMs = Number(process.env.SCHEDULER_MS || 5000);
const PIPELINE_TARGETS = [3, 5, 10, 20];
const ROBOT_TARGETS = [9, 18, 36];
const DEFAULT_NEW_HOUSE_STAGE = "site_prep";

app.use(cors());
app.use(express.json());

let schedulerQueue = Promise.resolve();
let resetInFlight = null;
let resetInFlightTargets = null;
let serializedTaskSeq = 0;
let serializedPendingCount = 0;

const DEBUG_QUEUE = process.env.DEBUG_QUEUE === "1";
const DEBUG_RESET = process.env.DEBUG_RESET !== "0";

function runSerialized(task, label = "task") {
  const taskId = ++serializedTaskSeq;
  const enqueuedAt = Date.now();
  serializedPendingCount += 1;

  const shouldLogQueue = DEBUG_QUEUE || (DEBUG_RESET && label.startsWith("reset"));

  if (shouldLogQueue) {
    console.log(`[queue] enqueued id=${taskId} label=${label} pending=${serializedPendingCount}`);
  }

  const wrapped = async () => {
    const startedAt = Date.now();
    const waitMs = startedAt - enqueuedAt;

    if (shouldLogQueue) {
      console.log(`[queue] start id=${taskId} label=${label} wait_ms=${waitMs} pending=${serializedPendingCount}`);
    }

    try {
      const result = await task();
      if (shouldLogQueue) {
        console.log(`[queue] done id=${taskId} label=${label} run_ms=${Date.now() - startedAt}`);
      }
      return result;
    } catch (error) {
      if (shouldLogQueue) {
        console.error(`[queue] error id=${taskId} label=${label} run_ms=${Date.now() - startedAt} message=${error?.message}`);
      }
      throw error;
    } finally {
      serializedPendingCount = Math.max(0, serializedPendingCount - 1);
      if (shouldLogQueue) {
        console.log(`[queue] settle id=${taskId} label=${label} pending=${serializedPendingCount}`);
      }
    }
  };

  const next = schedulerQueue.then(wrapped, wrapped);
  schedulerQueue = next.catch(() => {});
  return next;
}

let autoTickRunning = false;

async function ensureClusters(client, targetCount = 3) {
  const existing = await client.query(`SELECT id FROM robot_clusters ORDER BY id`);
  if (existing.rowCount >= targetCount) return existing.rows.map((r) => r.id);

  for (let i = existing.rowCount + 1; i <= targetCount; i += 1) {
    await client.query(`INSERT INTO robot_clusters (name) VALUES ($1)`, [`Cluster ${i}`]);
  }

  const after = await client.query(`SELECT id FROM robot_clusters ORDER BY id`);
  return after.rows.map((r) => r.id);
}

async function seedTerrainForHouse(client, houseId, stage) {
  await client.query(
    `WITH terrain AS (
       SELECT x, y,
         0::numeric(8,3) AS target_grade,
         (SIN((x + 1) / 1.7) * 0.42 + COS((y + 2) / 1.3) * 0.31)::numeric(8,4) AS base_relief
       FROM generate_series(0,9) x
       CROSS JOIN generate_series(0,9) y
     )
     INSERT INTO terrain_cells (
       house_id,
       x,
       y,
       target_grade,
       current_grade,
       soil_density,
       compaction_score,
       obstacle_type,
       obstacle_cleared,
       status,
       assigned_robot_id,
       updated_at
     )
     SELECT
       $1,
       t.x,
       t.y,
       t.target_grade,
       CASE
         WHEN $2 = 'site_prep' THEN ROUND((
           t.base_relief
           + rv.grade_delta
           + CASE rv.obstacle_type
             WHEN 'rock' THEN 0.42
             WHEN 'root' THEN 0.25
             WHEN 'debris' THEN 0.16
             ELSE 0
           END
         )::numeric, 3)
         ELSE t.target_grade
       END AS current_grade,
       ROUND((0.82 + rv.soil_noise * 0.46)::numeric, 3) AS soil_density,
       CASE
         WHEN $2 = 'site_prep' THEN ROUND((CASE
           WHEN rv.progress < 0.35 THEN 0.14 + rv.compact_noise * 0.18
           WHEN rv.progress < 0.7 THEN 0.38 + rv.compact_noise * 0.24
           ELSE 0.66 + rv.compact_noise * 0.16
         END)::numeric, 3)
         ELSE ROUND((0.92 + rv.compact_noise * 0.08)::numeric, 3)
       END AS compaction_score,
       CASE
         WHEN $2 = 'site_prep' THEN rv.obstacle_type
         ELSE 'none'
       END AS obstacle_type,
       CASE
         WHEN $2 = 'site_prep' THEN (rv.obstacle_type = 'none' AND rv.progress > 0.72)
         ELSE TRUE
       END AS obstacle_cleared,
       CASE
         WHEN $2 = 'site_prep' THEN CASE
           WHEN rv.obstacle_type <> 'none' AND rv.progress < 0.7 THEN 'raw'
           WHEN rv.progress < 0.45 THEN 'raw'
           WHEN rv.progress < 0.78 THEN 'grading'
           ELSE 'compacted'
         END
         ELSE 'ready'
       END AS status,
       NULL,
       NOW()
     FROM terrain t
     CROSS JOIN LATERAL (
       SELECT
         random() AS progress,
         ((random() - 0.5) * 0.36) AS grade_delta,
         random() AS soil_noise,
         random() AS compact_noise,
         CASE
           WHEN obstacle_roll < 0.08 THEN 'rock'
           WHEN obstacle_roll < 0.18 THEN 'root'
           WHEN obstacle_roll < 0.28 THEN 'debris'
           ELSE 'none'
         END AS obstacle_type
       FROM (SELECT random() AS obstacle_roll) obs
     ) rv
     ON CONFLICT (house_id, x, y) DO UPDATE
     SET target_grade = EXCLUDED.target_grade,
         current_grade = EXCLUDED.current_grade,
         soil_density = EXCLUDED.soil_density,
         compaction_score = EXCLUDED.compaction_score,
         obstacle_type = EXCLUDED.obstacle_type,
         obstacle_cleared = EXCLUDED.obstacle_cleared,
         status = EXCLUDED.status,
         assigned_robot_id = NULL,
         updated_at = NOW()`,
    [houseId, stage]
  );
}

async function createHouseWithGrid(client, stage = DEFAULT_NEW_HOUSE_STAGE) {
  const insertHouse = await client.query(
    `INSERT INTO houses (name, stage, started_at)
     VALUES ('temp', $1, NOW())
     RETURNING id`,
    [stage]
  );

  const houseId = insertHouse.rows[0].id;
  await client.query(`UPDATE houses SET name = $2 WHERE id = $1`, [houseId, `House ${houseId}`]);

  await client.query(
    `WITH grid AS (
       SELECT x, y, z,
         CASE
           WHEN z = 0 THEN 'foundation'
           WHEN z BETWEEN 1 AND 3 THEN CASE WHEN (x + y) % 4 = 0 THEN 'mep' ELSE 'wall' END
           ELSE 'roof'
         END AS component_type,
         (z * 10000 + y * 100 + x) AS build_sequence
       FROM generate_series(0,9) x
       CROSS JOIN generate_series(0,9) y
       CROSS JOIN generate_series(0,4) z
     )
     INSERT INTO grid_cells (house_id, x, y, z, component_type, build_sequence, status, filled_at)
     SELECT $1, g.x, g.y, g.z, g.component_type, g.build_sequence,
       CASE
         WHEN $2 IN ('site_prep', 'surveying', 'foundation') THEN 'empty'
         WHEN $2 = 'framing' AND g.component_type = 'foundation' THEN 'filled'
         WHEN $2 = 'mep' AND g.component_type IN ('foundation', 'wall') THEN 'filled'
         WHEN $2 = 'finishing' AND g.component_type IN ('foundation', 'wall', 'mep') THEN 'filled'
         WHEN $2 IN ('sealing', 'community_assembly', 'complete') THEN 'filled'
         ELSE 'empty'
       END,
       CASE
         WHEN $2 = 'framing' AND g.component_type = 'foundation' THEN NOW()
         WHEN $2 = 'mep' AND g.component_type IN ('foundation', 'wall') THEN NOW()
         WHEN $2 = 'finishing' AND g.component_type IN ('foundation', 'wall', 'mep') THEN NOW()
         WHEN $2 IN ('sealing', 'community_assembly', 'complete') THEN NOW()
         ELSE NULL
       END
     FROM grid g`,
    [houseId, stage]
  );

  await seedTerrainForHouse(client, houseId, stage);
}

async function adjustHouseCount(client, target) {
  const active = await client.query(`SELECT id FROM houses WHERE is_active = TRUE ORDER BY id`);
  const current = active.rowCount;

  if (current < target) {
    const toCreate = target - current;
    for (let i = 0; i < toCreate; i += 1) {
      await createHouseWithGrid(client, DEFAULT_NEW_HOUSE_STAGE);
    }
  }

  if (current > target) {
    const toDeactivate = await client.query(
      `SELECT id
       FROM houses
       WHERE is_active = TRUE
       ORDER BY id DESC
       LIMIT $1`,
      [current - target]
    );
    const ids = toDeactivate.rows.map((r) => r.id);

    if (ids.length) {
      await client.query(`UPDATE houses SET is_active = FALSE, cluster_id = NULL WHERE id = ANY($1::int[])`, [ids]);
      await client.query(`UPDATE robot_clusters SET status = 'idle', current_house_id = NULL, updated_at = NOW() WHERE current_house_id = ANY($1::int[])`, [ids]);
      await client.query(`UPDATE robots SET active_house_id = NULL, status = 'idle', updated_at = NOW() WHERE active_house_id = ANY($1::int[])`, [ids]);
      await client.query(`DELETE FROM fabricator_queue WHERE house_id = ANY($1::int[]) AND consumed_at IS NULL`, [ids]);
      await client.query(`DELETE FROM assembly_kits WHERE house_id = ANY($1::int[])`, [ids]);
      await client.query(`DELETE FROM site_surveys WHERE house_id = ANY($1::int[])`, [ids]);
      await client.query(`DELETE FROM block_verifications WHERE house_id = ANY($1::int[])`, [ids]);
      await client.query(`DELETE FROM house_maintenance WHERE house_id = ANY($1::int[])`, [ids]);
      await client.query(`UPDATE grid_cells SET status = 'empty', assigned_robot_id = NULL, reserved_at = NULL WHERE house_id = ANY($1::int[]) AND status = 'reserved'`, [ids]);
      await client.query(`UPDATE terrain_cells SET assigned_robot_id = NULL, updated_at = NOW() WHERE house_id = ANY($1::int[])`, [ids]);
    }
  }
}

async function adjustRobotCount(client, target) {
  const clusters = await ensureClusters(client, 3);
  const currentRes = await client.query(`SELECT COUNT(*)::int AS count FROM robots`);
  const current = currentRes.rows[0].count;

  if (current < target) {
    const toAdd = target - current;
    for (let i = 0; i < toAdd; i += 1) {
      const clusterId = clusters[i % clusters.length];
      await client.query(
        `INSERT INTO robots (cluster_id, name)
         VALUES ($1, $2)`,
        [clusterId, `Cluster ${clusterId} Robot ${Date.now()}-${i}`]
      );
    }
    return;
  }

  if (current > target) {
    const toRemove = current - target;
    const removeIdle = await client.query(
      `SELECT id FROM robots
       WHERE active_house_id IS NULL
       ORDER BY id DESC
       LIMIT $1`,
      [toRemove]
    );

    let ids = removeIdle.rows.map((r) => r.id);

    if (ids.length < toRemove) {
      const removeBusy = await client.query(
        `SELECT id FROM robots
         WHERE id <> ALL($1::int[])
         ORDER BY id DESC
         LIMIT $2`,
        [ids.length ? ids : [0], toRemove - ids.length]
      );
      ids = ids.concat(removeBusy.rows.map((r) => r.id));
    }

    if (ids.length) {
      await client.query(
        `DELETE FROM fabricator_queue
         WHERE consumed_at IS NULL
           AND cell_id IN (
             SELECT id FROM grid_cells WHERE assigned_robot_id = ANY($1::int[])
           )`,
        [ids]
      );

      await client.query(
        `UPDATE grid_cells
         SET status = 'empty', assigned_robot_id = NULL, reserved_at = NULL
         WHERE assigned_robot_id = ANY($1::int[])
           AND status = 'reserved'`,
        [ids]
      );

      await client.query(
        `UPDATE terrain_cells
         SET assigned_robot_id = NULL, updated_at = NOW()
         WHERE assigned_robot_id = ANY($1::int[])`,
        [ids]
      );

      await client.query(
        `UPDATE assembly_kits
         SET activated_robot_id = NULL,
             status = CASE WHEN status = 'activated' THEN 'passed' ELSE status END,
             updated_at = NOW()
         WHERE activated_robot_id = ANY($1::int[])`,
        [ids]
      );

      await client.query(`DELETE FROM robots WHERE id = ANY($1::int[])`, [ids]);

      await client.query(`
        UPDATE robot_clusters rc
        SET status = 'idle', current_house_id = NULL, updated_at = NOW()
        WHERE rc.current_house_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM robots r
            WHERE r.cluster_id = rc.id
              AND r.active_house_id = rc.current_house_id
          )
      `);

      await client.query(`
        UPDATE houses h
        SET cluster_id = NULL
        WHERE h.cluster_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM robot_clusters rc
            WHERE rc.id = h.cluster_id
              AND rc.current_house_id = h.id
          )
      `);
    }
  }
}

async function resetExperimentState(client) {
  await client.query(`TRUNCATE metrics_samples`);

  await client.query(`DELETE FROM fabricator_queue WHERE consumed_at IS NULL`);
  await client.query(`DELETE FROM assembly_kits WHERE house_id IN (SELECT id FROM houses WHERE is_active = TRUE)`);
  await client.query(`DELETE FROM site_surveys WHERE house_id IN (SELECT id FROM houses WHERE is_active = TRUE)`);
  await client.query(`DELETE FROM survey_runs WHERE house_id IN (SELECT id FROM houses WHERE is_active = TRUE)`);
  await client.query(`DELETE FROM block_verifications WHERE house_id IN (SELECT id FROM houses WHERE is_active = TRUE)`);
  await client.query(`DELETE FROM house_maintenance WHERE house_id IN (SELECT id FROM houses WHERE is_active = TRUE)`);
  await client.query(`DELETE FROM logistics_lane_segments WHERE house_id IN (SELECT id FROM houses WHERE is_active = TRUE)`);

  await client.query(`
    UPDATE grid_cells
    SET status = 'empty',
        assigned_robot_id = NULL,
        reserved_at = NULL,
        filled_at = NULL
    WHERE house_id IN (SELECT id FROM houses WHERE is_active = TRUE)
  `);

  await client.query(`
    UPDATE terrain_cells
    SET assigned_robot_id = NULL,
        updated_at = NOW()
    WHERE house_id IN (SELECT id FROM houses WHERE is_active = TRUE)
  `);

  const activeHouses = await client.query(`SELECT id FROM houses WHERE is_active = TRUE ORDER BY id`);

  for (let i = 0; i < activeHouses.rowCount; i += 1) {
    const houseId = activeHouses.rows[i].id;
    const stage = DEFAULT_NEW_HOUSE_STAGE;

    await client.query(
      `UPDATE houses
       SET stage = $2,
           completed_at = NULL,
           cluster_id = NULL,
           started_at = NOW(),
           survey_status = 'pending',
           soil_signature = NULL,
           site_zone = 'BUILDABLE',
           recommended_build_zone = NULL,
           survey_uncertainty_remaining = 1,
           survey_stop_reason = NULL,
           survey_probe_count = 0,
           reference_patch_x = NULL,
           reference_patch_y = NULL,
           reference_patch_protected = FALSE,
           reference_patch_stamped_at = NULL,
           sealing_complete = FALSE,
           sealing_started_at = NULL,
           sealing_progress = 0
       WHERE id = $1`,
      [houseId, stage]
    );

    await client.query(
      `UPDATE grid_cells
       SET status = 'empty',
           filled_at = NULL,
           assigned_robot_id = NULL,
           reserved_at = NULL
       WHERE house_id = $1`,
      [houseId]
    );

    await seedTerrainForHouse(client, houseId, stage);
  }

  await client.query(`
    UPDATE robot_clusters
    SET status = 'idle', current_house_id = NULL, updated_at = NOW()
  `);

  await client.query(`
    UPDATE robots
    SET status = 'idle',
        pos_x = 0,
        pos_y = 0,
        pos_z = 0,
        busy_until = NULL,
        active_house_id = NULL,
        total_work_seconds = 0,
        total_idle_seconds = 0,
        total_placements = 0,
        total_placement_retries = 0,
        total_placement_failures = 0,
        updated_at = NOW()
  `);
}

app.get("/api/health", async (_req, res) => {
  try {
    await pool.query("SELECT 1");
    res.json({ ok: true });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/api/state", async (_req, res) => {
  const state = await systemState();
  res.json(state);
});

app.post("/api/survey-site", async (req, res) => {
  try {
    const houseId = Number(req.body?.house_id);
    if (!Number.isFinite(houseId)) {
      res.status(400).json({ ok: false, error: "house_id is required" });
      return;
    }

    const result = await runSerialized(async () => surveyHouse(houseId), `survey:${houseId}`);
    const state = await systemState();
    broadcast({ type: "state", data: state });
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/api/verify-block", async (req, res) => {
  try {
    const houseId = Number(req.body?.house_id);
    const soilSignature = String(req.body?.soil_signature || "").trim();

    if (!Number.isFinite(houseId) || !soilSignature) {
      res.status(400).json({ ok: false, error: "house_id and soil_signature are required" });
      return;
    }

    const result = await runSerialized(async () => verifyBlock({ houseId, soilSignature }), `verify-block:${houseId}`);
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/api/run-weathering-test", async (req, res) => {
  try {
    const soilSignature = String(req.body?.soil_signature || "").trim();
    if (!soilSignature) {
      res.status(400).json({ ok: false, error: "soil_signature is required" });
      return;
    }

    const result = await runSerialized(async () => runWeatheringTest(soilSignature), `weathering:${soilSignature}`);
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/api/complete-sealing", async (req, res) => {
  try {
    const houseId = Number(req.body?.house_id);
    const soilSignature = String(req.body?.soil_signature || "").trim();

    if (!Number.isFinite(houseId) || !soilSignature) {
      res.status(400).json({ ok: false, error: "house_id and soil_signature are required" });
      return;
    }

    const result = await runSerialized(async () => completeSealingForHouse(houseId, soilSignature), `complete-sealing:${houseId}`);
    const state = await systemState();
    broadcast({ type: "state", data: state });
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/api/trigger-maintenance", async (req, res) => {
  try {
    const houseId = Number(req.body?.house_id);
    if (!Number.isFinite(houseId)) {
      res.status(400).json({ ok: false, error: "house_id is required" });
      return;
    }

    const result = await runSerialized(async () => triggerMaintenanceForHouse(houseId), `trigger-maintenance:${houseId}`);
    const state = await systemState();
    broadcast({ type: "state", data: state });
    res.json({ ok: true, result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/api/soil-library", async (_req, res) => {
  try {
    const result = await pool.query(
      `SELECT *
       FROM soil_library
       ORDER BY short_term_confidence DESC, long_term_confidence DESC, updated_at DESC`
    );
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/api/site-heatmap/:houseId", async (req, res) => {
  try {
    const houseId = Number(req.params.houseId);
    if (!Number.isFinite(houseId)) {
      res.status(400).json({ ok: false, error: "houseId must be numeric" });
      return;
    }

    const result = await pool.query(
      `SELECT *
       FROM site_surveys
       WHERE house_id = $1
       ORDER BY surveyed_at DESC, probe_y, probe_x`,
      [houseId]
    );

    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/api/maintenance-alerts", async (req, res) => {
  try {
    const withinDays = Number(req.query?.within_days);
    const days = Number.isFinite(withinDays) ? Math.max(1, Math.min(3650, withinDays)) : 180;
    const alerts = await getMaintenanceAlerts(days);
    res.json(alerts);
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.get("/api/logistics/lanes", async (_req, res) => {
  try {
    const rows = await logisticsLaneSummary();
    res.json(rows);
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message });
  }
});
app.get("/api/suburb/grid", async (req, res) => {
  const maxHousesRaw = Number(req.query?.max_houses);
  const maxHouses = Number.isFinite(maxHousesRaw) ? Math.max(1, Math.min(120, maxHousesRaw)) : 60;

  const result = await pool.query(
    `WITH active_houses AS (
       SELECT id
       FROM houses
       WHERE is_active = TRUE
       ORDER BY id
       LIMIT $1
     )
     SELECT
       g.id,
       g.house_id,
       g.x,
       g.y,
       g.z,
       g.status,
       g.component_type,
       g.assigned_robot_id
     FROM grid_cells g
     JOIN active_houses ah ON ah.id = g.house_id
     ORDER BY g.house_id, g.build_sequence`,
    [maxHouses]
  );

  res.json(result.rows);
});

app.get("/api/suburb/terrain", async (req, res) => {
  const maxHousesRaw = Number(req.query?.max_houses);
  const maxHouses = Number.isFinite(maxHousesRaw) ? Math.max(1, Math.min(120, maxHousesRaw)) : 60;

  const result = await pool.query(
    `WITH active_houses AS (
       SELECT id
       FROM houses
       WHERE is_active = TRUE
       ORDER BY id
       LIMIT $1
     )
     SELECT
       t.id,
       t.house_id,
       t.x,
       t.y,
       t.target_grade,
       t.current_grade,
       t.compaction_score,
       t.obstacle_type,
       t.obstacle_cleared,
       t.status,
       t.assigned_robot_id
     FROM terrain_cells t
     JOIN active_houses ah ON ah.id = t.house_id
     ORDER BY t.house_id, t.y, t.x`,
    [maxHouses]
  );

  res.json(result.rows);
});

app.get("/api/metrics/curve", async (_req, res) => {
  const result = await pool.query(`
    SELECT
      active_houses,
      ROUND(AVG(robot_idle_percent)::numeric, 2) AS avg_idle,
      ROUND(AVG(pipeline_efficiency)::numeric, 2) AS avg_efficiency,
      COUNT(*)::int AS samples
    FROM metrics_samples
    WHERE active_houses > 0
    GROUP BY active_houses
    ORDER BY active_houses
  `);
  res.json(result.rows);
});

app.get("/api/metrics/matrix", async (_req, res) => {
  const result = await pool.query(`
    SELECT
      active_houses,
      active_robots,
      ROUND(AVG(robot_idle_percent)::numeric, 2) AS avg_idle,
      ROUND(AVG(pipeline_efficiency)::numeric, 2) AS avg_efficiency,
      ROUND(AVG(throughput_cells_per_hour)::numeric, 2) AS avg_throughput,
      ROUND(AVG(avg_retries_per_placement)::numeric, 3) AS avg_retries,
      ROUND(AVG(placement_failures)::numeric, 2) AS avg_failures,
      ROUND(AVG(terrain_ready_percent)::numeric, 2) AS avg_terrain_ready,
      ROUND(AVG(avg_grade_error)::numeric, 4) AS avg_grade_error,
      ROUND(AVG(avg_compaction)::numeric, 3) AS avg_compaction,
      ROUND(AVG(obstacle_cells_remaining)::numeric, 2) AS avg_obstacles_remaining,
      ROUND(AVG(community_kits_activated)::numeric, 2) AS avg_kits_activated,
      ROUND(AVG(active_surveys)::numeric, 2) AS avg_active_surveys,
      ROUND(AVG(soil_recipes_learned)::numeric, 2) AS avg_soil_recipes,
      ROUND(AVG(blocks_verified)::numeric, 2) AS avg_blocks_verified,
      ROUND(AVG(blocks_failed_qc)::numeric, 2) AS avg_blocks_failed,
      ROUND(AVG(houses_in_maintenance)::numeric, 2) AS avg_maintenance_houses,
      ROUND(AVG(avg_ttl_days)::numeric, 2) AS avg_ttl_days,
      ROUND(AVG(avg_lane_condition)::numeric, 3) AS avg_lane_condition,
      ROUND(AVG(conditioned_lane_percent)::numeric, 2) AS avg_conditioned_lane_percent,
      ROUND(AVG(degraded_lane_segments)::numeric, 2) AS avg_degraded_lane_segments,
      ROUND(AVG(stale_relay_segments)::numeric, 2) AS avg_stale_relay_segments,
      ROUND(AVG(lane_verification_events)::numeric, 2) AS avg_lane_verification_events,
      ROUND(AVG(reference_patches_protected)::numeric, 2) AS avg_reference_patches_protected,
      ROUND(AVG(reference_patches_total)::numeric, 2) AS avg_reference_patches_total,
      COUNT(*)::int AS samples
    FROM metrics_samples
    WHERE active_houses > 0 AND active_robots > 0
    GROUP BY active_houses, active_robots
    ORDER BY active_houses, active_robots
  `);
  res.json(result.rows);
});

app.get("/api/houses/:houseId/grid", async (req, res) => {
  const houseId = Number(req.params.houseId);
  const result = await pool.query(
    `SELECT id, x, y, z, status, component_type, build_sequence, assigned_robot_id, filled_at
     FROM grid_cells
     WHERE house_id = $1
     ORDER BY build_sequence
     LIMIT 1200`,
    [houseId]
  );
  res.json(result.rows);
});

app.post("/api/pipeline/target", async (req, res) => {
  try {
    const requested = Number(req.body?.target_houses);
    const target = Number.isFinite(requested) ? Math.max(1, Math.min(60, requested)) : 3;

    const state = await runSerialized(async () => {
      await withTx(async (client) => {
        await adjustHouseCount(client, target);
      });

      for (let i = 0; i < 3; i += 1) {
        await schedulerTick();
      }

      return systemState();
    }, `pipeline-target:${target}`);

    broadcast({ type: "state", data: state });
    res.json({ ok: true, target_houses: target, allowed_targets: PIPELINE_TARGETS, state });
  } catch (error) {
    console.error("Pipeline target error", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/api/robots/target", async (req, res) => {
  try {
    const requested = Number(req.body?.target_robots);
    const target = Number.isFinite(requested) ? Math.max(3, Math.min(120, requested)) : 9;

    const state = await runSerialized(async () => {
      await withTx(async (client) => {
        await adjustRobotCount(client, target);
      });

      for (let i = 0; i < 3; i += 1) {
        await schedulerTick();
      }

      return systemState();
    }, `robot-target:${target}`);

    broadcast({ type: "state", data: state });
    res.json({ ok: true, target_robots: target, allowed_targets: ROBOT_TARGETS, state });
  } catch (error) {
    console.error("Robot target error", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/api/experiment/reset", async (req, res) => {
  try {
    const houseTargetRaw = Number(req.body?.target_houses);
    const robotTargetRaw = Number(req.body?.target_robots);

    const targetHouses = Number.isFinite(houseTargetRaw) ? Math.max(1, Math.min(60, houseTargetRaw)) : null;
    const targetRobots = Number.isFinite(robotTargetRaw) ? Math.max(3, Math.min(120, robotTargetRaw)) : null;

    if (DEBUG_RESET) {
      console.log(`[reset] request target_houses=${targetHouses ?? "auto"} target_robots=${targetRobots ?? "auto"} in_flight=${Boolean(resetInFlight)} pending=${serializedPendingCount}`);
    }

    if (!resetInFlight) {
      resetInFlightTargets = { targetHouses, targetRobots };
      const resetLabel = `reset:h=${targetHouses ?? "auto"}:r=${targetRobots ?? "auto"}`;

      resetInFlight = runSerialized(async () => {
        const resetStartedAt = Date.now();
        if (DEBUG_RESET) {
          console.log(`[reset] begin label=${resetLabel}`);
        }

        const txStartedAt = Date.now();
        await withTx(async (client) => {
          if (targetHouses !== null) {
            const adjustHousesStartedAt = Date.now();
            await adjustHouseCount(client, targetHouses);
            if (DEBUG_RESET) {
              console.log(`[reset] step=adjust_house_count ms=${Date.now() - adjustHousesStartedAt} target=${targetHouses}`);
            }
          }

          if (targetRobots !== null) {
            const adjustRobotsStartedAt = Date.now();
            await adjustRobotCount(client, targetRobots);
            if (DEBUG_RESET) {
              console.log(`[reset] step=adjust_robot_count ms=${Date.now() - adjustRobotsStartedAt} target=${targetRobots}`);
            }
          }

          const resetStateStartedAt = Date.now();
          await resetExperimentState(client);
          if (DEBUG_RESET) {
            console.log(`[reset] step=reset_experiment_state ms=${Date.now() - resetStateStartedAt}`);
          }
        });

        if (DEBUG_RESET) {
          console.log(`[reset] step=withTx ms=${Date.now() - txStartedAt}`);
        }

        const sampleStartedAt = Date.now();
        await sampleMetrics();
        if (DEBUG_RESET) {
          console.log(`[reset] step=sample_metrics ms=${Date.now() - sampleStartedAt}`);
        }

        const stateStartedAt = Date.now();
        const state = await systemState();
        if (DEBUG_RESET) {
          console.log(`[reset] step=system_state ms=${Date.now() - stateStartedAt}`);
          console.log(`[reset] complete label=${resetLabel} total_ms=${Date.now() - resetStartedAt}`);
        }

        return state;
      }, resetLabel).finally(() => {
        if (DEBUG_RESET) {
          console.log("[reset] clear_inflight");
        }
        resetInFlight = null;
        resetInFlightTargets = null;
      });
    } else if (DEBUG_RESET) {
      console.log(`[reset] join_inflight target_houses=${targetHouses ?? "auto"} target_robots=${targetRobots ?? "auto"}`);
    }

    const activeTargets = resetInFlightTargets ?? { targetHouses, targetRobots };
    const joinedInflightReset = activeTargets.targetHouses !== targetHouses || activeTargets.targetRobots !== targetRobots;
    const state = await resetInFlight;

    broadcast({ type: "state", data: state });

    res.json({
      ok: true,
      state,
      target_houses: activeTargets.targetHouses,
      target_robots: activeTargets.targetRobots,
      joined_inflight_reset: joinedInflightReset
    });
  } catch (error) {
    console.error("Experiment reset error", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/api/tick", async (_req, res) => {
  try {
    await runSerialized(async () => {
      await schedulerTick();
    }, "manual-tick");
    res.json({ ok: true });
  } catch (error) {
    console.error("Manual tick error", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/api/reset-metrics", async (_req, res) => {
  try {
    await runSerialized(async () => {
      await pool.query("TRUNCATE metrics_samples");
      await sampleMetrics();
    }, "reset-metrics");
    res.json({ ok: true });
  } catch (error) {
    console.error("Reset metrics error", error);
    res.status(500).json({ ok: false, error: error.message });
  }
});

const server = app.listen(port, () => {
  console.log(`Server listening on http://localhost:${port}`);
});

const wss = new WebSocketServer({ server, path: "/ws" });

function broadcast(payload) {
  const encoded = JSON.stringify(payload);
  wss.clients.forEach((client) => {
    if (client.readyState === 1) client.send(encoded);
  });
}

wss.on("connection", async (socket) => {
  const state = await systemState();
  socket.send(JSON.stringify({ type: "state", data: state }));
});

setInterval(() => {
  if (autoTickRunning || resetInFlight) return;
  autoTickRunning = true;

  runSerialized(async () => {
    await schedulerTick();
    const state = await systemState();
    broadcast({ type: "state", data: state });
  }, "auto-tick").catch((error) => {
    console.error("Tick error", error);
    broadcast({ type: "error", data: { message: error.message } });
  }).finally(() => {
    autoTickRunning = false;
  });
}, tickMs);

process.on("unhandledRejection", (error) => {
  console.error("Unhandled rejection", error);
});

process.on("SIGINT", async () => {
  await pool.end();
  process.exit(0);
});





