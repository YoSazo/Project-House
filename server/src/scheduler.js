import { pool, withTx } from "./db.js";

const STAGE_ORDER = ["site_prep", "surveying", "foundation", "framing", "mep", "finishing", "sealing", "community_assembly", "complete"];
const CLUSTER_ASSIGNABLE_STAGES = ["site_prep", "surveying", "foundation", "framing", "mep", "finishing", "sealing"];

const STAGE_COMPONENT = {
  foundation: "foundation",
  framing: "wall",
  mep: "mep",
  finishing: "roof"
};

const MOVE_SECONDS_PER_STEP = 2;
const PLACE_SECONDS = 6;
const FABRICATION_SECONDS = Number(process.env.FABRICATION_SECONDS || 25);

const MAX_INSERTION_RETRIES = Number(process.env.MAX_INSERTION_RETRIES || 3);
const IMPEDANCE_THRESHOLD = Number(process.env.IMPEDANCE_THRESHOLD || 0.72);
const IMPEDANCE_VARIANCE = Number(process.env.IMPEDANCE_VARIANCE || 0.35);
const RETRY_ADJUST_SECONDS = Number(process.env.RETRY_ADJUST_SECONDS || 2);
const FAILED_INSERTION_PENALTY_SECONDS = Number(process.env.FAILED_INSERTION_PENALTY_SECONDS || 4);

const GRADE_TOLERANCE = Number(process.env.GRADE_TOLERANCE || 0.05);
const GRADING_STEP = Number(process.env.GRADING_STEP || 0.18);
const COMPACTION_THRESHOLD = Number(process.env.COMPACTION_THRESHOLD || 0.9);
const COMPACTION_STEP = Number(process.env.COMPACTION_STEP || 0.14);
const SITE_PREP_BASE_SECONDS = Number(process.env.SITE_PREP_BASE_SECONDS || 5);
const SITE_GRADE_SECONDS = Number(process.env.SITE_GRADE_SECONDS || 2);
const SITE_COMPACT_SECONDS = Number(process.env.SITE_COMPACT_SECONDS || 3);

const SITE_PREP_OBSTACLE_SECONDS = {
  none: 0,
  root: 6,
  rock: 8,
  debris: 4
};

const SITE_PREP_OBSTACLE_CLEAR_CHANCE = {
  none: 1,
  root: 0.72,
  rock: 0.55,
  debris: 0.86
};

const COMMUNITY_KITS_PER_HOUSE = Number(process.env.COMMUNITY_KITS_PER_HOUSE || 3);
const COMMUNITY_ASSEMBLY_MIN_PROGRESS = Number(process.env.COMMUNITY_ASSEMBLY_MIN_PROGRESS || 0.18);
const COMMUNITY_ASSEMBLY_MAX_PROGRESS = Number(process.env.COMMUNITY_ASSEMBLY_MAX_PROGRESS || 0.44);
const COMMUNITY_QA_BASE_PASS = Number(process.env.COMMUNITY_QA_BASE_PASS || 0.84);

const SURVEY_PROBE_COUNT = Number(process.env.SURVEY_PROBE_COUNT || 20);
const SURVEY_REJECT_RATIO = Number(process.env.SURVEY_REJECT_RATIO || 0.3);
const SEALING_STEP_PER_ROBOT = Number(process.env.SEALING_STEP_PER_ROBOT || 0.06);
const SEALING_STEP_MIN = Number(process.env.SEALING_STEP_MIN || 0.12);
const WEATHERING_IDLE_SECONDS = Number(process.env.WEATHERING_IDLE_SECONDS || 30);
const MAINTENANCE_CHECK_SECONDS = Number(process.env.MAINTENANCE_CHECK_SECONDS || 60);

let lastWeatheringRunAt = 0;
let lastMaintenanceRunAt = 0;

const COMPONENT_BASE_IMPEDANCE = {
  foundation: 0.58,
  wall: 0.5,
  mep: 0.63,
  roof: 0.47
};

function nextStage(stage) {
  const idx = STAGE_ORDER.indexOf(stage);
  return idx >= 0 && idx < STAGE_ORDER.length - 1 ? STAGE_ORDER[idx + 1] : stage;
}

function randRange(min, max) {
  return min + Math.random() * (max - min);
}

export function generateSoilSignature() {
  const clay = Math.floor(20 + Math.random() * 40);
  const sand = Math.floor(20 + Math.random() * 40);
  const silt = Math.max(0, 100 - clay - sand);
  const organic = Math.random() < 0.15 ? Math.floor(8 + Math.random() * 10) : Math.floor(Math.random() * 5);
  const salinity = Math.random() < 0.1 ? 2 + Math.random() * 3 : Math.random();
  return `clay${clay}_sand${sand}_silt${silt}_org${organic}_sal${salinity.toFixed(1)}`;
}

function parseSoilSignature(soilSignature) {
  const match = String(soilSignature || "").match(/clay(\d+)_sand(\d+)_silt(\d+)_org(\d+)_sal([\d.]+)/);
  if (!match) {
    return { clay: 40, sand: 35, organic: 2, salinity: 0.4 };
  }

  return {
    clay: Number(match[1]),
    sand: Number(match[2]),
    organic: Number(match[4]),
    salinity: Number(match[5])
  };
}

function defaultRecipeForSoil(soilSignature) {
  const parsed = parseSoilSignature(soilSignature);
  const lime = parsed.organic > 8 ? 12 : 10;
  const cement = parsed.salinity > 2 ? 7 : 5;
  const clay = Math.max(30, Math.min(55, parsed.clay));
  const sand = Math.max(25, Math.min(55, parsed.sand));

  return {
    clay,
    sand,
    lime,
    cement
  };
}

async function ensureSoilLibraryRow(client, soilSignature) {
  const parsed = parseSoilSignature(soilSignature);
  await client.query(
    `INSERT INTO soil_library (soil_signature, clay_pct, sand_pct, organic_pct, salinity, recipe, updated_at)
     VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW())
     ON CONFLICT (soil_signature) DO NOTHING`,
    [soilSignature, parsed.clay, parsed.sand, parsed.organic, parsed.salinity, JSON.stringify(defaultRecipeForSoil(soilSignature))]
  );
}

async function ensureHouseSoilSignature(client, houseId) {
  const existing = await client.query(`SELECT soil_signature FROM houses WHERE id = $1`, [houseId]);
  let soilSignature = existing.rows[0]?.soil_signature;

  if (!soilSignature) {
    soilSignature = generateSoilSignature();
    await client.query(
      `UPDATE houses
       SET soil_signature = $2,
           site_zone = COALESCE(site_zone, 'BUILDABLE')
       WHERE id = $1`,
      [houseId, soilSignature]
    );
  }

  await ensureSoilLibraryRow(client, soilSignature);
  return soilSignature;
}

async function verifyBlockTx(client, { houseId, soilSignature }) {
  await ensureSoilLibraryRow(client, soilSignature);

  const known = await client.query(
    `SELECT *
     FROM soil_library
     WHERE soil_signature = $1`,
    [soilSignature]
  );

  const soil = known.rows[0];
  const shortConfidence = Number(soil?.short_term_confidence ?? 0);
  const totalVerified = Number(soil?.total_blocks_verified ?? 0);

  if (soil && shortConfidence > 0.95) {
    await client.query(
      `INSERT INTO block_verifications (
        house_id,
        soil_signature,
        penetration_resistance,
        moisture_pct,
        density,
        passed,
        retries,
        verification_mode,
        weathering_cycles,
        created_at
      ) VALUES ($1, $2, NULL, NULL, NULL, TRUE, 0, 'inline', 0, NOW())`,
      [houseId, soilSignature]
    );

    await client.query(
      `UPDATE soil_library
       SET total_blocks_verified = total_blocks_verified + 1,
           updated_at = NOW()
       WHERE soil_signature = $1`,
      [soilSignature]
    );

    return { passed: true, skipped: true, confidence: shortConfidence, retries: 0 };
  }

  const penetration = 100 + Math.random() * 150;
  const moisture = 8 + Math.random() * 20;
  const density = 1600 + Math.random() * 400;
  const passed = penetration > 120 && moisture < 25 && density > 1700;
  const retries = passed ? 0 : Math.floor(Math.random() * 3);

  await client.query(
    `INSERT INTO block_verifications (
      house_id,
      soil_signature,
      penetration_resistance,
      moisture_pct,
      density,
      passed,
      retries,
      verification_mode,
      weathering_cycles,
      created_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'inline', 0, NOW())`,
    [houseId, soilSignature, penetration, moisture, density, passed, retries]
  );

  const newConfidence = totalVerified > 0
    ? ((shortConfidence * totalVerified) + (passed ? 1 : 0)) / (totalVerified + 1)
    : (passed ? 0.5 : 0.1);

  await client.query(
    `UPDATE soil_library
     SET short_term_confidence = $2,
         total_blocks_verified = $3,
         recipe = COALESCE(recipe, $4::jsonb),
         updated_at = NOW()
     WHERE soil_signature = $1`,
    [soilSignature, newConfidence, totalVerified + 1, JSON.stringify(defaultRecipeForSoil(soilSignature))]
  );

  return { passed, skipped: false, retries, penetration, moisture, density, confidence: newConfidence };
}

async function weatheringTestTx(client, soilSignature) {
  await ensureSoilLibraryRow(client, soilSignature);

  const cycles = 12;
  const erosionScore = 0.7 + Math.random() * 0.3;
  const capillaryRise = Math.random() * 30;
  const longTermPassed = erosionScore > 0.8 && capillaryRise < 20;
  const longTermConfidence = longTermPassed ? 0.75 + Math.random() * 0.2 : 0.3 + Math.random() * 0.3;

  await client.query(
    `INSERT INTO block_verifications (
      house_id,
      soil_signature,
      penetration_resistance,
      moisture_pct,
      density,
      passed,
      retries,
      verification_mode,
      weathering_cycles,
      created_at
    ) VALUES (NULL, $1, NULL, NULL, NULL, $2, 0, 'accelerated_weathering', $3, NOW())`,
    [soilSignature, longTermPassed, cycles]
  );

  await client.query(
    `UPDATE soil_library
     SET long_term_confidence = $2,
         weathering_cycles_tested = GREATEST(weathering_cycles_tested, $3),
         erosion_score = $4,
         updated_at = NOW()
     WHERE soil_signature = $1`,
    [soilSignature, longTermConfidence, cycles, erosionScore]
  );

  return { longTermConfidence, erosionScore, capillaryRise, cycles, passed: longTermPassed };
}

async function surveyHouseTx(client, houseId) {
  await client.query(`UPDATE houses SET survey_status = 'surveying' WHERE id = $1`, [houseId]);
  await client.query(`DELETE FROM site_surveys WHERE house_id = $1`, [houseId]);

  const probes = [];
  for (let i = 0; i < SURVEY_PROBE_COUNT; i += 1) {
    const soilSignature = generateSoilSignature();
    const penetration = 50 + Math.random() * 200;
    const moisture = 5 + Math.random() * 40;
    const parsed = parseSoilSignature(soilSignature);
    const organic = Math.min(20, parsed.organic + Math.random() * 2.5);
    const salinity = parsed.salinity;

    const status = organic > 10 || salinity > 3 || penetration < 60
      ? 'REJECT'
      : (penetration < 100 ? 'MARGINAL' : 'BUILDABLE');

    probes.push({
      probe_x: (i % 5) * 5,
      probe_y: Math.floor(i / 5) * 5,
      depth_meters: 2 + Math.random() * 1.5,
      soil_signature: soilSignature,
      status,
      penetration_resistance: penetration,
      moisture_pct: moisture,
      organic_pct: organic,
      salinity
    });
  }

  for (const probe of probes) {
    await client.query(
      `INSERT INTO site_surveys (
        house_id,
        probe_x,
        probe_y,
        depth_meters,
        soil_signature,
        status,
        penetration_resistance,
        moisture_pct,
        organic_pct,
        salinity,
        notes,
        surveyed_at
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'scheduler', NOW())`,
      [
        houseId,
        probe.probe_x,
        probe.probe_y,
        probe.depth_meters,
        probe.soil_signature,
        probe.status,
        probe.penetration_resistance,
        probe.moisture_pct,
        probe.organic_pct,
        probe.salinity
      ]
    );
  }

  const rejectCount = probes.filter((probe) => probe.status === 'REJECT').length;
  const buildable = probes.filter((probe) => probe.status === 'BUILDABLE');
  const dominantSignature = buildable[0]?.soil_signature ?? probes[0]?.soil_signature ?? generateSoilSignature();
  const approved = rejectCount <= Math.floor(SURVEY_PROBE_COUNT * SURVEY_REJECT_RATIO);

  await ensureSoilLibraryRow(client, dominantSignature);

  await client.query(
    `UPDATE houses
     SET survey_status = $2,
         soil_signature = $3,
         site_zone = $4
     WHERE id = $1`,
    [houseId, approved ? 'approved' : 'rejected', dominantSignature, approved ? 'BUILDABLE' : 'REJECT']
  );

  return {
    status: approved ? 'approved' : 'rejected',
    rejectCount,
    probes
  };
}

async function completeSealingTx(client, { houseId, soilSignature }) {
  await ensureSoilLibraryRow(client, soilSignature);

  const soil = await client.query(
    `SELECT short_term_confidence, long_term_confidence, recipe
     FROM soil_library
     WHERE soil_signature = $1`,
    [soilSignature]
  );

  const row = soil.rows[0] || {};
  const longTermConfidence = Number(row.long_term_confidence || 0);
  const shortTermConfidence = Number(row.short_term_confidence || 0);
  const baseTTL = 365 * 5;
  const soilMultiplier = longTermConfidence > 0.85 ? 1.5 : (shortTermConfidence > 0.9 ? 1.2 : 1);
  const ttlDays = Math.max(365, Math.round(baseTTL * soilMultiplier));

  const coatingApplied = new Date();
  const ttlExpires = new Date(coatingApplied.getTime() + ttlDays * 86400000);
  const alertAt = new Date(ttlExpires.getTime() - 180 * 86400000);

  await client.query(
    `UPDATE houses
     SET sealing_complete = TRUE,
         sealing_progress = 1,
         sealing_started_at = COALESCE(sealing_started_at, NOW())
     WHERE id = $1`,
    [houseId]
  );

  await client.query(
    `INSERT INTO house_maintenance (
      house_id,
      coating_version,
      coating_applied_at,
      ttl_expires_at,
      alert_at,
      ttl_days,
      improved_recipe,
      status,
      next_ttl_multiplier,
      updated_at
    ) VALUES ($1, 1, $2, $3, $4, $5, $6::jsonb, 'ok', 2.0, NOW())
    ON CONFLICT (house_id) DO UPDATE
    SET coating_applied_at = EXCLUDED.coating_applied_at,
        ttl_expires_at = EXCLUDED.ttl_expires_at,
        alert_at = EXCLUDED.alert_at,
        ttl_days = EXCLUDED.ttl_days,
        improved_recipe = EXCLUDED.improved_recipe,
        status = 'ok',
        updated_at = NOW()`,
    [houseId, coatingApplied.toISOString(), ttlExpires.toISOString(), alertAt.toISOString(), ttlDays, JSON.stringify(row.recipe || defaultRecipeForSoil(soilSignature))]
  );

  return { ttlDays, ttlExpires: ttlExpires.toISOString(), alertAt: alertAt.toISOString() };
}

async function triggerMaintenanceTx(client, houseId) {
  const maintenance = await client.query(
    `SELECT *
     FROM house_maintenance
     WHERE house_id = $1`,
    [houseId]
  );

  const row = maintenance.rows[0];
  if (!row) return null;

  await client.query(
    `UPDATE house_maintenance
     SET status = 'recoating', updated_at = NOW()
     WHERE house_id = $1`,
    [houseId]
  );

  const newVersion = Number(row.coating_version) + 1;
  const ttlMultiplier = Number(row.next_ttl_multiplier || 2);
  const newTTL = Math.max(365, Math.round(Number(row.ttl_days || 365) * ttlMultiplier));

  const newApplied = new Date();
  const newExpiry = new Date(newApplied.getTime() + newTTL * 86400000);
  const newAlert = new Date(newExpiry.getTime() - 180 * 86400000);

  await client.query(
    `UPDATE house_maintenance
     SET coating_version = $2,
         coating_applied_at = $3,
         ttl_days = $4,
         ttl_expires_at = $5,
         alert_at = $6,
         status = 'ok',
         next_ttl_multiplier = LEAST(next_ttl_multiplier * 1.5, 10),
         updated_at = NOW()
     WHERE house_id = $1`,
    [houseId, newVersion, newApplied.toISOString(), newTTL, newExpiry.toISOString(), newAlert.toISOString()]
  );

  return { houseId, newVersion, newTTL, newExpiry: newExpiry.toISOString(), newAlert: newAlert.toISOString() };
}

async function getMaintenanceAlertsTx(client, withinDays = 180) {
  const alertWindow = new Date(Date.now() + withinDays * 86400000).toISOString();
  const result = await client.query(
    `SELECT hm.*, h.name AS house_name
     FROM house_maintenance hm
     JOIN houses h ON h.id = hm.house_id
     WHERE h.is_active = TRUE
       AND hm.alert_at IS NOT NULL
       AND hm.alert_at < $1
       AND hm.status IN ('ok', 'alert')
     ORDER BY hm.alert_at ASC`,
    [alertWindow]
  );

  return result.rows;
}

async function maybeRunIdleWeathering(client) {
  const now = Date.now();
  if (now - lastWeatheringRunAt < WEATHERING_IDLE_SECONDS * 1000) return;

  const idleRobots = await client.query(`SELECT COUNT(*)::int AS count FROM robots WHERE status = 'idle'`);
  if (Number(idleRobots.rows[0]?.count || 0) <= 0) return;

  const candidate = await client.query(
    `SELECT soil_signature
     FROM soil_library
     ORDER BY long_term_confidence ASC NULLS FIRST, updated_at ASC
     LIMIT 1`
  );

  const soilSignature = candidate.rows[0]?.soil_signature;
  if (!soilSignature) return;

  await weatheringTestTx(client, soilSignature);
  lastWeatheringRunAt = now;
}

async function maybeRunMaintenance(client) {
  const now = Date.now();
  if (now - lastMaintenanceRunAt < MAINTENANCE_CHECK_SECONDS * 1000) return;

  const alerts = await getMaintenanceAlertsTx(client, 180);
  if (!alerts.length) {
    lastMaintenanceRunAt = now;
    return;
  }

  await client.query(
    `UPDATE house_maintenance
     SET status = 'alert', updated_at = NOW()
     WHERE house_id = ANY($1::int[])
       AND status = 'ok'`,
    [alerts.map((alert) => alert.house_id)]
  );

  const idleRobots = await client.query(
    `SELECT id
     FROM robots
     WHERE status = 'idle'
     ORDER BY id`
  );

  let budget = idleRobots.rowCount;
  for (const alert of alerts) {
    if (budget <= 0) break;
    await triggerMaintenanceTx(client, alert.house_id);
    budget -= 1;
  }

  lastMaintenanceRunAt = now;
}
function simulateInsertion(componentType) {
  const base = COMPONENT_BASE_IMPEDANCE[componentType] ?? 0.5;
  let retries = 0;
  let resistance = base;

  while (retries < MAX_INSERTION_RETRIES) {
    resistance = base + Math.random() * IMPEDANCE_VARIANCE;
    if (resistance <= IMPEDANCE_THRESHOLD) {
      return { success: true, retries, resistance };
    }
    retries += 1;
  }

  return { success: false, retries, resistance };
}

async function releaseHouseCluster(client, houseId) {
  await client.query(
    `UPDATE robot_clusters
     SET status = 'idle', current_house_id = NULL, updated_at = NOW()
     WHERE current_house_id = $1`,
    [houseId]
  );

  await client.query(
    `UPDATE robots
     SET active_house_id = NULL, status = 'idle', updated_at = NOW()
     WHERE active_house_id = $1`,
    [houseId]
  );
}

async function reconcileAssignments(client) {
  await client.query(`
    UPDATE robot_clusters rc
    SET status = 'idle', current_house_id = NULL, updated_at = NOW()
    WHERE rc.current_house_id IS NULL
       OR NOT EXISTS (
         SELECT 1
         FROM houses h
         WHERE h.id = rc.current_house_id
           AND h.is_active = TRUE
           AND h.stage <> 'complete'
       )
  `);

  await client.query(`
    UPDATE houses h
    SET cluster_id = NULL
    WHERE h.is_active = TRUE
      AND h.stage <> 'complete'
      AND h.cluster_id IS NOT NULL
      AND NOT EXISTS (
        SELECT 1
        FROM robot_clusters rc
        WHERE rc.id = h.cluster_id
          AND rc.current_house_id = h.id
      )
  `);

  await client.query(`
    UPDATE robots r
    SET active_house_id = rc.current_house_id,
        status = CASE WHEN rc.current_house_id IS NULL THEN 'idle' ELSE r.status END,
        updated_at = NOW()
    FROM robot_clusters rc
    WHERE rc.id = r.cluster_id
      AND (
        r.active_house_id IS DISTINCT FROM rc.current_house_id
        OR (rc.current_house_id IS NULL AND r.status <> 'idle')
      )
  `);
}

async function assignIdleClusters(client) {
  const idleClusters = await client.query(
    `SELECT id FROM robot_clusters WHERE status = 'idle' AND current_house_id IS NULL ORDER BY id`
  );

  if (!idleClusters.rowCount) return;

  const stageLoad = await client.query(
    `SELECT stage, COUNT(*)::int AS house_count
     FROM houses
     WHERE is_active = TRUE AND stage = ANY($1::text[])
     GROUP BY stage`,
    [CLUSTER_ASSIGNABLE_STAGES]
  );

  const byStage = Object.fromEntries(stageLoad.rows.map((r) => [r.stage, r.house_count]));

  const targetStage = CLUSTER_ASSIGNABLE_STAGES
    .filter((stage) => (byStage[stage] ?? 0) > 0)
    .sort((a, b) => (byStage[a] ?? 0) - (byStage[b] ?? 0))[0];

  if (!targetStage) return;

  const candidateHouses = await client.query(
    `SELECT id
     FROM houses
     WHERE is_active = TRUE
       AND stage = $1
       AND cluster_id IS NULL
     ORDER BY started_at
     LIMIT $2`,
    [targetStage, idleClusters.rowCount]
  );

  for (let i = 0; i < candidateHouses.rowCount; i += 1) {
    const clusterId = idleClusters.rows[i]?.id;
    const houseId = candidateHouses.rows[i]?.id;
    if (!clusterId || !houseId) continue;

    await client.query(`UPDATE houses SET cluster_id = $1 WHERE id = $2`, [clusterId, houseId]);

    await client.query(
      `UPDATE robot_clusters
       SET status = 'busy', current_house_id = $1, updated_at = NOW()
       WHERE id = $2`,
      [houseId, clusterId]
    );

    await client.query(
      `UPDATE robots
       SET active_house_id = $1, updated_at = NOW()
       WHERE cluster_id = $2`,
      [houseId, clusterId]
    );
  }
}

async function reserveCellAndQueueFabrication(client, robot) {
  if (!robot.active_house_id) return null;

  const houseRes = await client.query(`SELECT stage FROM houses WHERE id = $1`, [robot.active_house_id]);
  const stage = houseRes.rows[0]?.stage;
  if (!stage || stage === "complete") return null;

  const component = STAGE_COMPONENT[stage];
  if (!component) return null;

  const nextCell = await client.query(
    `SELECT id, x, y, z, component_type
     FROM grid_cells
     WHERE house_id = $1
       AND status = 'empty'
       AND component_type = $2
     ORDER BY build_sequence
     LIMIT 1
     FOR UPDATE SKIP LOCKED`,
    [robot.active_house_id, component]
  );

  const cell = nextCell.rows[0];
  if (!cell) return null;

  await client.query(
    `UPDATE grid_cells
     SET status = 'reserved', assigned_robot_id = $1, reserved_at = NOW()
     WHERE id = $2`,
    [robot.id, cell.id]
  );

  await client.query(
    `INSERT INTO fabricator_queue (house_id, cell_id, component_type, status, ready_at)
     VALUES ($1, $2, $3, 'queued', NOW() + make_interval(secs => $4))`,
    [robot.active_house_id, cell.id, cell.component_type, FABRICATION_SECONDS]
  );

  return cell;
}

function movementSeconds(robot, cell) {
  const dist = Math.abs(robot.pos_x - cell.x) + Math.abs(robot.pos_y - cell.y) + Math.abs(robot.pos_z - cell.z);
  return dist * MOVE_SECONDS_PER_STEP + PLACE_SECONDS;
}

async function runSitePrepStep(client, robot) {
  const terrainJob = await client.query(
    `SELECT id, x, y, target_grade, current_grade, soil_density, compaction_score, status, obstacle_type, obstacle_cleared
     FROM terrain_cells
     WHERE house_id = $1
       AND status <> 'ready'
     ORDER BY CASE WHEN obstacle_type <> 'none' AND obstacle_cleared = FALSE THEN 0 ELSE 1 END,
              CASE status
                WHEN 'raw' THEN 0
                WHEN 'grading' THEN 1
                WHEN 'compacted' THEN 2
                ELSE 3
              END,
              y, x
     LIMIT 1
     FOR UPDATE SKIP LOCKED`,
    [robot.active_house_id]
  );

  const cell = terrainJob.rows[0];

  if (!cell) {
    await client.query(
      `UPDATE robots
       SET status = 'idle', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  let currentGrade = Number(cell.current_grade);
  let compaction = Number(cell.compaction_score);
  const targetGrade = Number(cell.target_grade);
  const soilDensity = Number(cell.soil_density);

  const obstacleType = cell.obstacle_type || "none";
  let obstacleCleared = Boolean(cell.obstacle_cleared);

  let nextStatus = cell.status;
  let gradingWork = 0;
  let compactWork = 0;
  let obstacleWork = 0;

  if (obstacleType !== "none" && !obstacleCleared) {
    obstacleWork = 1;
    const clearChance = SITE_PREP_OBSTACLE_CLEAR_CHANCE[obstacleType] ?? 0.65;

    if (Math.random() <= clearChance) {
      obstacleCleared = true;
      currentGrade += (targetGrade - currentGrade) * 0.22;
      nextStatus = "raw";
    } else {
      compaction = Math.max(0, compaction - 0.03);
      nextStatus = "raw";
    }
  }

  if (obstacleCleared || obstacleType === "none") {
    const gradeError = targetGrade - currentGrade;
    if (Math.abs(gradeError) > GRADE_TOLERANCE) {
      const stepCap = GRADING_STEP * (obstacleType === "rock" ? 0.78 : 1);
      const step = Math.sign(gradeError) * Math.min(Math.abs(gradeError), stepCap + Math.random() * 0.04);
      currentGrade += step;
      nextStatus = "grading";
      gradingWork = 1;
    }

    const remainingError = Math.abs(targetGrade - currentGrade);
    if (remainingError <= GRADE_TOLERANCE) {
      const soilFactor = Math.max(0.62, 1.34 - soilDensity);
      const gain = COMPACTION_STEP * soilFactor * (0.84 + Math.random() * 0.32);
      compaction = Math.min(1, compaction + gain);
      compactWork = 1;
      nextStatus = compaction >= COMPACTION_THRESHOLD ? "ready" : "compacted";
    } else {
      compaction = Math.max(0, compaction - 0.01);
    }
  }

  const moveDist = Math.abs(robot.pos_x - cell.x) + Math.abs(robot.pos_y - cell.y) + Math.abs(robot.pos_z - 0);
  const obstacleSeconds = obstacleWork ? SITE_PREP_OBSTACLE_SECONDS[obstacleType] ?? 4 : 0;
  const workSec = Math.max(
    3,
    Math.round(moveDist * MOVE_SECONDS_PER_STEP + SITE_PREP_BASE_SECONDS + gradingWork * SITE_GRADE_SECONDS + compactWork * SITE_COMPACT_SECONDS + obstacleSeconds)
  );

  await client.query(
    `UPDATE terrain_cells
     SET current_grade = $2,
         compaction_score = $3,
         status = $4,
         obstacle_cleared = $5,
         assigned_robot_id = $6,
         updated_at = NOW()
     WHERE id = $1`,
    [cell.id, currentGrade, compaction, nextStatus, obstacleCleared, robot.id]
  );

  await client.query(
    `UPDATE robots
     SET status = 'moving',
         pos_x = $2,
         pos_y = $3,
         pos_z = 0,
         busy_until = NOW() + make_interval(secs => $4),
         total_work_seconds = total_work_seconds + $4,
         updated_at = NOW()
     WHERE id = $1`,
    [robot.id, cell.x, cell.y, workSec]
  );
}

async function runSurveyingStep(client, robot) {
  const house = await client.query(`SELECT id, survey_status FROM houses WHERE id = $1`, [robot.active_house_id]);
  const row = house.rows[0];

  if (!row) {
    await client.query(
      `UPDATE robots SET status = 'idle', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW() WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  if (row.survey_status === 'pending' || row.survey_status === 'surveying') {
    await surveyHouseTx(client, row.id);
  }

  const x = Math.floor(Math.random() * 10);
  const y = Math.floor(Math.random() * 10);
  const workSec = Math.max(4, Math.round(6 + Math.random() * 6));

  await client.query(
    `UPDATE robots
     SET status = 'moving',
         pos_x = $2,
         pos_y = $3,
         pos_z = 0,
         busy_until = NOW() + make_interval(secs => $4),
         total_work_seconds = total_work_seconds + $4,
         updated_at = NOW()
     WHERE id = $1`,
    [robot.id, x, y, workSec]
  );
}

async function runSealingRobotStep(client, robot) {
  const x = Math.floor(Math.random() * 10);
  const y = Math.floor(Math.random() * 10);
  const workSec = Math.max(4, Math.round(5 + Math.random() * 4));

  await client.query(
    `UPDATE robots
     SET status = 'placing',
         pos_x = $2,
         pos_y = $3,
         pos_z = 4,
         busy_until = NOW() + make_interval(secs => $4),
         total_work_seconds = total_work_seconds + $4,
         updated_at = NOW()
     WHERE id = $1`,
    [robot.id, x, y, workSec]
  );
}

async function ensureCommunityKits(client, houseId) {
  await client.query(
    `INSERT INTO assembly_kits (house_id, kit_index, family_label, status, progress, retries, updated_at)
     SELECT $1,
            gs,
            CONCAT('Family ', (($1 + gs) % 9) + 1),
            'queued',
            0,
            0,
            NOW()
     FROM generate_series(1, $2::int) gs
     ON CONFLICT (house_id, kit_index) DO NOTHING`,
    [houseId, COMMUNITY_KITS_PER_HOUSE]
  );
}

async function pickClusterForCommunityRobot(client) {
  const existing = await client.query(`
    SELECT rc.id, COUNT(r.id)::int AS robot_count
    FROM robot_clusters rc
    LEFT JOIN robots r ON r.cluster_id = rc.id
    GROUP BY rc.id
    ORDER BY robot_count ASC, rc.id ASC
    LIMIT 1
  `);

  if (existing.rowCount) {
    return existing.rows[0].id;
  }

  const created = await client.query(
    `INSERT INTO robot_clusters (name, status, updated_at)
     VALUES ($1, 'idle', NOW())
     RETURNING id`,
    [`Community Cluster ${Date.now()}`]
  );

  return created.rows[0].id;
}

async function activateCommunityKit(client, kit) {
  const clusterId = await pickClusterForCommunityRobot(client);

  const robot = await client.query(
    `INSERT INTO robots (cluster_id, name, status, pos_x, pos_y, pos_z, busy_until, active_house_id, updated_at)
     VALUES ($1, $2, 'idle', 0, 0, 0, NULL, NULL, NOW())
     RETURNING id`,
    [clusterId, `Community H${kit.house_id} Kit${kit.kit_index} ${Date.now()}`]
  );

  await client.query(
    `UPDATE assembly_kits
     SET status = 'activated',
         progress = 1,
         activated_robot_id = $2,
         completed_at = NOW(),
         updated_at = NOW()
     WHERE id = $1`,
    [kit.id, robot.rows[0].id]
  );
}

async function runCommunityAssemblyStep(client) {
  const houses = await client.query(
    `SELECT id
     FROM houses
     WHERE is_active = TRUE
       AND stage = 'community_assembly'
     ORDER BY started_at, id`
  );

  for (const house of houses.rows) {
    await ensureCommunityKits(client, house.id);

    const nextKit = await client.query(
      `SELECT id, house_id, kit_index, status, progress, retries
       FROM assembly_kits
       WHERE house_id = $1
         AND status <> 'activated'
       ORDER BY CASE status
         WHEN 'queued' THEN 0
         WHEN 'assembling' THEN 1
         WHEN 'qa' THEN 2
         WHEN 'failed' THEN 3
         WHEN 'passed' THEN 4
         ELSE 5
       END,
       kit_index
       LIMIT 1
       FOR UPDATE SKIP LOCKED`,
      [house.id]
    );

    const kit = nextKit.rows[0];
    if (!kit) continue;

    const progress = Number(kit.progress || 0);

    if (kit.status === "queued" || kit.status === "assembling") {
      const delta = kit.status === "queued"
        ? randRange(COMMUNITY_ASSEMBLY_MIN_PROGRESS + 0.08, COMMUNITY_ASSEMBLY_MAX_PROGRESS)
        : randRange(COMMUNITY_ASSEMBLY_MIN_PROGRESS, COMMUNITY_ASSEMBLY_MAX_PROGRESS - 0.06);

      const nextProgress = Math.min(1, progress + delta);
      const nextStatus = nextProgress >= 1 ? "qa" : "assembling";

      await client.query(
        `UPDATE assembly_kits
         SET status = $2,
             progress = $3,
             started_at = COALESCE(started_at, NOW()),
             updated_at = NOW()
         WHERE id = $1`,
        [kit.id, nextStatus, nextProgress]
      );
      continue;
    }

    if (kit.status === "qa") {
      const passChance = Math.max(0.45, COMMUNITY_QA_BASE_PASS - Number(kit.retries || 0) * 0.1);
      if (Math.random() <= passChance) {
        await client.query(
          `UPDATE assembly_kits
           SET status = 'passed', progress = 1, updated_at = NOW()
           WHERE id = $1`,
          [kit.id]
        );
      } else {
        await client.query(
          `UPDATE assembly_kits
           SET status = 'failed',
               retries = retries + 1,
               progress = GREATEST(0.5, progress - 0.2),
               updated_at = NOW()
           WHERE id = $1`,
          [kit.id]
        );
      }
      continue;
    }

    if (kit.status === "failed") {
      await client.query(
        `UPDATE assembly_kits
         SET status = 'assembling',
             progress = GREATEST(0.35, progress - 0.1),
             updated_at = NOW()
         WHERE id = $1`,
        [kit.id]
      );
      continue;
    }

    if (kit.status === "passed") {
      await activateCommunityKit(client, kit);
    }
  }
}

async function runRobotStep(client, robot) {
  if (!robot.active_house_id) {
    await client.query(
      `UPDATE robots
       SET status = 'idle', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  const houseRes = await client.query(`SELECT id, stage FROM houses WHERE id = $1`, [robot.active_house_id]);
  const stage = houseRes.rows[0]?.stage;

  if (!stage || stage === "complete") {
    await client.query(
      `UPDATE robots
       SET status = 'idle', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  if (stage === "site_prep") {
    await runSitePrepStep(client, robot);
    return;
  }

  if (stage === "surveying") {
    await runSurveyingStep(client, robot);
    return;
  }

  if (stage === "sealing") {
    await runSealingRobotStep(client, robot);
    return;
  }

  if (stage === "community_assembly") {
    await client.query(
      `UPDATE robots
       SET status = 'idle', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  const availableJob = await client.query(
    `SELECT fq.id AS queue_id, fq.cell_id, gc.x, gc.y, gc.z, gc.component_type
     FROM fabricator_queue fq
     JOIN grid_cells gc ON gc.id = fq.cell_id
     WHERE fq.house_id = $1
       AND fq.status IN ('queued', 'ready')
       AND gc.assigned_robot_id = $2
     ORDER BY fq.created_at
     LIMIT 1`,
    [robot.active_house_id, robot.id]
  );

  const job = availableJob.rows[0];

  if (!job) {
    const reserved = await client.query(
      `SELECT 1 FROM grid_cells WHERE assigned_robot_id = $1 AND status = 'reserved' LIMIT 1`,
      [robot.id]
    );

    if (!reserved.rowCount) {
      await reserveCellAndQueueFabrication(client, robot);
    }

    await client.query(
      `UPDATE robots
       SET status = 'idle', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  const readyRes = await client.query(
    `UPDATE fabricator_queue
     SET status = CASE WHEN ready_at <= NOW() THEN 'ready' ELSE status END
     WHERE id = $1
     RETURNING status`,
    [job.queue_id]
  );

  if (readyRes.rows[0]?.status !== "ready") {
    await client.query(
      `UPDATE robots
       SET status = 'waiting_component', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  const soilSignature = await ensureHouseSoilSignature(client, robot.active_house_id);
  const qc = await verifyBlockTx(client, { houseId: robot.active_house_id, soilSignature });

  if (!qc.passed) {
    const failSec = Math.max(4, 4 + Number(qc.retries || 0) * RETRY_ADJUST_SECONDS);

    await client.query(
      `UPDATE grid_cells
       SET status = 'empty', assigned_robot_id = NULL, reserved_at = NULL
       WHERE id = $1`,
      [job.cell_id]
    );

    await client.query(
      `UPDATE fabricator_queue
       SET status = 'consumed', consumed_at = NOW()
       WHERE id = $1`,
      [job.queue_id]
    );

    await client.query(
      `UPDATE robots
       SET status = 'waiting_component',
           busy_until = NOW() + make_interval(secs => $2),
           total_work_seconds = total_work_seconds + $2,
           updated_at = NOW()
       WHERE id = $1`,
      [robot.id, failSec]
    );

    return;
  }

  const cell = { x: job.x, y: job.y, z: job.z };
  const insertion = simulateInsertion(job.component_type);
  const retrySeconds = insertion.retries * RETRY_ADJUST_SECONDS;
  const failurePenalty = insertion.success ? 0 : FAILED_INSERTION_PENALTY_SECONDS;
  const workSec = movementSeconds(robot, cell) + retrySeconds + failurePenalty;

  await client.query(
    `UPDATE robots
     SET status = 'placing',
         pos_x = $2,
         pos_y = $3,
         pos_z = $4,
         busy_until = NOW() + make_interval(secs => $5),
         total_work_seconds = total_work_seconds + $5,
         total_placements = total_placements + 1,
         total_placement_retries = total_placement_retries + $6,
         total_placement_failures = total_placement_failures + $7,
         updated_at = NOW()
     WHERE id = $1`,
    [robot.id, cell.x, cell.y, cell.z, workSec, insertion.retries, insertion.success ? 0 : 1]
  );

  if (insertion.success) {
    await client.query(`UPDATE grid_cells SET status = 'filled', filled_at = NOW() WHERE id = $1`, [job.cell_id]);
  } else {
    await client.query(
      `UPDATE grid_cells
       SET status = 'empty', assigned_robot_id = NULL, reserved_at = NULL
       WHERE id = $1`,
      [job.cell_id]
    );
  }

  await client.query(
    `UPDATE fabricator_queue
     SET status = 'consumed', consumed_at = NOW()
     WHERE id = $1`,
    [job.queue_id]
  );
}

async function progressHouseStages(client) {
  const houses = await client.query(
    `SELECT id, stage, survey_status, soil_signature, sealing_progress
     FROM houses
     WHERE is_active = TRUE
       AND stage <> 'complete'
     ORDER BY started_at, id`
  );

  for (const house of houses.rows) {
    if (house.stage === "site_prep") {
      const remainingTerrain = await client.query(
        `SELECT COUNT(*)::int AS remaining
         FROM terrain_cells
         WHERE house_id = $1
           AND (status <> 'ready' OR (obstacle_type <> 'none' AND obstacle_cleared = FALSE))`,
        [house.id]
      );

      if (remainingTerrain.rows[0].remaining > 0) {
        continue;
      }

      await client.query(
        `UPDATE houses
         SET stage = 'surveying',
             survey_status = 'pending'
         WHERE id = $1`,
        [house.id]
      );
      continue;
    }

    if (house.stage === "surveying") {
      if (house.survey_status === "pending" || house.survey_status === "surveying") {
        continue;
      }

      if (house.survey_status === "rejected") {
        await client.query(
          `UPDATE terrain_cells
           SET current_grade = target_grade + ((random() - 0.5) * 0.7),
               compaction_score = 0.2 + random() * 0.25,
               status = 'raw',
               obstacle_type = CASE
                 WHEN random() < 0.08 THEN 'rock'
                 WHEN random() < 0.18 THEN 'root'
                 WHEN random() < 0.28 THEN 'debris'
                 ELSE 'none'
               END,
               obstacle_cleared = CASE WHEN random() < 0.55 THEN FALSE ELSE TRUE END,
               assigned_robot_id = NULL,
               updated_at = NOW()
           WHERE house_id = $1`,
          [house.id]
        );

        await client.query(
          `UPDATE houses
           SET stage = 'site_prep',
               survey_status = 'pending',
               site_zone = 'MARGINAL'
           WHERE id = $1`,
          [house.id]
        );
        continue;
      }

      await client.query(`UPDATE houses SET stage = 'foundation' WHERE id = $1`, [house.id]);
      continue;
    }

    if (house.stage === "sealing") {
      const assignedRobots = await client.query(
        `SELECT COUNT(*)::int AS count
         FROM robots
         WHERE active_house_id = $1`,
        [house.id]
      );

      const robotCount = Number(assignedRobots.rows[0]?.count || 0);
      const increment = Math.max(SEALING_STEP_MIN, robotCount * SEALING_STEP_PER_ROBOT);
      const progress = Math.min(1, Number(house.sealing_progress || 0) + increment);

      await client.query(
        `UPDATE houses
         SET sealing_progress = $2,
             sealing_started_at = COALESCE(sealing_started_at, NOW())
         WHERE id = $1`,
        [house.id, progress]
      );

      if (progress < 1) {
        continue;
      }

      const soilSignature = house.soil_signature || (await ensureHouseSoilSignature(client, house.id));
      await completeSealingTx(client, { houseId: house.id, soilSignature });

      await client.query(
        `UPDATE houses
         SET stage = 'community_assembly'
         WHERE id = $1`,
        [house.id]
      );

      await releaseHouseCluster(client, house.id);
      await ensureCommunityKits(client, house.id);
      continue;
    }

    if (house.stage === "community_assembly") {
      await ensureCommunityKits(client, house.id);

      const remainingKits = await client.query(
        `SELECT COUNT(*)::int AS remaining
         FROM assembly_kits
         WHERE house_id = $1
           AND status <> 'activated'`,
        [house.id]
      );

      if (remainingKits.rows[0].remaining > 0) {
        continue;
      }

      await client.query(
        `UPDATE houses
         SET stage = 'complete', completed_at = NOW(), cluster_id = NULL
         WHERE id = $1`,
        [house.id]
      );

      await releaseHouseCluster(client, house.id);
      continue;
    }

    const component = STAGE_COMPONENT[house.stage];
    if (!component) continue;

    const remaining = await client.query(
      `SELECT COUNT(*)::int AS remaining
       FROM grid_cells
       WHERE house_id = $1
         AND component_type = $2
         AND status <> 'filled'`,
      [house.id, component]
    );

    if (remaining.rows[0].remaining > 0) continue;

    const stage = nextStage(house.stage);

    if (stage === "sealing") {
      await client.query(
        `UPDATE houses
         SET stage = 'sealing',
             sealing_progress = 0,
             sealing_started_at = NOW(),
             sealing_complete = FALSE
         WHERE id = $1`,
        [house.id]
      );
      continue;
    }

    if (stage === "community_assembly") {
      await client.query(`UPDATE houses SET stage = 'community_assembly', cluster_id = NULL WHERE id = $1`, [house.id]);
      await releaseHouseCluster(client, house.id);
      await ensureCommunityKits(client, house.id);
      continue;
    }

    const isComplete = stage === "complete";

    await client.query(
      `UPDATE houses
       SET stage = $2,
           completed_at = CASE WHEN $2 = 'complete' THEN NOW() ELSE completed_at END,
           cluster_id = CASE WHEN $2 = 'complete' THEN NULL ELSE cluster_id END
       WHERE id = $1`,
      [house.id, stage]
    );

    if (isComplete) {
      await releaseHouseCluster(client, house.id);
    }
  }
}

export async function schedulerTick() {
  await withTx(async (client) => {
    await reconcileAssignments(client);
    await assignIdleClusters(client);

    const robotsRes = await client.query(
      `SELECT id, active_house_id, pos_x, pos_y, pos_z
       FROM robots
       ORDER BY id`
    );

    for (const robot of robotsRes.rows) {
      await runRobotStep(client, robot);
    }

    await runCommunityAssemblyStep(client);
    await progressHouseStages(client);
    await maybeRunIdleWeathering(client);
    await maybeRunMaintenance(client);
  });

  await sampleMetrics();
}

export async function sampleMetrics() {
  const [counts, robotTime, houseTime, placementStats, terrainStats, communityStats, surveyStats, soilStats, blockStats, maintenanceStats] = await Promise.all([
    pool.query(`
      SELECT
        COUNT(DISTINCT h.id)::int AS active_houses,
        COUNT(DISTINCT h.id) FILTER (WHERE h.stage = 'complete')::int AS houses_completed,
        COUNT(*) FILTER (WHERE g.status = 'filled')::int AS cells_filled,
        COUNT(*)::int AS total_cells,
        (SELECT COUNT(*)::int FROM robots) AS active_robots
      FROM houses h
      JOIN grid_cells g ON g.house_id = h.id
      WHERE h.is_active = TRUE
    `),
    pool.query(`
      SELECT
        COALESCE(SUM(total_idle_seconds),0)::float AS idle,
        COALESCE(SUM(total_work_seconds),0)::float AS work
      FROM robots
    `),
    pool.query(`
      SELECT
        COALESCE(EXTRACT(EPOCH FROM (NOW() - MIN(started_at))) / 3600.0, 0) AS elapsed_hours
      FROM houses
      WHERE is_active = TRUE
    `),
    pool.query(`
      SELECT
        COALESCE(SUM(total_placement_retries),0)::int AS corrections,
        COALESCE(SUM(total_placement_failures),0)::int AS failures,
        COALESCE(SUM(total_placements),0)::int AS placements
      FROM robots
    `),
    pool.query(`
      SELECT
        COUNT(*)::int AS terrain_total,
        COUNT(*) FILTER (WHERE t.status = 'ready')::int AS terrain_ready,
        COUNT(*) FILTER (WHERE t.obstacle_type <> 'none' AND t.obstacle_cleared = FALSE)::int AS obstacles_remaining,
        COALESCE(AVG(ABS(t.current_grade - t.target_grade)), 0)::float AS avg_grade_error,
        COALESCE(AVG(t.compaction_score), 0)::float AS avg_compaction
      FROM terrain_cells t
      JOIN houses h ON h.id = t.house_id
      WHERE h.is_active = TRUE
    `),
    pool.query(`
      SELECT
        COALESCE(COUNT(*) FILTER (WHERE ak.status = 'activated'), 0)::int AS kits_activated,
        COALESCE(COUNT(*) FILTER (WHERE ak.status = 'failed'), 0)::int AS kits_failed,
        COALESCE(COUNT(*) FILTER (WHERE ak.status <> 'activated'), 0)::int AS kits_pending
      FROM assembly_kits ak
      JOIN houses h ON h.id = ak.house_id
      WHERE h.is_active = TRUE
    `),
    pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE survey_status = 'surveying')::int AS active_surveys
      FROM houses
      WHERE is_active = TRUE
    `),
    pool.query(`
      SELECT COUNT(*)::int AS soil_recipes_learned
      FROM soil_library
    `),
    pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE verification_mode = 'inline' AND passed = TRUE)::int AS blocks_verified,
        COUNT(*) FILTER (WHERE verification_mode = 'inline' AND passed = FALSE)::int AS blocks_failed_qc
      FROM block_verifications
    `),
    pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE hm.status IN ('alert', 'recoating'))::int AS houses_in_maintenance,
        COALESCE(AVG(hm.ttl_days), 0)::float AS avg_ttl_days
      FROM house_maintenance hm
      JOIN houses h ON h.id = hm.house_id
      WHERE h.is_active = TRUE
    `)
  ]);

  const c = counts.rows[0];
  const t = robotTime.rows[0];
  const p = placementStats.rows[0];
  const terrain = terrainStats.rows[0];
  const community = communityStats.rows[0];

  const elapsedHours = Number(houseTime.rows[0]?.elapsed_hours || 0);
  const totalTime = Number(t.idle) + Number(t.work);
  const idlePercent = totalTime > 0 ? (Number(t.idle) / totalTime) * 100 : 0;
  const throughput = elapsedHours > 0 ? Number(c.cells_filled) / elapsedHours : 0;
  const totalCells = Number(c.total_cells || 0);
  const filledCells = Number(c.cells_filled || 0);
  const pipelineEfficiency = totalCells === 0 ? 0 : (filledCells / totalCells) * 100;
  const avgRetries = Number(p.placements) > 0 ? Number(p.corrections) / Number(p.placements) : 0;

  const terrainTotal = Number(terrain.terrain_total || 0);
  const terrainReady = Number(terrain.terrain_ready || 0);
  const terrainReadyPercent = terrainTotal > 0 ? (terrainReady / terrainTotal) * 100 : 100;

  await pool.query(
    `INSERT INTO metrics_samples
      (active_houses, active_robots, houses_completed, cells_filled, total_cells, robot_idle_percent, throughput_cells_per_hour, pipeline_efficiency, placement_corrections, placement_failures, avg_retries_per_placement, terrain_ready_percent, avg_grade_error, avg_compaction, obstacle_cells_remaining, community_kits_activated, community_kits_failed, community_kits_pending, active_surveys, soil_recipes_learned, blocks_verified, blocks_failed_qc, houses_in_maintenance, avg_ttl_days)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)`,
    [
      c.active_houses,
      c.active_robots,
      c.houses_completed,
      c.cells_filled,
      c.total_cells,
      idlePercent,
      throughput,
      pipelineEfficiency,
      p.corrections,
      p.failures,
      avgRetries,
      terrainReadyPercent,
      terrain.avg_grade_error,
      terrain.avg_compaction,
      terrain.obstacles_remaining,
      community.kits_activated,
      community.kits_failed,
      community.kits_pending,
      Number(surveyStats.rows[0]?.active_surveys || 0),
      Number(soilStats.rows[0]?.soil_recipes_learned || 0),
      Number(blockStats.rows[0]?.blocks_verified || 0),
      Number(blockStats.rows[0]?.blocks_failed_qc || 0),
      Number(maintenanceStats.rows[0]?.houses_in_maintenance || 0),
      Number(maintenanceStats.rows[0]?.avg_ttl_days || 0)
    ]
  );
}

export async function systemState() {
  const [houses, robots, queue, metrics, cells, terrainCounts, terrainObstacles, assemblyQueue, assemblyCounts, maintenance, surveySummary, causalGlobal, causalByHouse] = await Promise.all([
    pool.query(`SELECT * FROM houses WHERE is_active = TRUE ORDER BY id`),
    pool.query(`SELECT * FROM robots ORDER BY id`),
    pool.query(`
      SELECT status, COUNT(*)::int AS count
      FROM fabricator_queue
      WHERE consumed_at IS NULL
      GROUP BY status
      ORDER BY status
    `),
    pool.query(`SELECT * FROM metrics_samples ORDER BY sampled_at DESC LIMIT 1`),
    pool.query(`
      SELECT g.house_id, g.status, COUNT(*)::int AS count
      FROM grid_cells g
      JOIN houses h ON h.id = g.house_id
      WHERE h.is_active = TRUE
      GROUP BY g.house_id, g.status
      ORDER BY g.house_id, g.status
    `),
    pool.query(`
      SELECT
        t.house_id,
        t.status,
        COUNT(*)::int AS count,
        ROUND(AVG(ABS(t.current_grade - t.target_grade))::numeric, 3) AS avg_grade_error,
        ROUND(AVG(t.compaction_score)::numeric, 3) AS avg_compaction
      FROM terrain_cells t
      JOIN houses h ON h.id = t.house_id
      WHERE h.is_active = TRUE
      GROUP BY t.house_id, t.status
      ORDER BY t.house_id, t.status
    `),
    pool.query(`
      SELECT
        t.house_id,
        COUNT(*) FILTER (WHERE t.obstacle_type <> 'none' AND t.obstacle_cleared = FALSE)::int AS obstacles_remaining
      FROM terrain_cells t
      JOIN houses h ON h.id = t.house_id
      WHERE h.is_active = TRUE
      GROUP BY t.house_id
      ORDER BY t.house_id
    `),
    pool.query(`
      SELECT ak.status, COUNT(*)::int AS count
      FROM assembly_kits ak
      JOIN houses h ON h.id = ak.house_id
      WHERE h.is_active = TRUE
      GROUP BY ak.status
      ORDER BY ak.status
    `),
    pool.query(`
      SELECT ak.house_id, ak.status, COUNT(*)::int AS count
      FROM assembly_kits ak
      JOIN houses h ON h.id = ak.house_id
      WHERE h.is_active = TRUE
      GROUP BY ak.house_id, ak.status
      ORDER BY ak.house_id, ak.status
    `),
    pool.query(`
      SELECT hm.*, h.name AS house_name
      FROM house_maintenance hm
      JOIN houses h ON h.id = hm.house_id
      WHERE h.is_active = TRUE
      ORDER BY hm.alert_at NULLS LAST, hm.house_id
    `),
    pool.query(`
      SELECT
        s.house_id,
        COUNT(*)::int AS probes,
        COUNT(*) FILTER (WHERE s.status = 'BUILDABLE')::int AS buildable,
        COUNT(*) FILTER (WHERE s.status = 'MARGINAL')::int AS marginal,
        COUNT(*) FILTER (WHERE s.status = 'REJECT')::int AS reject
      FROM site_surveys s
      JOIN houses h ON h.id = s.house_id
      WHERE h.is_active = TRUE
      GROUP BY s.house_id
      ORDER BY s.house_id
    `),
    pool.query(`
      WITH survey AS (
        SELECT
          COUNT(*) FILTER (WHERE h.survey_status IN ('approved', 'rejected'))::int AS surveyed_houses,
          COUNT(*) FILTER (WHERE h.survey_status = 'approved')::int AS approved_sites,
          COUNT(*) FILTER (WHERE h.survey_status = 'rejected')::int AS rejected_sites
        FROM houses h
        WHERE h.is_active = TRUE
      ),
      qc AS (
        SELECT
          COUNT(*) FILTER (WHERE verification_mode = 'inline')::int AS inline_total,
          COUNT(*) FILTER (WHERE verification_mode = 'inline' AND passed = TRUE)::int AS inline_passed,
          COUNT(*) FILTER (WHERE verification_mode = 'inline' AND passed = FALSE)::int AS inline_failed,
          COALESCE(AVG(retries) FILTER (WHERE verification_mode = 'inline'), 0)::numeric AS inline_avg_retries
        FROM block_verifications
      ),
      confidence AS (
        SELECT
          COALESCE(AVG(short_term_confidence), 0)::numeric AS avg_short_confidence,
          COALESCE(AVG(long_term_confidence), 0)::numeric AS avg_long_confidence
        FROM soil_library
      ),
      ttl AS (
        SELECT
          COALESCE(AVG(hm.ttl_days), 0)::numeric AS avg_ttl_days,
          COALESCE(MIN(hm.ttl_days), 0)::int AS min_ttl_days,
          COALESCE(MAX(hm.ttl_days), 0)::int AS max_ttl_days,
          COUNT(*) FILTER (WHERE hm.status IN ('alert', 'recoating'))::int AS maintenance_load,
          COALESCE(AVG(hm.coating_version), 0)::numeric AS avg_coating_version
        FROM house_maintenance hm
        JOIN houses h ON h.id = hm.house_id
        WHERE h.is_active = TRUE
      ),
      moisture AS (
        SELECT
          COALESCE(AVG(LEAST(1.0, GREATEST(0.0,
            (1 - COALESCE(sl.long_term_confidence, 0)) * 0.55
            + COALESCE(sl.salinity, 0) * 0.08
            + CASE WHEN h.sealing_complete THEN -0.18 ELSE 0.14 END
            + CASE
              WHEN h.survey_status = 'rejected' THEN 0.2
              WHEN h.survey_status = 'approved' THEN -0.04
              ELSE 0.08
            END
          ))), 0)::numeric AS avg_moisture_risk,
          COALESCE(AVG(LEAST(1.0, GREATEST(0.0,
            (1 - COALESCE(sl.long_term_confidence, 0)) * 0.55
            + COALESCE(sl.salinity, 0) * 0.08
            + CASE WHEN h.sealing_complete THEN -0.18 ELSE 0.14 END
            + CASE
              WHEN h.survey_status = 'rejected' THEN 0.2
              WHEN h.survey_status = 'approved' THEN -0.04
              ELSE 0.08
            END
          ))) FILTER (WHERE h.sealing_complete = TRUE), 0)::numeric AS sealed_moisture_risk,
          COALESCE(AVG(LEAST(1.0, GREATEST(0.0,
            (1 - COALESCE(sl.long_term_confidence, 0)) * 0.55
            + COALESCE(sl.salinity, 0) * 0.08
            + CASE WHEN h.sealing_complete THEN -0.18 ELSE 0.14 END
            + CASE
              WHEN h.survey_status = 'rejected' THEN 0.2
              WHEN h.survey_status = 'approved' THEN -0.04
              ELSE 0.08
            END
          ))) FILTER (WHERE h.sealing_complete = FALSE), 0)::numeric AS unsealed_moisture_risk
        FROM houses h
        LEFT JOIN soil_library sl ON sl.soil_signature = h.soil_signature
        WHERE h.is_active = TRUE
      )
      SELECT
        survey.surveyed_houses,
        survey.approved_sites,
        survey.rejected_sites,
        qc.inline_total,
        qc.inline_passed,
        qc.inline_failed,
        ROUND(qc.inline_avg_retries, 3) AS inline_avg_retries,
        ROUND(confidence.avg_short_confidence, 3) AS avg_short_confidence,
        ROUND(confidence.avg_long_confidence, 3) AS avg_long_confidence,
        ROUND(ttl.avg_ttl_days, 1) AS avg_ttl_days,
        ttl.min_ttl_days,
        ttl.max_ttl_days,
        ttl.maintenance_load,
        ROUND(ttl.avg_coating_version, 2) AS avg_coating_version,
        ROUND(moisture.avg_moisture_risk, 3) AS avg_moisture_risk,
        ROUND(moisture.sealed_moisture_risk, 3) AS sealed_moisture_risk,
        ROUND(moisture.unsealed_moisture_risk, 3) AS unsealed_moisture_risk
      FROM survey, qc, confidence, ttl, moisture
    `),
    pool.query(`
      WITH survey AS (
        SELECT
          s.house_id,
          COUNT(*)::int AS probes,
          COUNT(*) FILTER (WHERE s.status = 'BUILDABLE')::int AS buildable,
          COUNT(*) FILTER (WHERE s.status = 'REJECT')::int AS reject
        FROM site_surveys s
        GROUP BY s.house_id
      ),
      qc AS (
        SELECT
          b.house_id,
          COUNT(*) FILTER (WHERE b.verification_mode = 'inline')::int AS inline_total,
          COUNT(*) FILTER (WHERE b.verification_mode = 'inline' AND b.passed = TRUE)::int AS inline_passed,
          COUNT(*) FILTER (WHERE b.verification_mode = 'inline' AND b.passed = FALSE)::int AS inline_failed,
          COALESCE(AVG(b.retries) FILTER (WHERE b.verification_mode = 'inline'), 0)::numeric AS inline_avg_retries
        FROM block_verifications b
        GROUP BY b.house_id
      )
      SELECT
        h.id AS house_id,
        h.name,
        h.survey_status,
        h.soil_signature,
        h.sealing_complete,
        COALESCE(survey.probes, 0)::int AS survey_probes,
        COALESCE(survey.buildable, 0)::int AS survey_buildable,
        COALESCE(survey.reject, 0)::int AS survey_reject,
        COALESCE(qc.inline_total, 0)::int AS qc_total,
        COALESCE(qc.inline_passed, 0)::int AS qc_passed,
        COALESCE(qc.inline_failed, 0)::int AS qc_failed,
        ROUND(COALESCE(qc.inline_avg_retries, 0), 3) AS qc_avg_retries,
        ROUND(COALESCE(sl.short_term_confidence, 0), 3) AS short_confidence,
        ROUND(COALESCE(sl.long_term_confidence, 0), 3) AS long_confidence,
        hm.ttl_days,
        hm.coating_version,
        hm.status AS maintenance_status,
        ROUND(LEAST(1.0, GREATEST(0.0,
          (1 - COALESCE(sl.long_term_confidence, 0)) * 0.55
          + COALESCE(sl.salinity, 0) * 0.08
          + CASE WHEN h.sealing_complete THEN -0.18 ELSE 0.14 END
          + CASE
            WHEN h.survey_status = 'rejected' THEN 0.2
            WHEN h.survey_status = 'approved' THEN -0.04
            ELSE 0.08
          END
        )), 3) AS moisture_risk
      FROM houses h
      LEFT JOIN survey ON survey.house_id = h.id
      LEFT JOIN qc ON qc.house_id = h.id
      LEFT JOIN soil_library sl ON sl.soil_signature = h.soil_signature
      LEFT JOIN house_maintenance hm ON hm.house_id = h.id
      WHERE h.is_active = TRUE
      ORDER BY h.id
    `)
  ]);

  return {
    houses: houses.rows,
    robots: robots.rows,
    fabricatorQueue: queue.rows,
    assemblyQueue: assemblyQueue.rows,
    metrics: metrics.rows[0] || null,
    cellCounts: cells.rows,
    terrainCounts: terrainCounts.rows,
    terrainObstacles: terrainObstacles.rows,
    assemblyCounts: assemblyCounts.rows,
    maintenance: maintenance.rows,
    surveySummary: surveySummary.rows
  };
}

export async function surveyHouse(houseId) {
  return withTx(async (client) => surveyHouseTx(client, Number(houseId)));
}

export async function verifyBlock({ houseId, soilSignature }) {
  return withTx(async (client) => verifyBlockTx(client, { houseId: Number(houseId), soilSignature }));
}

export async function runWeatheringTest(soilSignature) {
  return withTx(async (client) => weatheringTestTx(client, soilSignature));
}

export async function completeSealingForHouse(houseId, soilSignature) {
  return withTx(async (client) => completeSealingTx(client, { houseId: Number(houseId), soilSignature }));
}

export async function triggerMaintenanceForHouse(houseId) {
  return withTx(async (client) => triggerMaintenanceTx(client, Number(houseId)));
}

export async function getMaintenanceAlerts(withinDays = 180) {
  return pool.query(
    `SELECT hm.*, h.name AS house_name
     FROM house_maintenance hm
     JOIN houses h ON h.id = hm.house_id
     WHERE h.is_active = TRUE
       AND hm.alert_at IS NOT NULL
       AND hm.alert_at < NOW() + make_interval(days => $1)
       AND hm.status IN ('ok', 'alert')
     ORDER BY hm.alert_at ASC`,
    [Number(withinDays)]
  ).then((result) => result.rows);
}





