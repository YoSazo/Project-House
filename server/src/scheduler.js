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
const ASSEMBLY_CALIBRATION_TIGHTEN = Number(process.env.ASSEMBLY_CALIBRATION_TIGHTEN || 0.08);
const ASSEMBLY_PRE_SCAN_TOLERANCE = Number(process.env.ASSEMBLY_PRE_SCAN_TOLERANCE || 0.74);
const ASSEMBLY_PRE_SCAN_TOLERANCE_CAL = Number(process.env.ASSEMBLY_PRE_SCAN_TOLERANCE_CAL || 0.66);
const ASSEMBLY_POST_SEAT_MIN = Number(process.env.ASSEMBLY_POST_SEAT_MIN || 0.58);
const ASSEMBLY_POST_SEAT_MIN_CAL = Number(process.env.ASSEMBLY_POST_SEAT_MIN_CAL || 0.68);
const ASSEMBLY_PRE_SCAN_SECONDS = Number(process.env.ASSEMBLY_PRE_SCAN_SECONDS || 2);
const ASSEMBLY_POST_SEAT_SECONDS = Number(process.env.ASSEMBLY_POST_SEAT_SECONDS || 2);
const ASSEMBLY_CALIBRATION_SLOWDOWN_SECONDS = Number(process.env.ASSEMBLY_CALIBRATION_SLOWDOWN_SECONDS || 4);

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
const SURVEY_GRID_WIDTH = Number(process.env.SURVEY_GRID_WIDTH || 5);
const SURVEY_GRID_STEP = Number(process.env.SURVEY_GRID_STEP || 5);
const SURVEY_MAX_ROUNDS = Number(process.env.SURVEY_MAX_ROUNDS || 4);
const SURVEY_PROBE_BUDGET = Number(process.env.SURVEY_PROBE_BUDGET || 48);
const SURVEY_CONVERGENCE_DELTA = Number(process.env.SURVEY_CONVERGENCE_DELTA || 0.03);
const SURVEY_BOUNDARY_STABILITY = Number(process.env.SURVEY_BOUNDARY_STABILITY || 0.05);
const SEALING_STEP_PER_ROBOT = Number(process.env.SEALING_STEP_PER_ROBOT || 0.06);
const SEALING_STEP_MIN = Number(process.env.SEALING_STEP_MIN || 0.12);
const WEATHERING_IDLE_SECONDS = Number(process.env.WEATHERING_IDLE_SECONDS || 30);
const MAINTENANCE_CHECK_SECONDS = Number(process.env.MAINTENANCE_CHECK_SECONDS || 60);
const SURVEY_CONFIDENCE_BUILDABLE = Number(process.env.SURVEY_CONFIDENCE_BUILDABLE || 0.8);
const SURVEY_CONFIDENCE_MARGINAL = Number(process.env.SURVEY_CONFIDENCE_MARGINAL || 0.55);
const SURVEY_CONFIDENCE_CONVERGED = Number(process.env.SURVEY_CONFIDENCE_CONVERGED || 0.4);
const LOGISTICS_SEGMENT_COUNT = Number(process.env.LOGISTICS_SEGMENT_COUNT || 12);
const LOGISTICS_STALE_RELAY_SECONDS = Number(process.env.LOGISTICS_STALE_RELAY_SECONDS || 120);
const LOGISTICS_CONDITION_GAIN = Number(process.env.LOGISTICS_CONDITION_GAIN || 0.055);
const LOGISTICS_DEGRADE_RATE = Number(process.env.LOGISTICS_DEGRADE_RATE || 0.06);
const LOGISTICS_VERIFY_INTERVAL = Number(process.env.LOGISTICS_VERIFY_INTERVAL || 5);
const LOGISTICS_TRAVEL_FACTOR_MIN = Number(process.env.LOGISTICS_TRAVEL_FACTOR_MIN || 0.72);
const LOGISTICS_TRAVEL_FACTOR_MAX = Number(process.env.LOGISTICS_TRAVEL_FACTOR_MAX || 1.35);

let lastWeatheringRunAt = 0;
let lastMaintenanceRunAt = 0;
const insertionProfileMemory = new Map();

const COMPONENT_BASE_IMPEDANCE = {
  foundation: 0.58,
  wall: 0.5,
  mep: 0.63,
  roof: 0.47
};

const COMPONENT_ROLE_MAP = {
  foundation: "foundation",
  wall: "structural_wall",
  mep: "mep_channel",
  roof: "roof_panel"
};

const COMPONENT_REQUESTED_SPECS = {
  foundation: { penetration_min: 140, moisture_max: 20, density_min: 1850, density_max: 2300, dimensional_tolerance_mm: 6 },
  wall: { penetration_min: 122, moisture_max: 24, density_min: 1720, density_max: 2200, dimensional_tolerance_mm: 8 },
  mep: { penetration_min: 110, moisture_max: 26, density_min: 1660, density_max: 2120, dimensional_tolerance_mm: 10 },
  roof: { penetration_min: 102, moisture_max: 28, density_min: 1600, density_max: 2050, dimensional_tolerance_mm: 11 }
};

const FAST_PASS_RELEASE_CONFIDENCE = Number(process.env.FAST_PASS_RELEASE_CONFIDENCE || 0.93);
const FAST_PASS_PROCESS_MATCH = Number(process.env.FAST_PASS_PROCESS_MATCH || 0.9);
const FAST_PASS_CHARACTERISTIC = Number(process.env.FAST_PASS_CHARACTERISTIC || 0.88);
const DRIFT_ESCALATION_THRESHOLD = Number(process.env.DRIFT_ESCALATION_THRESHOLD || 0.24);
const REWORK_DRIFT_THRESHOLD = Number(process.env.REWORK_DRIFT_THRESHOLD || 0.16);
const PROCESS_SIGNATURE_BLEND = Number(process.env.PROCESS_SIGNATURE_BLEND || 0.25);
const SIGNATURE_MATURITY_MIN_SAMPLES = Number(process.env.SIGNATURE_MATURITY_MIN_SAMPLES || 8);
const SIGNATURE_MATURITY_TARGET_SAMPLES = Number(process.env.SIGNATURE_MATURITY_TARGET_SAMPLES || 40);
const CONTRADICTION_GAP_THRESHOLD = Number(process.env.CONTRADICTION_GAP_THRESHOLD || 0.32);
const CONTRADICTION_LOW_SCORE = Number(process.env.CONTRADICTION_LOW_SCORE || 0.62);

const ROLE_CRITICALITY = {
  foundation: 1.0,
  structural_wall: 0.9,
  roof_panel: 0.82,
  mep_channel: 0.45
};

const ROLE_EXPOSURE = {
  foundation: 0.96,
  structural_wall: 0.86,
  roof_panel: 1.0,
  mep_channel: 0.4
};

function nextStage(stage) {
  const idx = STAGE_ORDER.indexOf(stage);
  return idx >= 0 && idx < STAGE_ORDER.length - 1 ? STAGE_ORDER[idx + 1] : stage;
}

function referencePatchForZone(zone) {
  if (zone === "zone_nw") return { x: 1, y: 1 };
  if (zone === "zone_ne") return { x: 8, y: 1 };
  if (zone === "zone_sw") return { x: 1, y: 8 };
  if (zone === "zone_se") return { x: 8, y: 8 };
  return { x: 1, y: 8 };
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

function requestedSpecForComponent(componentType) {
  return COMPONENT_REQUESTED_SPECS[componentType] || COMPONENT_REQUESTED_SPECS.wall;
}

function roleForComponent(componentType) {
  return COMPONENT_ROLE_MAP[componentType] || "structural_wall";
}

function roleCriticality(role) {
  return ROLE_CRITICALITY[role] ?? 0.7;
}

function roleExposure(role) {
  return ROLE_EXPOSURE[role] ?? 0.65;
}

function signatureMaturityScore(referenceSampleCount, totalVerified) {
  const ref = Math.max(0, Number(referenceSampleCount || 0));
  const total = Math.max(ref, Number(totalVerified || 0));
  const normalized = clamp(ref / Math.max(1, SIGNATURE_MATURITY_TARGET_SAMPLES), 0, 1);
  const reliability = clamp(total / Math.max(1, SIGNATURE_MATURITY_TARGET_SAMPLES * 1.5), 0, 1);
  return clamp((normalized * 0.75) + (reliability * 0.25), 0, 1);
}

function isProcessSensorContradiction(processScore, characteristicScore) {
  const p = Number(processScore || 0);
  const c = Number(characteristicScore || 0);
  const gap = Math.abs(p - c);
  const low = Math.min(p, c);
  const high = Math.max(p, c);

  return gap >= CONTRADICTION_GAP_THRESHOLD && low <= CONTRADICTION_LOW_SCORE && high >= FAST_PASS_PROCESS_MATCH;
}

function numericSimilarity(current, reference, spread = 0.2) {
  if (!Number.isFinite(current) || !Number.isFinite(reference)) return 0;
  const baseline = Math.max(1e-6, Math.abs(reference));
  const deltaRatio = Math.abs(current - reference) / baseline;
  return clamp(1 - (deltaRatio / Math.max(0.01, spread)), 0, 1);
}

function scoreWithinBand(value, min, max) {
  if (!Number.isFinite(value) || !Number.isFinite(min) || !Number.isFinite(max)) return 0;
  if (value >= min && value <= max) return 1;

  const band = Math.max(1, max - min);
  if (value < min) {
    return clamp(1 - ((min - value) / (band * 0.7)), 0, 1);
  }

  return clamp(1 - ((value - max) / (band * 0.7)), 0, 1);
}

function buildProcessSignature({ componentType, recipe, requestedSpec }) {
  const clay = Number(recipe?.clay ?? 40);
  const sand = Number(recipe?.sand ?? 35);
  const lime = Number(recipe?.lime ?? 10);
  const cement = Number(recipe?.cement ?? 5);

  return {
    component_type: componentType,
    mix_temp_c: Number((34 + clay * 0.12 + lime * 0.1 + randRange(-3.5, 3.5)).toFixed(2)),
    cure_minutes: Number((18 + cement * 0.85 + randRange(-4.2, 5.4)).toFixed(2)),
    compaction_index: Number((0.72 + cement * 0.012 + randRange(-0.11, 0.1)).toFixed(3)),
    blend_uniformity: Number((0.81 + randRange(-0.09, 0.12)).toFixed(3)),
    extrude_pressure: Number((360 + clay * 4 + requestedSpec.penetration_min * 0.7 + randRange(-42, 48)).toFixed(2)),
    moisture_window: Number((requestedSpec.moisture_max + randRange(-2.5, 2.5)).toFixed(2)),
    dimensional_tolerance_mm: requestedSpec.dimensional_tolerance_mm
  };
}

function processSignatureScore(referenceSignature, observedSignature) {
  const ref = referenceSignature && typeof referenceSignature === "object" ? referenceSignature : {};
  const obs = observedSignature && typeof observedSignature === "object" ? observedSignature : {};

  const numericKeys = ["mix_temp_c", "cure_minutes", "compaction_index", "blend_uniformity", "extrude_pressure", "moisture_window"];
  const available = numericKeys.filter((key) => Number.isFinite(Number(ref[key])));

  if (!available.length) {
    // No trusted baseline yet: pass by process plausibility.
    return 0.82;
  }

  const score = available.reduce((sum, key) => {
    const spread = key === "compaction_index" || key === "blend_uniformity" ? 0.25 : 0.2;
    return sum + numericSimilarity(Number(obs[key]), Number(ref[key]), spread);
  }, 0) / available.length;

  return clamp(score, 0, 1);
}

function blendProcessSignature(referenceSignature, observedSignature, blendFactor) {
  const ref = referenceSignature && typeof referenceSignature === "object" ? referenceSignature : {};
  const obs = observedSignature && typeof observedSignature === "object" ? observedSignature : {};
  const alpha = clamp(Number(blendFactor), 0.01, 0.95);

  const keys = new Set([...Object.keys(ref), ...Object.keys(obs)]);
  const blended = {};

  for (const key of keys) {
    const r = Number(ref[key]);
    const o = Number(obs[key]);
    if (Number.isFinite(r) && Number.isFinite(o)) {
      blended[key] = Number((r * (1 - alpha) + o * alpha).toFixed(4));
    } else if (obs[key] !== undefined) {
      blended[key] = obs[key];
    } else {
      blended[key] = ref[key];
    }
  }

  return blended;
}

function correctionDeltaFromSpec(machineTruth, requestedSpec) {
  const moistureDelta = Number((requestedSpec.moisture_max - Number(machineTruth.moisture_pct || 0)).toFixed(3));
  const densityDelta = Number((requestedSpec.density_min - Number(machineTruth.density || 0)).toFixed(3));
  const penetrationDelta = Number((requestedSpec.penetration_min - Number(machineTruth.penetration_resistance || 0)).toFixed(3));

  return {
    moisture_delta_pct: moistureDelta,
    density_delta: densityDelta,
    penetration_delta: penetrationDelta,
    tune_recipe: {
      increase_cement: densityDelta > 0 ? Number((Math.min(6, Math.abs(densityDelta) / 110)).toFixed(3)) : 0,
      reduce_water: moistureDelta < 0 ? Number((Math.min(6, Math.abs(moistureDelta) / 3)).toFixed(3)) : 0,
      increase_compaction: penetrationDelta > 0 ? Number((Math.min(0.3, penetrationDelta / 180)).toFixed(3)) : 0
    }
  };
}

async function updateHouseMaterialMix(client, houseId, materialKey) {
  const current = await client.query(
    `SELECT material_family_mix
     FROM houses
     WHERE id = $1`,
    [houseId]
  );

  const mix = current.rows[0]?.material_family_mix && typeof current.rows[0].material_family_mix === "object"
    ? { ...current.rows[0].material_family_mix }
    : {};

  mix[materialKey] = Number(mix[materialKey] || 0) + 1;

  await client.query(
    `UPDATE houses
     SET material_family_mix = $2::jsonb
     WHERE id = $1`,
    [houseId, JSON.stringify(mix)]
  );
}

async function verifyBlockTx(client, { houseId, soilSignature, componentType = "wall", queueId = null }) {
  await ensureSoilLibraryRow(client, soilSignature);

  const known = await client.query(
    `SELECT *
     FROM soil_library
     WHERE soil_signature = $1`,
    [soilSignature]
  );

  const soil = known.rows[0] || {};
  const requestedRole = roleForComponent(componentType);
  const requestedSpec = requestedSpecForComponent(componentType);
  const roleCrit = roleCriticality(requestedRole);

  const totalVerified = Number(soil.total_blocks_verified || 0);
  const shortConfidence = Number(soil.short_term_confidence || 0);
  const releasePrior = Number(soil.release_confidence || shortConfidence || 0);
  const longPrior = Number(soil.long_term_confidence || 0);
  const longevityPrior = Number(soil.longevity_confidence || longPrior || 0);
  const referenceSampleCount = Number(soil.reference_sample_count || 0);
  const priorMaturity = Number(soil.signature_maturity || signatureMaturityScore(referenceSampleCount, totalVerified));

  const recipe = soil.recipe || defaultRecipeForSoil(soilSignature);
  const processSignature = buildProcessSignature({ componentType, recipe, requestedSpec });
  const processMatchScore = processSignatureScore(soil.process_signature || {}, processSignature);

  const machineTruth = {
    penetration_resistance: Number((requestedSpec.penetration_min + randRange(-36, 90)).toFixed(3)),
    moisture_pct: Number((requestedSpec.moisture_max + randRange(-10, 8)).toFixed(3)),
    density: Number((requestedSpec.density_min + randRange(-190, 260)).toFixed(3)),
    dimensional_error_mm: Number((randRange(0, requestedSpec.dimensional_tolerance_mm * 1.7)).toFixed(3)),
    mass_proxy: Number((randRange(8, 22)).toFixed(3))
  };

  const characteristicScore = clamp((
    scoreWithinBand(machineTruth.penetration_resistance, requestedSpec.penetration_min, requestedSpec.penetration_min + 85) +
    scoreWithinBand(machineTruth.moisture_pct, Math.max(0, requestedSpec.moisture_max - 12), requestedSpec.moisture_max) +
    scoreWithinBand(machineTruth.density, requestedSpec.density_min, requestedSpec.density_max) +
    scoreWithinBand(machineTruth.dimensional_error_mm, 0, requestedSpec.dimensional_tolerance_mm)
  ) / 4, 0, 1);

  const driftScore = clamp(1 - ((processMatchScore * 0.62) + (characteristicScore * 0.38)), 0, 1);
  const contradictionDetected = isProcessSensorContradiction(processMatchScore, characteristicScore);

  let decision = "approve";
  let escalationReason = null;
  let escalationTier = null;
  let retries = 0;
  let passed = true;

  const maturityGatePassed = referenceSampleCount >= SIGNATURE_MATURITY_MIN_SAMPLES && priorMaturity >= 0.45;
  const fastPass = maturityGatePassed
    && releasePrior >= FAST_PASS_RELEASE_CONFIDENCE
    && processMatchScore >= FAST_PASS_PROCESS_MATCH
    && characteristicScore >= FAST_PASS_CHARACTERISTIC
    && driftScore < REWORK_DRIFT_THRESHOLD;

  if (contradictionDetected) {
    decision = "escalate";
    escalationReason = "tier3_process_sensor_contradiction";
    escalationTier = "tier3";
    retries = 3 + Math.floor(Math.random() * 2);
    passed = false;
  } else if (!fastPass) {
    if (driftScore >= DRIFT_ESCALATION_THRESHOLD || processMatchScore < 0.55 || characteristicScore < 0.5) {
      decision = "escalate";
      escalationReason = processMatchScore < 0.55
        ? "process_signature_drift"
        : (characteristicScore < 0.5 ? "requested_spec_mismatch" : "high_drift");
      escalationTier = "tier2";
      retries = 2 + Math.floor(Math.random() * 2);
      passed = false;
    } else if (driftScore >= REWORK_DRIFT_THRESHOLD || processMatchScore < 0.72 || characteristicScore < 0.72) {
      decision = "rework";
      escalationReason = "tune_and_retry";
      escalationTier = "tier1";
      retries = 1 + Math.floor(Math.random() * 2);
      passed = false;
    }
  }

  let correctionDelta = passed ? {} : correctionDeltaFromSpec(machineTruth, requestedSpec);
  if (contradictionDetected) {
    correctionDelta = {
      ...correctionDelta,
      contradiction_gap: Number(Math.abs(processMatchScore - characteristicScore).toFixed(4)),
      process_match_score: processMatchScore,
      characteristic_score: characteristicScore
    };
  }

  const blockId = `block-${houseId}-${queueId ?? "na"}-${Date.now()}-${Math.floor(Math.random() * 1000)}`;

  const shortTermConfidence = totalVerified > 0
    ? ((shortConfidence * totalVerified) + (passed ? 1 : 0)) / (totalVerified + 1)
    : (passed ? 0.5 : 0.12);

  const releaseConfidence = clamp(
    (releasePrior * 0.42)
    + (processMatchScore * 0.3)
    + (characteristicScore * 0.2)
    + (priorMaturity * 0.08)
    + (passed ? 0.04 : -0.12),
    0.01,
    0.999
  );

  const longTermConfidence = clamp(
    longPrior * 0.9 + (passed ? 0.02 : -0.03),
    0.01,
    0.999
  );

  const longevityConfidence = clamp(
    (longevityPrior * 0.5) + (longTermConfidence * 0.3) + (releaseConfidence * 0.2),
    0.01,
    0.999
  );

  const signatureContribution = passed && processMatchScore >= 0.72 && characteristicScore >= 0.72;
  const nextReferenceCount = referenceSampleCount + (signatureContribution ? 1 : 0);
  const nextMaturity = signatureMaturityScore(nextReferenceCount, totalVerified + 1);

  const nextSignature = signatureContribution
    ? (referenceSampleCount === 0
      ? processSignature
      : blendProcessSignature(soil.process_signature || {}, processSignature, PROCESS_SIGNATURE_BLEND))
    : (soil.process_signature || {});

  await client.query(
    `INSERT INTO block_verifications (
      house_id,
      block_id,
      soil_signature,
      requested_role,
      requested_spec,
      process_signature,
      machine_truth,
      penetration_resistance,
      moisture_pct,
      density,
      process_match_score,
      characteristic_score,
      drift_score,
      release_confidence,
      longevity_confidence,
      passed,
      retries,
      verification_mode,
      weathering_cycles,
      fast_pass,
      decision,
      escalation_reason,
      correction_delta,
      created_at
    ) VALUES (
      $1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
      'inline', 0, $18, $19, $20, $21::jsonb, NOW()
    )`,
    [
      houseId,
      blockId,
      soilSignature,
      requestedRole,
      JSON.stringify(requestedSpec),
      JSON.stringify(processSignature),
      JSON.stringify(machineTruth),
      machineTruth.penetration_resistance,
      machineTruth.moisture_pct,
      machineTruth.density,
      processMatchScore,
      characteristicScore,
      driftScore,
      releaseConfidence,
      longevityConfidence,
      passed,
      retries,
      fastPass,
      decision,
      escalationReason,
      JSON.stringify({ ...correctionDelta, escalation_tier: escalationTier, signature_maturity: nextMaturity, role_criticality: roleCrit })
    ]
  );

  await client.query(
    `UPDATE soil_library
     SET short_term_confidence = $2,
         long_term_confidence = $3,
         release_confidence = $4,
         longevity_confidence = $5,
         total_blocks_verified = $6,
         process_signature = $7::jsonb,
         reference_sample_count = $8,
         fast_pass_count = fast_pass_count + CASE WHEN $9 THEN 1 ELSE 0 END,
         drift_escalations = drift_escalations + CASE WHEN $10 THEN 1 ELSE 0 END,
         correction_events = correction_events + $11,
         last_requested_role = $12,
         last_drift_score = $13,
         recipe = COALESCE(recipe, $14::jsonb),
         signature_maturity = $15,
         updated_at = NOW()
     WHERE soil_signature = $1`,
    [
      soilSignature,
      shortTermConfidence,
      longTermConfidence,
      releaseConfidence,
      longevityConfidence,
      totalVerified + 1,
      JSON.stringify(nextSignature),
      nextReferenceCount,
      fastPass,
      decision === "escalate",
      retries,
      requestedRole,
      driftScore,
      JSON.stringify(defaultRecipeForSoil(soilSignature)),
      nextMaturity
    ]
  );

  await updateHouseMaterialMix(client, houseId, `${soilSignature}|${requestedRole}`);

  return {
    passed,
    skipped: fastPass,
    fastPass,
    decision,
    escalationReason,
    escalationTier,
    retries,
    penetration: machineTruth.penetration_resistance,
    moisture: machineTruth.moisture_pct,
    density: machineTruth.density,
    processMatchScore,
    characteristicScore,
    driftScore,
    releaseConfidence,
    longevityConfidence,
    requestedRole,
    requestedSpec,
    contradictionDetected,
    maturityScore: nextMaturity,
    roleCriticality: roleCrit,
    correctionDelta,
    blockId
  };
}


async function weatheringTestTx(client, soilSignature) {
  await ensureSoilLibraryRow(client, soilSignature);

  const current = await client.query(
    `SELECT long_term_confidence, longevity_confidence
     FROM soil_library
     WHERE soil_signature = $1`,
    [soilSignature]
  );

  const row = current.rows[0] || {};
  const priorLong = Number(row.long_term_confidence || 0);
  const priorLongevity = Number(row.longevity_confidence || priorLong || 0);

  const cycles = 12;
  const erosionScore = 0.7 + Math.random() * 0.3;
  const capillaryRise = Math.random() * 30;
  const longTermPassed = erosionScore > 0.8 && capillaryRise < 20;
  const observedLongTerm = longTermPassed ? 0.75 + Math.random() * 0.2 : 0.3 + Math.random() * 0.3;

  const longTermConfidence = clamp(priorLong * 0.35 + observedLongTerm * 0.65, 0.01, 0.999);
  const longevityConfidence = clamp(priorLongevity * 0.5 + longTermConfidence * 0.5, 0.01, 0.999);

  await client.query(
    `INSERT INTO block_verifications (
      house_id,
      block_id,
      soil_signature,
      requested_role,
      requested_spec,
      process_signature,
      machine_truth,
      penetration_resistance,
      moisture_pct,
      density,
      process_match_score,
      characteristic_score,
      drift_score,
      release_confidence,
      longevity_confidence,
      passed,
      retries,
      verification_mode,
      weathering_cycles,
      fast_pass,
      decision,
      escalation_reason,
      correction_delta,
      created_at
    ) VALUES (
      NULL, $1, $2, NULL, '{}'::jsonb, '{}'::jsonb, $3::jsonb, NULL, NULL, NULL, NULL, NULL, NULL, NULL, $4,
      $5, 0, 'accelerated_weathering', $6, FALSE, $7, NULL, '{}'::jsonb, NOW()
    )`,
    [
      `weather-${soilSignature}-${Date.now()}`,
      soilSignature,
      JSON.stringify({ erosion_score: erosionScore, capillary_rise_mm: capillaryRise }),
      longevityConfidence,
      longTermPassed,
      cycles,
      longTermPassed ? "approve" : "reject"
    ]
  );

  await client.query(
    `UPDATE soil_library
     SET long_term_confidence = $2,
         longevity_confidence = $3,
         weathering_cycles_tested = GREATEST(weathering_cycles_tested, $4),
         erosion_score = $5,
         updated_at = NOW()
     WHERE soil_signature = $1`,
    [soilSignature, longTermConfidence, longevityConfidence, cycles, erosionScore]
  );

  return { longTermConfidence, longevityConfidence, erosionScore, capillaryRise, cycles, passed: longTermPassed };
}

function clamp(
value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function classifySurveySignal({ penetration, organic, salinity, footBalance, sleeveSeat, braceFailure, partialPenetration }) {
  if (organic > 11 || salinity > 3.2 || penetration < 58 || (braceFailure && sleeveSeat < 0.35)) {
    return { status: "REJECT", reason: "weak_shallow_support" };
  }

  if (braceFailure || (partialPenetration && penetration < 96)) {
    return { status: "UNKNOWN", reason: "unknown_needs_adjacent_probes" };
  }

  if (penetration < 105 || partialPenetration || footBalance < 0.68 || sleeveSeat < 0.66) {
    return { status: "MARGINAL", reason: "needs_neighbor_confirmation" };
  }

  return { status: "BUILDABLE", reason: "stable_support_response" };
}

function deriveSurveyConfidence({ status, footBalance, sleeveSeat, penetration, organic, salinity, braceFailure, partialPenetration }) {
  const base = status === "BUILDABLE" ? 0.83 : (status === "MARGINAL" ? 0.6 : (status === "UNKNOWN" ? 0.48 : 0.34));
  const contact = ((footBalance + sleeveSeat) / 2) * 0.2;
  const forceTerm = clamp((penetration - 60) / 160, 0, 1) * 0.12;
  const chemistryPenalty = clamp((organic - 8) / 10, 0, 1) * 0.08 + clamp((salinity - 2) / 3, 0, 1) * 0.08;
  const failurePenalty = (braceFailure ? 0.16 : 0) + (partialPenetration ? 0.1 : 0);
  return clamp(base + contact + forceTerm - chemistryPenalty - failurePenalty + randRange(-0.03, 0.03), 0.05, 0.99);
}

function buildSurveyProbe(point, roundIndex = 0, densified = false) {
  const soilSignature = generateSoilSignature();
  const parsed = parseSoilSignature(soilSignature);

  const surfaceTilt = clamp(randRange(0.2, 8.5) + (densified ? randRange(0, 2.8) : 0), 0.1, 14.8);
  const footBalance = clamp(0.92 - surfaceTilt * 0.035 + randRange(-0.15, 0.09), 0.08, 0.99);
  const sleeveSeat = clamp(0.9 - parsed.organic * 0.03 - parsed.salinity * 0.05 + randRange(-0.1, 0.1), 0.05, 0.99);
  const braceFailure = sleeveSeat < 0.34 || (footBalance < 0.4 && Math.random() < 0.92);
  const partialPenetration = !braceFailure && (footBalance < 0.62 || sleeveSeat < 0.63 || Math.random() < 0.12);

  const maxProbeDepth = braceFailure
    ? randRange(0.55, 1.5)
    : (partialPenetration ? randRange(1.1, 2.2) : randRange(2.0, 3.3));

  const moisture = clamp(6 + Math.random() * 35 + parsed.organic * 0.2, 4, 52);

  let basePen = 65 + footBalance * 75 + sleeveSeat * 80 - parsed.organic * 2.2 - parsed.salinity * 7 + randRange(-18, 18);
  if (braceFailure) basePen -= 32;
  if (partialPenetration) basePen -= 14;
  const penetrationCurve = [
    clamp(basePen - 28 + randRange(-7, 7), 20, 260),
    clamp(basePen - 8 + randRange(-7, 7), 20, 280),
    clamp(basePen + 11 + randRange(-8, 8), 20, 320),
    clamp(basePen + 28 + randRange(-9, 9), 20, 360)
  ];

  const penetration = penetrationCurve[penetrationCurve.length - 1];
  const classification = classifySurveySignal({
    penetration,
    organic: parsed.organic,
    salinity: parsed.salinity,
    footBalance,
    sleeveSeat,
    braceFailure,
    partialPenetration
  });

  const normalizedStatus = classification.status === "UNKNOWN" ? "MARGINAL" : classification.status;
  const confidence = deriveSurveyConfidence({
    status: classification.status,
    footBalance,
    sleeveSeat,
    penetration,
    organic: parsed.organic,
    salinity: parsed.salinity,
    braceFailure,
    partialPenetration
  });

  return {
    probe_x: point.x,
    probe_y: point.y,
    depth_meters: roundIndex === 0 ? randRange(2.0, 3.1) : randRange(1.6, 2.8),
    soil_signature: soilSignature,
    status: normalizedStatus,
    penetration_resistance: penetration,
    moisture_pct: moisture,
    organic_pct: parsed.organic,
    salinity: parsed.salinity,
    surface_tilt_deg: surfaceTilt,
    foot_balance_score: footBalance,
    sleeve_seat_score: sleeveSeat,
    outer_brace_status: braceFailure ? "failed" : (partialPenetration ? "partial" : "stable"),
    max_probe_depth_m: maxProbeDepth,
    penetration_curve: penetrationCurve,
    brace_failure: braceFailure,
    partial_penetration: partialPenetration,
    confidence,
    classification_reason: classification.reason,
    densified,
    round_index: roundIndex
  };
}

function inferSurveyNeighborhood(probes) {
  return probes.map((probe) => {
    const neighbors = probes.filter((other) => {
      if (other === probe) return false;
      const dx = Math.abs(Number(other.probe_x) - Number(probe.probe_x));
      const dy = Math.abs(Number(other.probe_y) - Number(probe.probe_y));
      return dx <= SURVEY_GRID_STEP && dy <= SURVEY_GRID_STEP;
    });

    const buildableNear = neighbors.filter((n) => n.status === "BUILDABLE").length;
    const rejectNear = neighbors.filter((n) => n.status === "REJECT").length;
    const marginalNear = neighbors.filter((n) => n.status === "MARGINAL").length;

    const updated = { ...probe };

    if (String(updated.classification_reason || "").includes("unknown") && buildableNear >= 2 && rejectNear === 0) {
      updated.status = "MARGINAL";
      updated.classification_reason = "unknown_resolved_by_neighbors";
      updated.confidence = clamp(Number(updated.confidence) + 0.05, 0.05, 0.99);
    }

    if (updated.status === "MARGINAL" && buildableNear >= 4 && rejectNear === 0 && Number(updated.confidence) < SURVEY_CONFIDENCE_BUILDABLE) {
      updated.classification_reason = "neighbor_supported";
      updated.confidence = clamp(Number(updated.confidence) + 0.1, 0.05, 0.99);
    }

    if (updated.status === "BUILDABLE" && rejectNear >= 2 && Number(updated.confidence) < SURVEY_CONFIDENCE_BUILDABLE) {
      updated.status = "MARGINAL";
      updated.classification_reason = "contradictory_neighbors";
      updated.confidence = clamp(Number(updated.confidence) - 0.14, 0.05, 0.99);
    }

    if (updated.status === "REJECT" && buildableNear >= 4) {
      updated.status = "MARGINAL";
      updated.classification_reason = "local_anomaly";
      updated.confidence = clamp(Number(updated.confidence) + 0.1, 0.05, 0.99);
    }

    if ((updated.status === "MARGINAL" || updated.status === "REJECT") && rejectNear >= 3 && buildableNear <= 1) {
      updated.status = "REJECT";
      updated.classification_reason = "weak_zone_cluster";
      updated.confidence = clamp(Number(updated.confidence) + 0.06 + marginalNear * 0.01, 0.05, 0.99);
    }

    return updated;
  });
}

function computeSurveyUncertainty(probes) {
  if (!probes.length) {
    return { uncertainty: 1, buildableRatio: 0, rejectRatio: 0, marginalRatio: 0 };
  }

  const total = probes.length;
  const buildable = probes.filter((p) => p.status === "BUILDABLE").length;
  const reject = probes.filter((p) => p.status === "REJECT").length;
  const marginal = probes.filter((p) => p.status === "MARGINAL").length;
  const lowConfidence = probes.filter((p) => Number(p.confidence) < 0.75).length;

  const uncertainty = clamp((marginal / total) * 0.65 + (lowConfidence / total) * 0.35, 0, 1);

  return {
    uncertainty,
    buildableRatio: buildable / total,
    rejectRatio: reject / total,
    marginalRatio: marginal / total
  };
}

function recommendBuildZone(probes) {
  const buildable = probes.filter((p) => p.status === "BUILDABLE");
  if (!buildable.length) return "none";

  const zones = {
    zone_nw: { score: 0, count: 0 },
    zone_ne: { score: 0, count: 0 },
    zone_sw: { score: 0, count: 0 },
    zone_se: { score: 0, count: 0 }
  };

  for (const p of buildable) {
    const x = Number(p.probe_x);
    const y = Number(p.probe_y);
    const zone = x < 10 ? (y < 10 ? "zone_nw" : "zone_sw") : (y < 10 ? "zone_ne" : "zone_se");
    zones[zone].score += Number(p.confidence || 0);
    zones[zone].count += 1;
  }

  let best = "zone_nw";
  let bestValue = -1;
  for (const [zone, val] of Object.entries(zones)) {
    const avg = val.count > 0 ? val.score / val.count : 0;
    if (avg > bestValue) {
      bestValue = avg;
      best = zone;
    }
  }

  return best;
}

function nextDensificationPoints(probes, existingSet) {
  const uncertain = probes
    .filter((p) => p.status === "MARGINAL" || Number(p.confidence) < 0.75 || String(p.classification_reason || "").includes("neighbor") || String(p.classification_reason || "").includes("unknown"))
    .sort((a, b) => Number(a.confidence) - Number(b.confidence))
    .slice(0, 6);

  const points = [];
  for (const probe of uncertain) {
    const offsets = [
      [2, 0],
      [-2, 0],
      [0, 2],
      [0, -2]
    ];

    for (const [ox, oy] of offsets) {
      const x = clamp(Number(probe.probe_x) + ox, 0, (SURVEY_GRID_WIDTH - 1) * SURVEY_GRID_STEP);
      const y = clamp(Number(probe.probe_y) + oy, 0, (SURVEY_GRID_WIDTH - 1) * SURVEY_GRID_STEP);
      const key = `${x}:${y}`;
      if (!existingSet.has(key)) {
        existingSet.add(key);
        points.push({ x, y });
      }
    }
  }

  return points.slice(0, 10);
}

async function surveyHouseTx(client, houseId) {
  await client.query(`UPDATE houses SET survey_status = 'surveying' WHERE id = $1`, [houseId]);
  await client.query(`DELETE FROM site_surveys WHERE house_id = $1`, [houseId]);

  await client.query(
    `INSERT INTO survey_runs (house_id, run_status, uncertainty_remaining, probe_budget, probes_used, densification_rounds, boundary_shift, stopped_reason, recommended_build_zone, updated_at)
     VALUES ($1, 'running', 1, $2, 0, 0, 1, NULL, NULL, NOW())
     ON CONFLICT (house_id) DO UPDATE
       SET run_status = 'running',
           uncertainty_remaining = 1,
           probe_budget = EXCLUDED.probe_budget,
           probes_used = 0,
           densification_rounds = 0,
           boundary_shift = 1,
           stopped_reason = NULL,
           recommended_build_zone = NULL,
           updated_at = NOW()`,
    [houseId, SURVEY_PROBE_BUDGET]
  );

  const coarsePoints = [];
  for (let i = 0; i < SURVEY_PROBE_COUNT; i += 1) {
    coarsePoints.push({
      x: (i % SURVEY_GRID_WIDTH) * SURVEY_GRID_STEP,
      y: Math.floor(i / SURVEY_GRID_WIDTH) * SURVEY_GRID_STEP
    });
  }

  const seen = new Set(coarsePoints.map((p) => `${p.x}:${p.y}`));
  let probes = coarsePoints.map((point) => buildSurveyProbe(point, 0, false));
  probes = inferSurveyNeighborhood(probes);

  let { uncertainty, buildableRatio, rejectRatio } = computeSurveyUncertainty(probes);
  let previousUncertainty = 1;
  let previousRejectRatio = rejectRatio;
  let previousBuildRatio = buildableRatio;
  let boundaryShift = 1;
  let stableRounds = 0;
  let densificationRounds = 0;
  let stopReason = 'round_limit';

  for (let round = 1; round <= SURVEY_MAX_ROUNDS; round += 1) {
    if (probes.length >= SURVEY_PROBE_BUDGET) {
      stopReason = 'probe_budget_exhausted';
      break;
    }

    if (stableRounds >= 3 && uncertainty <= SURVEY_CONFIDENCE_CONVERGED) {
      stopReason = 'confidence_converged';
      break;
    }

    const candidates = nextDensificationPoints(probes, seen);
    if (!candidates.length) {
      stopReason = 'no_uncertain_regions';
      break;
    }

    const remaining = Math.max(0, SURVEY_PROBE_BUDGET - probes.length);
    const additions = candidates.slice(0, remaining).map((point) => buildSurveyProbe(point, round, true));
    if (!additions.length) {
      stopReason = 'probe_budget_exhausted';
      break;
    }

    probes = inferSurveyNeighborhood(probes.concat(additions));
    densificationRounds += 1;

    previousUncertainty = uncertainty;
    previousRejectRatio = rejectRatio;
    previousBuildRatio = buildableRatio;

    const next = computeSurveyUncertainty(probes);
    uncertainty = next.uncertainty;
    rejectRatio = next.rejectRatio;
    buildableRatio = next.buildableRatio;

    boundaryShift = Math.abs(rejectRatio - previousRejectRatio) + Math.abs(buildableRatio - previousBuildRatio);

    if (Math.abs(previousUncertainty - uncertainty) <= SURVEY_CONVERGENCE_DELTA && boundaryShift <= SURVEY_BOUNDARY_STABILITY) {
      stableRounds += 1;
    } else {
      stableRounds = 0;
    }
  }

  probes = inferSurveyNeighborhood(probes);
  const finalStats = computeSurveyUncertainty(probes);
  uncertainty = finalStats.uncertainty;
  rejectRatio = finalStats.rejectRatio;

  const rejectCount = probes.filter((probe) => probe.status === 'REJECT').length;
  const buildable = probes.filter((probe) => probe.status === 'BUILDABLE');

  const dominantSignature = (() => {
    const source = buildable.length ? buildable : probes;
    const counts = new Map();
    for (const p of source) counts.set(p.soil_signature, (counts.get(p.soil_signature) || 0) + 1);
    return [...counts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || generateSoilSignature();
  })();

  const approved = rejectRatio <= SURVEY_REJECT_RATIO && uncertainty <= SURVEY_CONFIDENCE_CONVERGED;
  const recommendedBuildZone = recommendBuildZone(probes);
  const referencePatch = approved ? referencePatchForZone(recommendedBuildZone) : null;

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
      surface_tilt_deg,
      foot_balance_score,
      sleeve_seat_score,
      outer_brace_status,
      max_probe_depth_m,
      penetration_curve,
      brace_failure,
      partial_penetration,
      confidence,
      classification_reason,
      densified,
      round_index,
      notes,
      surveyed_at
    )
    SELECT
      $1,
      p.probe_x,
      p.probe_y,
      p.depth_meters,
      p.soil_signature,
      p.status,
      p.penetration_resistance,
      p.moisture_pct,
      p.organic_pct,
      p.salinity,
      p.surface_tilt_deg,
      p.foot_balance_score,
      p.sleeve_seat_score,
      p.outer_brace_status,
      p.max_probe_depth_m,
      p.penetration_curve,
      p.brace_failure,
      p.partial_penetration,
      p.confidence,
      p.classification_reason,
      p.densified,
      p.round_index,
      CASE WHEN p.densified THEN 'scheduler_densified' ELSE 'scheduler_coarse' END,
      NOW()
    FROM jsonb_to_recordset($2::jsonb) AS p(
      probe_x int,
      probe_y int,
      depth_meters numeric,
      soil_signature text,
      status text,
      penetration_resistance numeric,
      moisture_pct numeric,
      organic_pct numeric,
      salinity numeric,
      surface_tilt_deg numeric,
      foot_balance_score numeric,
      sleeve_seat_score numeric,
      outer_brace_status text,
      max_probe_depth_m numeric,
      penetration_curve jsonb,
      brace_failure boolean,
      partial_penetration boolean,
      confidence numeric,
      classification_reason text,
      densified boolean,
      round_index int
    )`,
    [houseId, JSON.stringify(probes)]
  );

  await ensureSoilLibraryRow(client, dominantSignature);

  await client.query(
    `UPDATE houses
     SET survey_status = $2,
         soil_signature = $3,
         site_zone = $4,
         recommended_build_zone = $5,
         survey_uncertainty_remaining = $6,
         survey_stop_reason = $7,
         survey_probe_count = $8,
         reference_patch_x = $9,
         reference_patch_y = $10,
         reference_patch_protected = $11,
         reference_patch_stamped_at = $12
     WHERE id = $1`,
    [
      houseId,
      approved ? 'approved' : 'rejected',
      dominantSignature,
      approved ? 'BUILDABLE' : 'REJECT',
      recommendedBuildZone,
      uncertainty,
      stopReason,
      probes.length,
      referencePatch?.x ?? null,
      referencePatch?.y ?? null,
      Boolean(referencePatch),
      referencePatch ? new Date().toISOString() : null
    ]
  );

  const runStatus = stopReason === 'probe_budget_exhausted'
    ? 'budget_exhausted'
    : (uncertainty > 0.5 ? 'unstable' : 'complete');

  await client.query(
    `UPDATE survey_runs
     SET run_status = $2,
         uncertainty_remaining = $3,
         probes_used = $4,
         densification_rounds = $5,
         boundary_shift = $6,
         stopped_reason = $7,
         recommended_build_zone = $8,
         updated_at = NOW()
     WHERE house_id = $1`,
    [houseId, runStatus, uncertainty, probes.length, densificationRounds, boundaryShift, stopReason, recommendedBuildZone]
  );

  return {
    status: approved ? 'approved' : 'rejected',
    rejectCount,
    probes,
    uncertainty,
    densificationRounds,
    stopReason,
    recommendedBuildZone,
    referencePatch
  };
}

async function completeSealingTx(client, { houseId, soilSignature }) {
  await ensureSoilLibraryRow(client, soilSignature);

  const soil = await client.query(
    `SELECT short_term_confidence, long_term_confidence, release_confidence, longevity_confidence, recipe
     FROM soil_library
     WHERE soil_signature = $1`,
    [soilSignature]
  );

  const houseMixRes = await client.query(
    `SELECT material_family_mix
     FROM houses
     WHERE id = $1`,
    [houseId]
  );

  const row = soil.rows[0] || {};
  const houseMix = houseMixRes.rows[0]?.material_family_mix && typeof houseMixRes.rows[0].material_family_mix === "object"
    ? houseMixRes.rows[0].material_family_mix
    : {};

  const mixEntries = [];
  for (const [key, rawWeight] of Object.entries(houseMix)) {
    const weight = Number(rawWeight || 0);
    if (weight <= 0) continue;

    const [signature, rawRole] = String(key).split("|");
    const role = rawRole || "structural_wall";
    if (!signature) continue;

    mixEntries.push({
      signature,
      role,
      weight,
      criticality: roleCriticality(role),
      exposure: roleExposure(role)
    });
  }

  const uniqueSignatures = [...new Set(mixEntries.map((entry) => entry.signature))];
  const confBySig = new Map();

  if (uniqueSignatures.length > 0) {
    const confRows = await client.query(
      `SELECT soil_signature, long_term_confidence, longevity_confidence, release_confidence
       FROM soil_library
       WHERE soil_signature = ANY($1::text[])`,
      [uniqueSignatures]
    );

    for (const conf of confRows.rows) {
      confBySig.set(conf.soil_signature, {
        long: Number(conf.long_term_confidence || 0),
        longevity: Number(conf.longevity_confidence || 0),
        release: Number(conf.release_confidence || 0)
      });
    }
  }

  const defaultLong = Math.max(Number(row.long_term_confidence || 0), Number(row.longevity_confidence || 0));
  let weightedRoleSum = 0;
  let weightedRoleDenom = 0;
  let weakestCriticalConfidence = 1;
  let weakestCriticalRole = null;

  for (const entry of mixEntries) {
    const conf = confBySig.get(entry.signature);
    const baseConfidence = clamp(Math.max(Number(conf?.long || 0), Number(conf?.longevity || 0), defaultLong), 0, 1);
    const adjustedConfidence = clamp(baseConfidence - (entry.exposure * 0.04), 0, 1);

    const weightedRole = entry.weight * entry.criticality;
    weightedRoleSum += adjustedConfidence * weightedRole;
    weightedRoleDenom += weightedRole;

    if (entry.criticality >= 0.8 && adjustedConfidence < weakestCriticalConfidence) {
      weakestCriticalConfidence = adjustedConfidence;
      weakestCriticalRole = entry.role;
    }
  }

  const weightedRoleConfidence = weightedRoleDenom > 0
    ? (weightedRoleSum / weightedRoleDenom)
    : defaultLong;

  if (!Number.isFinite(weakestCriticalConfidence) || weakestCriticalConfidence >= 1) {
    weakestCriticalConfidence = weightedRoleConfidence;
  }

  const roleWeightedConfidence = clamp((weightedRoleConfidence * 0.58) + (weakestCriticalConfidence * 0.42), 0, 1);
  const longTermConfidence = clamp(Math.max(defaultLong * 0.4 + roleWeightedConfidence * 0.6, roleWeightedConfidence), 0.01, 0.999);
  const releaseConfidence = clamp(Number(row.release_confidence || row.short_term_confidence || 0), 0.01, 0.999);

  const limitingConfidence = Math.min(roleWeightedConfidence, weakestCriticalConfidence + 0.06);

  const baseTTL = 365 * 5;
  const soilMultiplier = limitingConfidence > 0.9
    ? 1.75
    : (limitingConfidence > 0.82
      ? 1.45
      : (limitingConfidence > 0.72
        ? 1.2
        : 1.0));
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
      role_weighted_confidence,
      weakest_critical_confidence,
      critical_constraint_role,
      updated_at
    ) VALUES ($1, 1, $2, $3, $4, $5, $6::jsonb, 'ok', 2.0, $7, $8, $9, NOW())
    ON CONFLICT (house_id) DO UPDATE
    SET coating_applied_at = EXCLUDED.coating_applied_at,
        ttl_expires_at = EXCLUDED.ttl_expires_at,
        alert_at = EXCLUDED.alert_at,
        ttl_days = EXCLUDED.ttl_days,
        improved_recipe = EXCLUDED.improved_recipe,
        status = 'ok',
        role_weighted_confidence = EXCLUDED.role_weighted_confidence,
        weakest_critical_confidence = EXCLUDED.weakest_critical_confidence,
        critical_constraint_role = EXCLUDED.critical_constraint_role,
        updated_at = NOW()`,
    [
      houseId,
      coatingApplied.toISOString(),
      ttlExpires.toISOString(),
      alertAt.toISOString(),
      ttlDays,
      JSON.stringify({
        ...(row.recipe || defaultRecipeForSoil(soilSignature)),
        ttl_role_weighted_confidence: Number(roleWeightedConfidence.toFixed(4)),
        weakest_critical_confidence: Number(weakestCriticalConfidence.toFixed(4)),
        critical_constraint_role: weakestCriticalRole || "none",
        limiting_confidence: Number(limitingConfidence.toFixed(4))
      }),
      roleWeightedConfidence,
      weakestCriticalConfidence,
      weakestCriticalRole
    ]
  );

  return {
    ttlDays,
    ttlExpires: ttlExpires.toISOString(),
    alertAt: alertAt.toISOString(),
    weightedLongConfidence: longTermConfidence,
    releaseConfidence,
    roleWeightedConfidence,
    weakestCriticalConfidence,
    criticalConstraintRole: weakestCriticalRole || "none"
  };
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
function insertionProfileKey(componentType, cell) {
  return `${componentType}:${cell?.z ?? 0}`;
}

function simulateInsertion(componentType, cell) {
  const base = COMPONENT_BASE_IMPEDANCE[componentType] ?? 0.5;
  const profileKey = insertionProfileKey(componentType, cell);

  if (!insertionProfileMemory.has(profileKey)) {
    insertionProfileMemory.set(profileKey, {
      attempts: 0,
      successfulSeats: 0
    });
  }

  const profile = insertionProfileMemory.get(profileKey);
  const calibrationPass = profile.successfulSeats === 0;
  const preScanTolerance = calibrationPass ? ASSEMBLY_PRE_SCAN_TOLERANCE_CAL : ASSEMBLY_PRE_SCAN_TOLERANCE;
  const preScanAlignment = Math.random();
  const preScanEscalated = preScanAlignment > preScanTolerance;

  profile.attempts += 1;

  if (preScanEscalated) {
    return {
      success: false,
      retries: 0,
      resistance: base,
      decision: "pre_insertion_drift_escalation",
      calibrationPass,
      preScanEscalated,
      preScanAlignment: Number(preScanAlignment.toFixed(4)),
      postSeatPassed: false,
      postSeatResistance: 0,
      preScanSeconds: ASSEMBLY_PRE_SCAN_SECONDS,
      postSeatSeconds: 0,
      calibrationSeconds: calibrationPass ? ASSEMBLY_CALIBRATION_SLOWDOWN_SECONDS : 0
    };
  }

  let retries = 0;
  let resistance = base;
  const threshold = calibrationPass
    ? Math.max(0.2, IMPEDANCE_THRESHOLD - ASSEMBLY_CALIBRATION_TIGHTEN)
    : IMPEDANCE_THRESHOLD;

  while (retries < MAX_INSERTION_RETRIES) {
    resistance = base + Math.random() * IMPEDANCE_VARIANCE;
    if (resistance <= threshold) {
      break;
    }
    retries += 1;
  }

  if (retries >= MAX_INSERTION_RETRIES && resistance > threshold) {
    return {
      success: false,
      retries,
      resistance,
      decision: "impedance_retry_exhausted",
      calibrationPass,
      preScanEscalated: false,
      preScanAlignment: Number(preScanAlignment.toFixed(4)),
      postSeatPassed: false,
      postSeatResistance: 0,
      preScanSeconds: ASSEMBLY_PRE_SCAN_SECONDS,
      postSeatSeconds: 0,
      calibrationSeconds: calibrationPass ? ASSEMBLY_CALIBRATION_SLOWDOWN_SECONDS : 0
    };
  }

  const postSeatResistance = Number((0.4 + Math.random() * 0.6).toFixed(4));
  const postSeatThreshold = calibrationPass ? ASSEMBLY_POST_SEAT_MIN_CAL : ASSEMBLY_POST_SEAT_MIN;
  const postSeatPassed = postSeatResistance >= postSeatThreshold;

  if (postSeatPassed) {
    profile.successfulSeats += 1;
  }

  return {
    success: postSeatPassed,
    retries,
    resistance,
    decision: postSeatPassed ? "seated" : "post_seat_micro_load_failed",
    calibrationPass,
    preScanEscalated: false,
    preScanAlignment: Number(preScanAlignment.toFixed(4)),
    postSeatPassed,
    postSeatResistance,
    preScanSeconds: ASSEMBLY_PRE_SCAN_SECONDS,
    postSeatSeconds: ASSEMBLY_POST_SEAT_SECONDS,
    calibrationSeconds: calibrationPass ? ASSEMBLY_CALIBRATION_SLOWDOWN_SECONDS : 0
  };
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

async function ensureLogisticsLanesForHouse(client, houseId) {
  await client.query(
    `INSERT INTO logistics_lane_segments (
      house_id,
      lane_id,
      segment_index,
      start_x,
      start_y,
      end_x,
      end_y,
      condition_score,
      grade_match_score,
      avg_drag_force,
      body_gauge_match,
      relay_consensus,
      reference_relay_age_s,
      passes,
      verification_events,
      status,
      is_reference_zone,
      restamp_required,
      updated_at
    )
    WITH house_ref AS (
      SELECT
        id,
        COALESCE(reference_patch_x, 1)::numeric AS ref_x,
        COALESCE(reference_patch_y, 8)::numeric AS ref_y
      FROM houses
      WHERE id = $1
    ), segments AS (
      SELECT generate_series(0, $2::int - 1) AS segment_index
    )
    SELECT
      hr.id,
      'lane_main',
      s.segment_index,
      ROUND((hr.ref_x + (s.segment_index::numeric / $2::numeric) * (4.5 - hr.ref_x))::numeric, 3),
      ROUND((hr.ref_y + (s.segment_index::numeric / $2::numeric) * (4.5 - hr.ref_y))::numeric, 3),
      ROUND((hr.ref_x + ((s.segment_index + 1)::numeric / $2::numeric) * (4.5 - hr.ref_x))::numeric, 3),
      ROUND((hr.ref_y + ((s.segment_index + 1)::numeric / $2::numeric) * (4.5 - hr.ref_y))::numeric, 3),
      0.22,
      0.28,
      50,
      0.3,
      0.42,
      160,
      0,
      0,
      'raw',
      s.segment_index = 0,
      FALSE,
      NOW()
    FROM house_ref hr
    CROSS JOIN segments s
    ON CONFLICT (house_id, lane_id, segment_index) DO NOTHING`,
    [houseId, LOGISTICS_SEGMENT_COUNT]
  );
}

async function applyLogisticsPass(client, { houseId, robotId, robotStatus }) {
  await ensureLogisticsLanesForHouse(client, houseId);

  const picked = await client.query(
    `SELECT *
     FROM logistics_lane_segments
     WHERE house_id = $1
     ORDER BY CASE status WHEN 'degraded' THEN 0 WHEN 'raw' THEN 1 WHEN 'conditioning' THEN 2 ELSE 3 END,
              condition_score ASC,
              segment_index ASC,
              updated_at ASC
     LIMIT 1
     FOR UPDATE SKIP LOCKED`,
    [houseId]
  );

  const seg = picked.rows[0];
  if (!seg) {
    return { travelFactor: 1, laneAvgCondition: 0.5, staleSegments: 0 };
  }

  const passes = Number(seg.passes || 0);
  const prevCondition = Number(seg.condition_score || 0.22);
  const prevRelayAge = Number(seg.reference_relay_age_s || 999);
  const needsVerify = prevRelayAge > LOGISTICS_STALE_RELAY_SECONDS || passes === 0 || (passes % LOGISTICS_VERIFY_INTERVAL === 0);

  let relayAge = Math.max(0, Math.round(prevRelayAge + randRange(-18, 30)));
  if (needsVerify) {
    relayAge = Math.max(4, Math.round(randRange(8, 34)));
  }

  const relayStale = relayAge > LOGISTICS_STALE_RELAY_SECONDS;

  let condition = prevCondition + LOGISTICS_CONDITION_GAIN + randRange(-0.02, 0.04);
  if (robotStatus === 'waiting_component') {
    condition -= 0.03;
  }
  if (relayStale) {
    condition -= LOGISTICS_DEGRADE_RATE;
  }

  condition = clamp(condition, 0.05, 0.99);

  const dragForce = clamp(58 - condition * 24 + randRange(-4.5, 4.5), 18, 90);
  const bodyGaugeMatch = clamp(condition + randRange(-0.15, 0.08), 0.05, 0.99);
  const relayConsensus = clamp(1 - relayAge / (LOGISTICS_STALE_RELAY_SECONDS * 1.8) + randRange(-0.08, 0.06), 0.05, 0.99);
  const gradeMatch = clamp(condition * 0.55 + bodyGaugeMatch * 0.25 + relayConsensus * 0.2 + randRange(-0.06, 0.05), 0.05, 0.99);

  let nextStatus = 'conditioning';
  if (condition < 0.45) nextStatus = 'raw';
  if (relayStale || gradeMatch < 0.46) nextStatus = 'degraded';
  if (condition >= 0.78 && !relayStale && gradeMatch >= 0.7) nextStatus = 'stable';

  const restampRequired = Boolean(seg.is_reference_zone) && relayStale;

  await client.query(
    `UPDATE logistics_lane_segments
     SET condition_score = $2,
         grade_match_score = $3,
         avg_drag_force = $4,
         body_gauge_match = $5,
         relay_consensus = $6,
         reference_relay_age_s = $7,
         passes = passes + 1,
         verification_events = verification_events + $8,
         status = $9,
         restamp_required = $10,
         last_verified_at = CASE WHEN $8 = 1 THEN NOW() ELSE last_verified_at END,
         updated_at = NOW()
     WHERE id = $1`,
    [seg.id, condition, gradeMatch, dragForce, bodyGaugeMatch, relayConsensus, relayAge, needsVerify ? 1 : 0, nextStatus, restampRequired]
  );

  const summary = await client.query(
    `SELECT
       COALESCE(AVG(condition_score), 0)::float AS avg_condition,
       COUNT(*) FILTER (WHERE reference_relay_age_s > $2)::int AS stale_segments,
       COUNT(*) FILTER (WHERE status = 'degraded')::int AS degraded_segments
     FROM logistics_lane_segments
     WHERE house_id = $1`,
    [houseId, LOGISTICS_STALE_RELAY_SECONDS]
  );

  const avgCondition = Number(summary.rows[0]?.avg_condition || 0.22);
  const travelFactor = clamp(LOGISTICS_TRAVEL_FACTOR_MAX - avgCondition * 0.7, LOGISTICS_TRAVEL_FACTOR_MIN, LOGISTICS_TRAVEL_FACTOR_MAX);

  return {
    travelFactor,
    laneAvgCondition: avgCondition,
    staleSegments: Number(summary.rows[0]?.stale_segments || 0),
    degradedSegments: Number(summary.rows[0]?.degraded_segments || 0),
    verifierTriggered: needsVerify,
    segmentId: seg.id,
    robotId
  };
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

  if (!stage || stage === 'complete') {
    await client.query(
      `UPDATE robots
       SET status = 'idle', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  if (stage === 'site_prep') {
    await runSitePrepStep(client, robot);
    return;
  }

  if (stage === 'surveying') {
    await runSurveyingStep(client, robot);
    return;
  }

  if (stage === 'sealing') {
    await runSealingRobotStep(client, robot);
    return;
  }

  if (stage === 'community_assembly') {
    await client.query(
      `UPDATE robots
       SET status = 'idle', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  let laneSnapshot = null;
  if (['foundation', 'framing', 'mep', 'finishing'].includes(stage)) {
    laneSnapshot = await applyLogisticsPass(client, {
      houseId: robot.active_house_id,
      robotId: robot.id,
      robotStatus: robot.status
    });
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

  if (readyRes.rows[0]?.status !== 'ready') {
    await client.query(
      `UPDATE robots
       SET status = 'waiting_component', total_idle_seconds = total_idle_seconds + 1, updated_at = NOW()
       WHERE id = $1`,
      [robot.id]
    );
    return;
  }

  const soilSignature = await ensureHouseSoilSignature(client, robot.active_house_id);
  const qc = await verifyBlockTx(client, { houseId: robot.active_house_id, soilSignature, componentType: job.component_type, queueId: job.queue_id });

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
  const insertion = simulateInsertion(job.component_type, cell);
  const retrySeconds = insertion.retries * RETRY_ADJUST_SECONDS;
  const failurePenalty = insertion.success ? 0 : FAILED_INSERTION_PENALTY_SECONDS;
  const insertionOverhead = Number(insertion.preScanSeconds || 0) + Number(insertion.postSeatSeconds || 0) + Number(insertion.calibrationSeconds || 0);
  const rawWorkSec = movementSeconds(robot, cell) + retrySeconds + failurePenalty + insertionOverhead;
  const laneTravelFactor = laneSnapshot?.travelFactor ?? 1;
  const workSec = Math.max(4, Math.round(rawWorkSec * laneTravelFactor));

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
             survey_status = 'pending',
             reference_patch_x = NULL,
             reference_patch_y = NULL,
             reference_patch_protected = FALSE,
             reference_patch_stamped_at = NULL
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
               site_zone = 'MARGINAL',
               reference_patch_x = NULL,
               reference_patch_y = NULL,
               reference_patch_protected = FALSE,
               reference_patch_stamped_at = NULL
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
  const [counts, robotTime, houseTime, placementStats, terrainStats, communityStats, surveyStats, soilStats, maturityStats, blockStats, verificationStats, maintenanceStats, surveyRunStats, laneStats, referencePatchStats] = await Promise.all([
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
    pool.query(
      `SELECT COALESCE(AVG(signature_maturity), 0)::float AS avg_signature_maturity
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
        COALESCE(AVG(CASE WHEN verification_mode = 'inline' THEN CASE WHEN fast_pass THEN 1 ELSE 0 END END), 0)::float AS fast_pass_rate,
        COUNT(*) FILTER (WHERE verification_mode = 'inline' AND decision = 'escalate')::int AS drift_escalations,
        COUNT(*) FILTER (WHERE verification_mode = 'inline' AND escalation_reason = 'tier3_process_sensor_contradiction')::int AS contradiction_escalations,
        COALESCE(SUM(retries) FILTER (WHERE verification_mode = 'inline' AND decision IN ('rework', 'escalate')), 0)::int AS rework_loops,
        COALESCE(AVG(release_confidence) FILTER (WHERE verification_mode = 'inline'), 0)::float AS avg_release_confidence,
        COALESCE(AVG(longevity_confidence) FILTER (WHERE verification_mode = 'inline'), 0)::float AS avg_longevity_confidence
      FROM block_verifications
    `),
    pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE hm.status IN ('alert', 'recoating'))::int AS houses_in_maintenance,
        COALESCE(AVG(hm.ttl_days), 0)::float AS avg_ttl_days,
        COALESCE(AVG(hm.role_weighted_confidence), 0)::float AS avg_ttl_role_weighted_confidence
      FROM house_maintenance hm
      JOIN houses h ON h.id = hm.house_id
      WHERE h.is_active = TRUE
    `),
    pool.query(`
      SELECT
        COALESCE(AVG(sr.uncertainty_remaining), 0)::float AS avg_survey_uncertainty,
        COALESCE(AVG(sr.densification_rounds), 0)::float AS survey_densification_rounds
      FROM survey_runs sr
      JOIN houses h ON h.id = sr.house_id
      WHERE h.is_active = TRUE
    `),
    pool.query(`
      SELECT
        COALESCE(AVG(ls.condition_score), 0)::float AS avg_lane_condition,
        COUNT(*)::int AS lane_segments_total,
        COUNT(*) FILTER (WHERE ls.condition_score >= 0.75)::int AS conditioned_segments,
        COUNT(*) FILTER (WHERE ls.status = 'degraded')::int AS degraded_segments,
        COUNT(*) FILTER (WHERE ls.reference_relay_age_s > $1)::int AS stale_segments,
        COALESCE(SUM(ls.verification_events), 0)::int AS verification_events
      FROM logistics_lane_segments ls
      JOIN houses h ON h.id = ls.house_id
      WHERE h.is_active = TRUE
    `, [LOGISTICS_STALE_RELAY_SECONDS]),
    pool.query(`
      SELECT
        COUNT(*) FILTER (WHERE reference_patch_x IS NOT NULL AND reference_patch_y IS NOT NULL)::int AS reference_total,
        COUNT(*) FILTER (WHERE reference_patch_x IS NOT NULL AND reference_patch_y IS NOT NULL AND reference_patch_protected = TRUE)::int AS reference_protected
      FROM houses
      WHERE is_active = TRUE
    `)
  ]);

  const c = counts.rows[0];
  const t = robotTime.rows[0];
  const p = placementStats.rows[0];
  const terrain = terrainStats.rows[0];
  const community = communityStats.rows[0];
  const lane = laneStats.rows[0] || {};
  const referencePatches = referencePatchStats.rows[0] || {};
  const verification = verificationStats.rows[0] || {};
  const maturity = maturityStats.rows[0] || {};

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

  const laneSegmentsTotal = Number(lane.lane_segments_total || 0);
  const conditionedSegments = Number(lane.conditioned_segments || 0);
  const conditionedLanePercent = laneSegmentsTotal > 0 ? (conditionedSegments / laneSegmentsTotal) * 100 : 0;

  await pool.query(
    `INSERT INTO metrics_samples
      (active_houses, active_robots, houses_completed, cells_filled, total_cells, robot_idle_percent, throughput_cells_per_hour, pipeline_efficiency, placement_corrections, placement_failures, avg_retries_per_placement, terrain_ready_percent, avg_grade_error, avg_compaction, obstacle_cells_remaining, community_kits_activated, community_kits_failed, community_kits_pending, active_surveys, soil_recipes_learned, blocks_verified, blocks_failed_qc, houses_in_maintenance, avg_ttl_days, avg_survey_uncertainty, survey_densification_rounds, avg_lane_condition, conditioned_lane_percent, degraded_lane_segments, stale_relay_segments, lane_verification_events, reference_patches_protected, reference_patches_total, verification_fast_pass_rate, verification_drift_escalations, verification_rework_loops, avg_release_confidence, avg_longevity_confidence, verification_contradictions, avg_signature_maturity, avg_ttl_role_weighted_confidence)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41)`,
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
      Number(maintenanceStats.rows[0]?.avg_ttl_days || 0),
      Number(surveyRunStats.rows[0]?.avg_survey_uncertainty || 0),
      Number(surveyRunStats.rows[0]?.survey_densification_rounds || 0),
      Number(lane.avg_lane_condition || 0),
      conditionedLanePercent,
      Number(lane.degraded_segments || 0),
      Number(lane.stale_segments || 0),
      Number(lane.verification_events || 0),
      Number(referencePatches.reference_protected || 0),
      Number(referencePatches.reference_total || 0),
      Number(verification.fast_pass_rate || 0) * 100,
      Number(verification.drift_escalations || 0),
      Number(verification.rework_loops || 0),
      Number(verification.avg_release_confidence || 0),
      Number(verification.avg_longevity_confidence || 0),
      Number(verification.contradiction_escalations || 0),
      Number(maturity.avg_signature_maturity || 0),
      Number(maintenanceStats.rows[0]?.avg_ttl_role_weighted_confidence || 0)
    ]
  );
}

export async function systemState() {
  const [houses, robots, queue, metrics, cells, terrainCounts, terrainObstacles, assemblyQueue, assemblyCounts, maintenance, surveySummary, surveyRuns, laneSummary, causalGlobal, causalByHouse] = await Promise.all([
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
        COUNT(*) FILTER (WHERE s.status = 'REJECT')::int AS reject,
        ROUND(COALESCE(AVG(s.confidence), 0)::numeric, 3) AS avg_confidence,
        ROUND(COALESCE(AVG(CASE WHEN s.densified THEN 1 ELSE 0 END), 0)::numeric, 3) AS densified_ratio
      FROM site_surveys s
      JOIN houses h ON h.id = s.house_id
      WHERE h.is_active = TRUE
      GROUP BY s.house_id
      ORDER BY s.house_id
    `),
    pool.query(`
      SELECT sr.*
      FROM survey_runs sr
      JOIN houses h ON h.id = sr.house_id
      WHERE h.is_active = TRUE
      ORDER BY sr.updated_at DESC
    `),    pool.query(`
      SELECT
        ls.house_id,
        ls.lane_id,
        COUNT(*)::int AS segments,
        ROUND(COALESCE(AVG(ls.condition_score), 0)::numeric, 3) AS avg_condition,
        ROUND(COALESCE(AVG(ls.grade_match_score), 0)::numeric, 3) AS avg_grade_match,
        ROUND(COALESCE(AVG(ls.body_gauge_match), 0)::numeric, 3) AS avg_body_gauge,
        ROUND(COALESCE(AVG(ls.relay_consensus), 0)::numeric, 3) AS avg_relay_consensus,
        ROUND(COALESCE(AVG(ls.reference_relay_age_s), 0)::numeric, 1) AS avg_relay_age_s,
        COUNT(*) FILTER (WHERE ls.condition_score >= 0.75)::int AS conditioned_segments,
        COUNT(*) FILTER (WHERE ls.status = 'degraded')::int AS degraded_segments,
        COUNT(*) FILTER (WHERE ls.reference_relay_age_s > $1)::int AS stale_segments,
        COALESCE(SUM(ls.verification_events), 0)::int AS verification_events,
        BOOL_OR(ls.restamp_required) AS restamp_required
      FROM logistics_lane_segments ls
      JOIN houses h ON h.id = ls.house_id
      WHERE h.is_active = TRUE
      GROUP BY ls.house_id, ls.lane_id
      ORDER BY ls.house_id, ls.lane_id
    `, [LOGISTICS_STALE_RELAY_SECONDS]),
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
    surveySummary: surveySummary.rows,
    surveyRuns: surveyRuns.rows,
    laneSummary: laneSummary.rows,
    causalGlobal: causalGlobal.rows[0] || {},
    causalByHouse: causalByHouse.rows
  };
}

export async function logisticsLaneSummary() {
  return pool.query(
    `SELECT
       ls.house_id,
       ls.lane_id,
       COUNT(*)::int AS segments,
       ROUND(COALESCE(AVG(ls.condition_score), 0)::numeric, 3) AS avg_condition,
       ROUND(COALESCE(AVG(ls.grade_match_score), 0)::numeric, 3) AS avg_grade_match,
       ROUND(COALESCE(AVG(ls.body_gauge_match), 0)::numeric, 3) AS avg_body_gauge,
       ROUND(COALESCE(AVG(ls.relay_consensus), 0)::numeric, 3) AS avg_relay_consensus,
       ROUND(COALESCE(AVG(ls.reference_relay_age_s), 0)::numeric, 1) AS avg_relay_age_s,
       COUNT(*) FILTER (WHERE ls.condition_score >= 0.75)::int AS conditioned_segments,
       COUNT(*) FILTER (WHERE ls.status = 'degraded')::int AS degraded_segments,
       COUNT(*) FILTER (WHERE ls.reference_relay_age_s > $1)::int AS stale_segments,
       COALESCE(SUM(ls.verification_events), 0)::int AS verification_events,
       BOOL_OR(ls.restamp_required) AS restamp_required
     FROM logistics_lane_segments ls
     JOIN houses h ON h.id = ls.house_id
     WHERE h.is_active = TRUE
     GROUP BY ls.house_id, ls.lane_id
     ORDER BY ls.house_id, ls.lane_id`,
    [LOGISTICS_STALE_RELAY_SECONDS]
  ).then((res) => res.rows);
}
export async function surveyHouse(houseId) {
  return withTx(async (client) => surveyHouseTx(client, Number(houseId)));
}

export async function verifyBlock({ houseId, soilSignature, componentType = "wall", queueId = null }) {
  return withTx(async (client) => verifyBlockTx(client, {
    houseId: Number(houseId),
    soilSignature,
    componentType,
    queueId
  }));
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






















