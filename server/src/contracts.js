// Master Interface Contract - House Brain Stigmergic Hive
// Single source of truth for all command, telemetry, and feedback schemas.
// scheduler.js and index.js can import and validate these at runtime.

// LAYER 1: COMMAND CONTRACTS (House Brain -> Robot)
export const COMMANDS = {
  survey: [
    "load_zone_target(zone_id)",
    "deploy_feet()",
    "equalize_load()",
    "seat_outer_sleeve(depth)",
    "probe_inner(depth)",
    "classify_point()",
    "densify_region(region_id)",
    "stamp_reference_patch()",
    "emit_heatmap(site_id)",
    "abort_probe(reason)"
  ],
  excavator: [
    "load_survey_zone_profile(zone_id)",
    "check_slurry_risk(zone_id)",
    "confirm_container_ready(container_id)",
    "goto(zone_id)",
    "penetrate(depth)",
    "engage_blades()",
    "start_lift()",
    "pause_lift(reason)",
    "route_good_stream(container_id)",
    "route_reject_stream(reject_id)",
    "clear_jam()",
    "emit_excavation_report(zone_id)",
    "emit_ground_truth_record(zone_id)",
    "abort_job(reason)"
  ],
  fabricator: [
    "load_recipe(recipe_id)",
    "check_feed_moisture()",
    "apply_conditioning(type, dose)",
    "deploy_anchor()",
    "confirm_anchor_seating()",
    "relocate_anchor(offset)",
    "fill_mold(volume_target)",
    "compress()",
    "eject_block()",
    "begin_dwell(dwell_seconds)",
    "release_to_verification(part_id)",
    "update_fill_volume(delta)",
    "update_dwell_seconds(value)",
    "flag_convergence_limit(batch_id)",
    "emit_fabrication_record(part_id)",
    "emit_feed_quality_record(batch_id)",
    "recycle_calibration_block(part_id)"
  ],
  verification: [
    "qc(block_id)",
    "runWeathering(signature)",
    "updateConfidence(signature)",
    "emit_fill_correction(part_id)",
    "emit_dwell_recommendation(part_id)",
    "emit_ttl_update(soil_signature)",
    "emit_neighbor_drift_response(house_id)"
  ],
  assembly: [
    "load_insertion_profile(component_type)",
    "claimCell(house_id)",
    "pre_insertion_drift_scan(cell_id)",
    "moveTo(x,y,z)",
    "place(block_id,x,y,z)",
    "post_seat_micro_load_check(cell_id)",
    "emit_placement_report(cell_id)",
    "emit_neighbor_drift_telemetry(house_id)",
    "escalate_geometry_failure(cell_id)"
  ],
  sealer: [
    "load_zone_vulnerability_map(house_id)",
    "broad_spray(house_id)",
    "confirm_moisture_baseline_reset(zone_id)",
    "focused_pressure_spray(zone_id)",
    "fuse_zone_scores(house_id)",
    "apply_edge_band_coat(zone_id)",
    "apply_zone_coat(zone_id)",
    "emit_water_stress_record(house_id)",
    "trigger_recoat(house_id)",
    "escalate_zone_failure(zone_id)"
  ],
  logistics: [
    "goto(x,y)",
    "pickup(payload_id)",
    "dock(station_id)",
    "lower_body_gauge()",
    "grade_until_match(segment_id)",
    "relay_reference_state()",
    "emit_lane_status()",
    "yield(robot_id)",
    "return_to_base()"
  ]
};

// LAYER 2: TELEMETRY SCHEMAS (Robot -> House Brain)
export const TELEMETRY_SCHEMAS = {
  site_survey_probe: {
    required: [
      "house_id", "probe_x", "probe_y", "depth_meters", "soil_signature",
      "status", "penetration_resistance", "moisture_pct", "confidence",
      "foot_balance_score", "sleeve_seat_score", "brace_failure",
      "partial_penetration", "penetration_curve", "round_index"
    ],
    optional: [
      "surface_tilt_deg", "organic_pct", "salinity",
      "classification_reason", "densified", "outer_brace_status",
      "max_probe_depth_m"
    ],
    enums: { status: ["BUILDABLE", "MARGINAL", "REJECT"] }
  },

  excavation_record: {
    required: [
      "robot_id", "zone_id", "depth_m", "blade_load_score",
      "lift_flow_score", "good_stream_rate", "reject_stream_rate",
      "jam_state", "blade_wear_index", "container_fill_pct",
      "backpressure_state", "timestamp"
    ],
    optional: ["expected_hard_layer_depth_m", "actual_hard_layer_depth_m"],
    enums: { backpressure_state: ["nominal", "elevated", "critical"] }
  },

  excavator_ground_truth_record: {
    required: [
      "robot_id", "zone_id", "survey_predicted_hard_layer_m",
      "actual_hard_layer_m", "survey_predicted_reject_ratio",
      "actual_reject_ratio", "blade_load_profile", "prediction_delta",
      "timestamp"
    ],
    optional: [],
    enums: { prediction_delta: ["within_tolerance", "over_predicted", "under_predicted"] }
  },

  fabrication_record: {
    required: [
      "part_id", "recipe_id", "batch_id", "fill_volume_used_L",
      "fill_volume_target_L", "incoming_moisture_pct",
      "conditioning_applied", "anchor_seat_confidence",
      "compression_force_kN", "press_depth_mm",
      "ejection_dwell_seconds", "calibration_cycle_count",
      "block_type", "timestamp"
    ],
    optional: ["convergence_limit_hit"],
    enums: {
      conditioning_applied: ["none", "water_dose", "dry_pass"],
      block_type: ["production", "calibration"]
    }
  },

  fabricator_feed_quality_record: {
    required: [
      "batch_id", "source_zone_id", "calibration_cycles_to_convergence",
      "fill_volume_delta_from_nominal", "conditioning_applied",
      "batch_reject_rate", "convergence_limit_hit", "assessment",
      "timestamp"
    ],
    optional: [],
    enums: {
      assessment: ["gate_well_tuned", "gate_slightly_permissive", "gate_too_permissive", "soil_flagged"]
    }
  },

  block_verification_record: {
    required: ["house_id", "soil_signature", "passed", "verification_mode", "weathering_cycles"],
    optional: [
      "penetration_resistance", "moisture_pct", "density",
      "retries", "fill_volume_correction_delta",
      "recommended_dwell_seconds", "handling_damage_flag"
    ],
    enums: { verification_mode: ["inline", "accelerated_weathering"] }
  },

  placement_report: {
    required: [
      "robot_id", "house_id", "cell_id", "component_type",
      "insertion_result", "retries", "resistance_at_seat",
      "calibration_pass", "pre_scan_drift_detected",
      "post_seat_micro_load_passed", "work_seconds", "timestamp"
    ],
    optional: ["escalation_reason", "neighbor_geometry_delta"],
    enums: {
      insertion_result: ["success", "failed", "escalated"],
      component_type: ["foundation", "wall", "mep", "roof"]
    }
  },

  neighbor_drift_telemetry: {
    required: [
      "robot_id", "house_id", "zone_id",
      "drift_detected", "drift_magnitude", "affected_cells",
      "timestamp"
    ],
    optional: ["escalated_to_house_brain"]
  },

  water_stress_record: {
    required: [
      "house_id", "broad_spray_ingress_score",
      "pressure_stress_score", "material_family_id",
      "zone_scores", "edge_band_scores",
      "baseline_moisture_pct", "reset_confirmed", "timestamp"
    ],
    optional: ["post_recoat_delta", "recoat_zone_state", "pre_recoat_ingress_delta", "gravity_bias_corrected"],
    enums: { recoat_zone_state: ["intact_aging", "degraded", "failed"] }
  },

  lane_segment_record: {
    required: [
      "house_id", "lane_id", "segment_index",
      "condition_score", "grade_match_score",
      "body_gauge_match", "relay_consensus",
      "reference_relay_age_s", "status", "passes",
      "verification_events", "restamp_required"
    ],
    optional: ["avg_drag_force", "last_verified_at"],
    enums: { status: ["raw", "conditioning", "stable", "degraded"] }
  }
};

// LAYER 3: FEEDBACK CONTRACTS (Robot -> Robot via House Brain)
export const FEEDBACK_PATHS = [
  {
    id: "excavator_to_survey",
    source: "excavator",
    destination: "survey",
    mediating_table: "site_surveys",
    trigger: "zone_completion",
    signal: "excavator_ground_truth_record",
    feedback_action: "calibrate_penetration_classification_model",
    fields: ["actual_hard_layer_m", "actual_reject_ratio", "blade_load_profile", "prediction_delta"],
    staleness_threshold_seconds: 300
  },
  {
    id: "fabricator_to_excavator",
    source: "fabricator",
    destination: "excavator",
    mediating_table: "fabricator_feed_quality_records",
    trigger: "batch_completion",
    signal: "fabricator_feed_quality_record",
    feedback_action: "tune_binary_gate_aperture_per_zone",
    fields: [
      "calibration_cycles_to_convergence", "fill_volume_delta_from_nominal",
      "conditioning_applied", "batch_reject_rate", "source_zone_id"
    ],
    staleness_threshold_seconds: 600
  },
  {
    id: "survey_to_excavator",
    source: "survey",
    destination: "excavator",
    mediating_table: "site_surveys",
    trigger: "pre_zone_engagement",
    signal: "zone_profile",
    feedback_action: "preload_depth_profile_and_moisture",
    fields: ["penetration_curve", "moisture_pct", "expected_hard_layer_depth_m", "zone_moisture_pct"],
    staleness_threshold_seconds: 900
  },
  {
    id: "survey_to_fabricator",
    source: "survey",
    destination: "fabricator",
    mediating_table: "site_surveys",
    trigger: "pre_batch_moisture_warning",
    signal: "zone_moisture_score",
    feedback_action: "pre_arm_conditioning_gate",
    fields: ["moisture_pct", "zone_id", "soil_signature"],
    staleness_threshold_seconds: 600
  },
  {
    id: "verification_to_fabricator_fill",
    source: "verification",
    destination: "fabricator",
    mediating_table: "block_verifications",
    trigger: "block_qc_result",
    signal: "fill_correction",
    feedback_action: "update_fill_volume_target",
    fields: ["fill_volume_correction_delta", "block_type", "density", "passed"],
    staleness_threshold_seconds: 60
  },
  {
    id: "verification_to_fabricator_dwell",
    source: "verification",
    destination: "fabricator",
    mediating_table: "block_verifications",
    trigger: "handling_damage_detected",
    signal: "dwell_recommendation",
    feedback_action: "update_ejection_dwell_seconds",
    fields: ["recommended_dwell_seconds", "handling_damage_flag"],
    staleness_threshold_seconds: 60
  },
  {
    id: "sealer_to_verification",
    source: "sealer",
    destination: "verification",
    mediating_table: "house_maintenance",
    trigger: "sealing_complete_or_recoat",
    signal: "water_stress_record",
    feedback_action: "calibrate_ttl_library_from_real_world_stress",
    fields: [
      "broad_spray_ingress_score", "pressure_stress_score",
      "post_recoat_delta", "material_family_id", "house_id"
    ],
    staleness_threshold_seconds: 1800
  },
  {
    id: "assembly_to_verification",
    source: "assembly",
    destination: "verification",
    mediating_table: "grid_cells",
    trigger: "neighbor_drift_detected",
    signal: "neighbor_drift_telemetry",
    feedback_action: "adjust_ttl_confidence_for_drifted_zone",
    fields: ["drift_magnitude", "affected_cells", "zone_id", "house_id"],
    staleness_threshold_seconds: 120
  },
  {
    id: "logistics_to_assembly",
    source: "logistics",
    destination: "assembly",
    mediating_table: "logistics_lane_segments",
    trigger: "per_pass",
    signal: "travel_factor",
    feedback_action: "scale_work_seconds_by_lane_condition",
    fields: ["condition_score", "relay_consensus", "reference_relay_age_s", "status"],
    staleness_threshold_seconds: 30
  },
  {
    id: "survey_to_logistics",
    source: "survey",
    destination: "logistics",
    mediating_table: "houses",
    trigger: "survey_approval",
    signal: "reference_patch_coords",
    feedback_action: "seed_relay_reference_origin",
    fields: [
      "reference_patch_x", "reference_patch_y",
      "reference_patch_protected", "recommended_build_zone"
    ],
    staleness_threshold_seconds: 3600
  }
];

// LAYER 4: PRECONDITION TREE (Cross-Robot Handshake Gates)
// This makes scheduler dependencies explicit:
// Excavator/Site Prep -> Fabricator -> Verification -> Assembly.
export const PRECONDITION_TREE = [
  {
    id: "excavator_to_fabricator",
    step: "Excavator -> Fabricator",
    handoff_signal: "surface_container_ready",
    depends_on: [],
    preconditions: [
      { id: "terrain_ready", label: "Terrain ready houses", metric: "houses_with_terrain_ready", minimum: 1 },
      { id: "survey_approved", label: "Survey-approved houses", metric: "houses_with_survey_approved", minimum: 1 }
    ],
    blocked_message: "Fabricator blocked until site prep and survey gate are satisfied."
  },
  {
    id: "fabricator_to_verification",
    step: "Fabricator -> Verification",
    handoff_signal: "fabrication_record_emitted",
    depends_on: ["excavator_to_fabricator"],
    preconditions: [
      { id: "ready_components", label: "Ready fabricated components", metric: "ready_fabricator_components", minimum: 1 },
      { id: "houses_with_ready_components", label: "Houses with ready components", metric: "houses_with_ready_fabricator_component", minimum: 1 }
    ],
    blocked_message: "Verification blocked until Fabricator emits ready components."
  },
  {
    id: "verification_to_assembly",
    step: "Verification -> Assembly",
    handoff_signal: "verification_approved",
    depends_on: ["fabricator_to_verification"],
    preconditions: [
      { id: "verification_approvals", label: "Inline verification approvals", metric: "verification_approvals", minimum: 1 },
      { id: "houses_with_qc_pass", label: "Houses with QC approval", metric: "houses_with_verification_approval", minimum: 1 }
    ],
    blocked_message: "Assembly blocked until Verification approves block quality."
  },
  {
    id: "assembly_execution",
    step: "Assembly Execution",
    handoff_signal: "placement_report",
    depends_on: ["verification_to_assembly"],
    preconditions: [
      { id: "reserved_cells", label: "Reserved cells awaiting placement", metric: "reserved_cells_waiting_assembly", minimum: 1 },
      { id: "active_assembly_robots", label: "Assembly robots active", metric: "active_assembly_robots", minimum: 1 }
    ],
    blocked_message: "No assembly placement can occur without claims and active assembly robots."
  }
];
export function validateTelemetry(schemaName, record) {
  const schema = TELEMETRY_SCHEMAS[schemaName];
  if (!schema) {
    return { valid: false, errors: [`Unknown schema: ${schemaName}`] };
  }

  const errors = [];

  for (const field of schema.required) {
    if (record[field] === undefined || record[field] === null) {
      errors.push(`Missing required field: ${field}`);
    }
  }

  if (schema.enums) {
    for (const [field, allowed] of Object.entries(schema.enums)) {
      if (record[field] !== undefined && !allowed.includes(record[field])) {
        errors.push(`Invalid value for ${field}: "${record[field]}" - allowed: ${allowed.join(", ")}`);
      }
    }
  }

  return { valid: errors.length === 0, errors, schema: schemaName };
}

export function getFeedbackPathHealth(pathId, lastEmittedAt) {
  const path = FEEDBACK_PATHS.find((p) => p.id === pathId);
  if (!path) return { status: "unknown", pathId };

  if (!lastEmittedAt) return { status: "never_emitted", pathId, path };

  const ageSeconds = (Date.now() - new Date(lastEmittedAt).getTime()) / 1000;
  const status = ageSeconds > path.staleness_threshold_seconds ? "stale" : "live";

  return { status, ageSeconds, threshold: path.staleness_threshold_seconds, pathId, path };
}

export function buildContractHealthSummary(lastEmittedByPathId) {
  return FEEDBACK_PATHS.map((path) => ({
    ...getFeedbackPathHealth(path.id, lastEmittedByPathId[path.id]),
    source: path.source,
    destination: path.destination,
    signal: path.signal,
    feedback_action: path.feedback_action,
    mediating_table: path.mediating_table
  }));
}

function evaluatePrecondition(snapshot, condition) {
  const value = Number(snapshot?.[condition.metric] || 0);
  return {
    ...condition,
    value,
    passed: value >= Number(condition.minimum || 0)
  };
}

export function buildPreconditionSummary(snapshot = {}) {
  const satisfied = new Set();

  return PRECONDITION_TREE.map((node) => {
    const dependencyFailures = (node.depends_on || []).filter((depId) => !satisfied.has(depId));
    const checks = (node.preconditions || []).map((condition) => evaluatePrecondition(snapshot, condition));
    const checkFailures = checks.filter((check) => !check.passed).map((check) => check.label);

    const isReady = dependencyFailures.length === 0 && checkFailures.length === 0;
    if (isReady) satisfied.add(node.id);

    const blockedBy = [
      ...dependencyFailures.map((depId) => `dependency:${depId}`),
      ...checkFailures
    ];

    return {
      id: node.id,
      step: node.step,
      handoff_signal: node.handoff_signal,
      status: isReady ? "ready" : "blocked",
      depends_on: node.depends_on || [],
      requires: checks,
      blocked_by: blockedBy,
      blocked_message: isReady ? null : node.blocked_message,
      evidence: checks.map((check) => `${check.label} ${check.value}/${check.minimum}`).join(" | ")
    };
  });
}

