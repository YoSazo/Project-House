CREATE TABLE IF NOT EXISTS houses (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  stage TEXT NOT NULL CHECK (stage IN ('site_prep', 'surveying', 'foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete')) DEFAULT 'site_prep',
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  cluster_id INTEGER,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  survey_status TEXT NOT NULL DEFAULT 'pending',
  soil_signature TEXT,
  site_zone TEXT NOT NULL DEFAULT 'BUILDABLE',
  sealing_complete BOOLEAN NOT NULL DEFAULT FALSE,
  sealing_started_at TIMESTAMPTZ,
  sealing_progress NUMERIC(6,3) NOT NULL DEFAULT 0,
  reference_patch_x INTEGER,
  reference_patch_y INTEGER,
  reference_patch_protected BOOLEAN NOT NULL DEFAULT FALSE,
  reference_patch_stamped_at TIMESTAMPTZ
);

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'houses_stage_check'
  ) THEN
    ALTER TABLE houses DROP CONSTRAINT houses_stage_check;
  END IF;
END $$;

ALTER TABLE houses
  ADD CONSTRAINT houses_stage_check
  CHECK (stage IN ('site_prep', 'surveying', 'foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete'));

ALTER TABLE houses
  ALTER COLUMN stage SET DEFAULT 'site_prep';

ALTER TABLE houses ADD COLUMN IF NOT EXISTS survey_status TEXT NOT NULL DEFAULT 'pending';
ALTER TABLE houses ADD COLUMN IF NOT EXISTS soil_signature TEXT;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS site_zone TEXT NOT NULL DEFAULT 'BUILDABLE';
ALTER TABLE houses ADD COLUMN IF NOT EXISTS sealing_complete BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS sealing_started_at TIMESTAMPTZ;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS sealing_progress NUMERIC(6,3) NOT NULL DEFAULT 0;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS reference_patch_x INTEGER;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS reference_patch_y INTEGER;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS reference_patch_protected BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS reference_patch_stamped_at TIMESTAMPTZ;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'houses_survey_status_check'
  ) THEN
    ALTER TABLE houses DROP CONSTRAINT houses_survey_status_check;
  END IF;
END $$;

ALTER TABLE houses
  ADD CONSTRAINT houses_survey_status_check
  CHECK (survey_status IN ('pending', 'surveying', 'approved', 'rejected'));

CREATE TABLE IF NOT EXISTS robot_clusters (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('idle', 'busy')) DEFAULT 'idle',
  current_house_id INTEGER REFERENCES houses(id) ON DELETE SET NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_houses_cluster'
  ) THEN
    ALTER TABLE houses
      ADD CONSTRAINT fk_houses_cluster
      FOREIGN KEY (cluster_id) REFERENCES robot_clusters(id) ON DELETE SET NULL;
  END IF;
END $$;

CREATE TABLE IF NOT EXISTS robots (
  id SERIAL PRIMARY KEY,
  cluster_id INTEGER NOT NULL REFERENCES robot_clusters(id) ON DELETE CASCADE,
  name TEXT NOT NULL,
  chassis_id TEXT,            -- physical chassis serial (Skateboard identity)
  toolhead_id TEXT,           -- currently mounted toolhead serial (NULL = bare chassis)
  toolhead_type TEXT,         -- current toolhead type for quick lookup
  status TEXT NOT NULL CHECK (status IN ('idle', 'moving', 'waiting_component', 'placing')) DEFAULT 'idle',
  pos_x INTEGER NOT NULL DEFAULT 0,
  pos_y INTEGER NOT NULL DEFAULT 0,
  pos_z INTEGER NOT NULL DEFAULT 0,
  busy_until TIMESTAMPTZ,
  active_house_id INTEGER REFERENCES houses(id) ON DELETE SET NULL,
  total_work_seconds INTEGER NOT NULL DEFAULT 0,
  total_idle_seconds INTEGER NOT NULL DEFAULT 0,
  total_placements INTEGER NOT NULL DEFAULT 0,
  total_placement_retries INTEGER NOT NULL DEFAULT 0,
  total_placement_failures INTEGER NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS grid_cells (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  z INTEGER NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('empty', 'reserved', 'filled')) DEFAULT 'empty',
  component_type TEXT NOT NULL CHECK (component_type IN ('foundation', 'wall', 'mep', 'roof')),
  build_sequence INTEGER NOT NULL,
  assigned_robot_id INTEGER REFERENCES robots(id) ON DELETE SET NULL,
  reserved_at TIMESTAMPTZ,
  filled_at TIMESTAMPTZ,
  UNIQUE (house_id, x, y, z)
);

CREATE INDEX IF NOT EXISTS idx_grid_cells_status_sequence ON grid_cells (house_id, status, build_sequence);
CREATE INDEX IF NOT EXISTS idx_grid_cells_component_status ON grid_cells (house_id, component_type, status);

CREATE TABLE IF NOT EXISTS terrain_cells (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  x INTEGER NOT NULL,
  y INTEGER NOT NULL,
  target_grade NUMERIC(8,3) NOT NULL,
  current_grade NUMERIC(8,3) NOT NULL,
  soil_density NUMERIC(5,3) NOT NULL,
  compaction_score NUMERIC(5,3) NOT NULL DEFAULT 0,
  obstacle_type TEXT NOT NULL CHECK (obstacle_type IN ('none', 'root', 'rock', 'debris')) DEFAULT 'none',
  obstacle_cleared BOOLEAN NOT NULL DEFAULT TRUE,
  status TEXT NOT NULL CHECK (status IN ('raw', 'grading', 'compacted', 'ready')) DEFAULT 'raw',
  assigned_robot_id INTEGER REFERENCES robots(id) ON DELETE SET NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (house_id, x, y)
);

CREATE INDEX IF NOT EXISTS idx_terrain_cells_house_status ON terrain_cells (house_id, status);
CREATE INDEX IF NOT EXISTS idx_terrain_cells_assigned_robot ON terrain_cells (assigned_robot_id);

ALTER TABLE terrain_cells ADD COLUMN IF NOT EXISTS obstacle_type TEXT NOT NULL DEFAULT 'none';
ALTER TABLE terrain_cells ADD COLUMN IF NOT EXISTS obstacle_cleared BOOLEAN NOT NULL DEFAULT TRUE;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'terrain_cells_obstacle_type_check'
  ) THEN
    ALTER TABLE terrain_cells DROP CONSTRAINT terrain_cells_obstacle_type_check;
  END IF;
END $$;

ALTER TABLE terrain_cells
  ADD CONSTRAINT terrain_cells_obstacle_type_check
  CHECK (obstacle_type IN ('none', 'root', 'rock', 'debris'));

CREATE TABLE IF NOT EXISTS logistics_lane_segments (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  lane_id TEXT NOT NULL DEFAULT 'lane_main',
  segment_index INTEGER NOT NULL,
  start_x NUMERIC(8,3) NOT NULL,
  start_y NUMERIC(8,3) NOT NULL,
  end_x NUMERIC(8,3) NOT NULL,
  end_y NUMERIC(8,3) NOT NULL,
  condition_score NUMERIC(8,4) NOT NULL DEFAULT 0.22,
  grade_match_score NUMERIC(8,4) NOT NULL DEFAULT 0,
  avg_drag_force NUMERIC(8,4) NOT NULL DEFAULT 0,
  body_gauge_match NUMERIC(8,4) NOT NULL DEFAULT 0,
  relay_consensus NUMERIC(8,4) NOT NULL DEFAULT 0,
  reference_relay_age_s INTEGER NOT NULL DEFAULT 999,
  passes INTEGER NOT NULL DEFAULT 0,
  verification_events INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL CHECK (status IN ('raw', 'conditioning', 'stable', 'degraded')) DEFAULT 'raw',
  is_reference_zone BOOLEAN NOT NULL DEFAULT FALSE,
  restamp_required BOOLEAN NOT NULL DEFAULT FALSE,
  last_verified_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (house_id, lane_id, segment_index)
);

CREATE INDEX IF NOT EXISTS idx_logistics_lane_segments_house ON logistics_lane_segments (house_id, lane_id);
CREATE INDEX IF NOT EXISTS idx_logistics_lane_segments_status ON logistics_lane_segments (status, updated_at DESC);
CREATE TABLE IF NOT EXISTS fabricator_queue (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  cell_id BIGINT NOT NULL REFERENCES grid_cells(id) ON DELETE CASCADE,
  component_type TEXT NOT NULL CHECK (component_type IN ('foundation', 'wall', 'mep', 'roof')),
  status TEXT NOT NULL CHECK (status IN ('queued', 'ready', 'consumed')) DEFAULT 'queued',
  ready_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  consumed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_fabricator_queue_status_ready ON fabricator_queue (status, ready_at);

CREATE TABLE IF NOT EXISTS assembly_kits (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  kit_index INTEGER NOT NULL,
  family_label TEXT NOT NULL DEFAULT 'unassigned',
  status TEXT NOT NULL CHECK (status IN ('queued', 'assembling', 'qa', 'failed', 'passed', 'activated')) DEFAULT 'queued',
  progress NUMERIC(6,3) NOT NULL DEFAULT 0,
  retries INTEGER NOT NULL DEFAULT 0,
  activated_robot_id INTEGER REFERENCES robots(id) ON DELETE SET NULL,
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (house_id, kit_index)
);

CREATE INDEX IF NOT EXISTS idx_assembly_kits_house_status ON assembly_kits (house_id, status);
CREATE INDEX IF NOT EXISTS idx_assembly_kits_status ON assembly_kits (status);

CREATE TABLE IF NOT EXISTS soil_library (
  id BIGSERIAL PRIMARY KEY,
  soil_signature TEXT NOT NULL UNIQUE,
  clay_pct NUMERIC(6,3),
  sand_pct NUMERIC(6,3),
  organic_pct NUMERIC(6,3),
  salinity NUMERIC(6,3),
  recipe JSONB NOT NULL DEFAULT '{"clay":45,"sand":40,"lime":10,"cement":5}'::jsonb,
  short_term_confidence NUMERIC(8,4) NOT NULL DEFAULT 0,
  long_term_confidence NUMERIC(8,4) NOT NULL DEFAULT 0,
  weathering_cycles_tested INTEGER NOT NULL DEFAULT 0,
  erosion_score NUMERIC(8,4),
  total_blocks_verified INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS site_surveys (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  probe_x INTEGER NOT NULL,
  probe_y INTEGER NOT NULL,
  depth_meters NUMERIC(8,3) NOT NULL,
  soil_signature TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('BUILDABLE', 'MARGINAL', 'REJECT')),
  penetration_resistance NUMERIC(10,4),
  moisture_pct NUMERIC(10,4),
  organic_pct NUMERIC(10,4),
  salinity NUMERIC(10,4),
  notes TEXT,
  surveyed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_site_surveys_house ON site_surveys (house_id, surveyed_at DESC);
CREATE INDEX IF NOT EXISTS idx_site_surveys_status ON site_surveys (status);

CREATE TABLE IF NOT EXISTS block_verifications (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER REFERENCES houses(id) ON DELETE SET NULL,
  soil_signature TEXT NOT NULL,
  penetration_resistance NUMERIC(10,4),
  moisture_pct NUMERIC(10,4),
  density NUMERIC(10,4),
  passed BOOLEAN NOT NULL DEFAULT FALSE,
  retries INTEGER NOT NULL DEFAULT 0,
  verification_mode TEXT NOT NULL CHECK (verification_mode IN ('inline', 'accelerated_weathering')),
  weathering_cycles INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_block_verifications_signature ON block_verifications (soil_signature, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_block_verifications_house ON block_verifications (house_id, created_at DESC);

CREATE TABLE IF NOT EXISTS house_maintenance (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL UNIQUE REFERENCES houses(id) ON DELETE CASCADE,
  coating_version INTEGER NOT NULL DEFAULT 1,
  coating_applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  ttl_expires_at TIMESTAMPTZ,
  alert_at TIMESTAMPTZ,
  ttl_days INTEGER NOT NULL DEFAULT 0,
  improved_recipe JSONB,
  status TEXT NOT NULL CHECK (status IN ('ok', 'alert', 'recoating', 'complete')) DEFAULT 'ok',
  next_ttl_multiplier NUMERIC(8,3) NOT NULL DEFAULT 2.0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_house_maintenance_alert ON house_maintenance (status, alert_at);

CREATE TABLE IF NOT EXISTS metrics_samples (
  id BIGSERIAL PRIMARY KEY,
  sampled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  active_houses INTEGER NOT NULL DEFAULT 0,
  active_robots INTEGER NOT NULL DEFAULT 0,
  houses_completed INTEGER NOT NULL,
  cells_filled INTEGER NOT NULL,
  total_cells INTEGER NOT NULL,
  robot_idle_percent NUMERIC(6,2) NOT NULL,
  throughput_cells_per_hour NUMERIC(10,2) NOT NULL,
  pipeline_efficiency NUMERIC(6,2) NOT NULL,
  placement_corrections INTEGER NOT NULL DEFAULT 0,
  placement_failures INTEGER NOT NULL DEFAULT 0,
  avg_retries_per_placement NUMERIC(8,3) NOT NULL DEFAULT 0,
  terrain_ready_percent NUMERIC(6,2) NOT NULL DEFAULT 100,
  avg_grade_error NUMERIC(8,4) NOT NULL DEFAULT 0,
  avg_compaction NUMERIC(8,4) NOT NULL DEFAULT 1,
  obstacle_cells_remaining INTEGER NOT NULL DEFAULT 0,
  community_kits_activated INTEGER NOT NULL DEFAULT 0,
  community_kits_failed INTEGER NOT NULL DEFAULT 0,
  community_kits_pending INTEGER NOT NULL DEFAULT 0,
  active_surveys INTEGER NOT NULL DEFAULT 0,
  soil_recipes_learned INTEGER NOT NULL DEFAULT 0,
  blocks_verified INTEGER NOT NULL DEFAULT 0,
  blocks_failed_qc INTEGER NOT NULL DEFAULT 0,
  houses_in_maintenance INTEGER NOT NULL DEFAULT 0,
  avg_ttl_days NUMERIC(10,3) NOT NULL DEFAULT 0,
  avg_lane_condition NUMERIC(8,4) NOT NULL DEFAULT 0,
  conditioned_lane_percent NUMERIC(6,2) NOT NULL DEFAULT 0,
  degraded_lane_segments INTEGER NOT NULL DEFAULT 0,
  stale_relay_segments INTEGER NOT NULL DEFAULT 0,
  lane_verification_events INTEGER NOT NULL DEFAULT 0,
  reference_patches_protected INTEGER NOT NULL DEFAULT 0,
  reference_patches_total INTEGER NOT NULL DEFAULT 0
);

ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS active_houses INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS active_robots INTEGER NOT NULL DEFAULT 0;
ALTER TABLE robots ADD COLUMN IF NOT EXISTS total_placements INTEGER NOT NULL DEFAULT 0;
ALTER TABLE robots ADD COLUMN IF NOT EXISTS total_placement_retries INTEGER NOT NULL DEFAULT 0;
ALTER TABLE robots ADD COLUMN IF NOT EXISTS total_placement_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS placement_corrections INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS placement_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_retries_per_placement NUMERIC(8,3) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS terrain_ready_percent NUMERIC(6,2) NOT NULL DEFAULT 100;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_grade_error NUMERIC(8,4) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_compaction NUMERIC(8,4) NOT NULL DEFAULT 1;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS obstacle_cells_remaining INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS community_kits_activated INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS community_kits_failed INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS community_kits_pending INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS active_surveys INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS soil_recipes_learned INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS blocks_verified INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS blocks_failed_qc INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS houses_in_maintenance INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_ttl_days NUMERIC(10,3) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_lane_condition NUMERIC(8,4) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS conditioned_lane_percent NUMERIC(6,2) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS degraded_lane_segments INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS stale_relay_segments INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS lane_verification_events INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS reference_patches_protected INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS reference_patches_total INTEGER NOT NULL DEFAULT 0;

ALTER TABLE houses ADD COLUMN IF NOT EXISTS recommended_build_zone TEXT;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS survey_uncertainty_remaining NUMERIC(8,4) NOT NULL DEFAULT 1;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS survey_stop_reason TEXT;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS survey_probe_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE houses ADD COLUMN IF NOT EXISTS material_family_mix JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS surface_tilt_deg NUMERIC(8,3);
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS foot_balance_score NUMERIC(8,4);
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS sleeve_seat_score NUMERIC(8,4);
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS outer_brace_status TEXT;
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS max_probe_depth_m NUMERIC(8,3);
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS penetration_curve JSONB NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS brace_failure BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS partial_penetration BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS confidence NUMERIC(8,4);
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS classification_reason TEXT;
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS densified BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE site_surveys ADD COLUMN IF NOT EXISTS round_index INTEGER NOT NULL DEFAULT 0;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'site_surveys_outer_brace_status_check'
  ) THEN
    ALTER TABLE site_surveys DROP CONSTRAINT site_surveys_outer_brace_status_check;
  END IF;
END $$;

ALTER TABLE site_surveys
  ADD CONSTRAINT site_surveys_outer_brace_status_check
  CHECK (outer_brace_status IS NULL OR outer_brace_status IN ('stable', 'partial', 'failed'));

CREATE TABLE IF NOT EXISTS survey_runs (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL UNIQUE REFERENCES houses(id) ON DELETE CASCADE,
  run_status TEXT NOT NULL CHECK (run_status IN ('running', 'complete', 'budget_exhausted', 'unstable')) DEFAULT 'running',
  uncertainty_remaining NUMERIC(8,4) NOT NULL DEFAULT 1,
  probe_budget INTEGER NOT NULL DEFAULT 48,
  probes_used INTEGER NOT NULL DEFAULT 0,
  densification_rounds INTEGER NOT NULL DEFAULT 0,
  boundary_shift NUMERIC(8,4) NOT NULL DEFAULT 1,
  stopped_reason TEXT,
  recommended_build_zone TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_survey_runs_status ON survey_runs (run_status, updated_at DESC);

ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_survey_uncertainty NUMERIC(8,4) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS survey_densification_rounds NUMERIC(8,3) NOT NULL DEFAULT 0;



ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS verification_fast_pass_rate NUMERIC(8,3) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS verification_drift_escalations INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS verification_rework_loops INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_release_confidence NUMERIC(8,3) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_longevity_confidence NUMERIC(8,3) NOT NULL DEFAULT 0;

ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS process_signature JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS reference_sample_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS fast_pass_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS drift_escalations INTEGER NOT NULL DEFAULT 0;
ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS correction_events INTEGER NOT NULL DEFAULT 0;
ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS release_confidence NUMERIC(8,4) NOT NULL DEFAULT 0;
ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS longevity_confidence NUMERIC(8,4) NOT NULL DEFAULT 0;
ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS last_requested_role TEXT;
ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS last_drift_score NUMERIC(8,4);

ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS block_id TEXT;
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS requested_role TEXT;
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS requested_spec JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS process_signature JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS machine_truth JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS process_match_score NUMERIC(8,4);
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS characteristic_score NUMERIC(8,4);
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS drift_score NUMERIC(8,4);
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS release_confidence NUMERIC(8,4);
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS longevity_confidence NUMERIC(8,4);
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS fast_pass BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS decision TEXT;
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS escalation_reason TEXT;
ALTER TABLE block_verifications ADD COLUMN IF NOT EXISTS correction_delta JSONB NOT NULL DEFAULT '{}'::jsonb;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'block_verifications_decision_check'
  ) THEN
    ALTER TABLE block_verifications DROP CONSTRAINT block_verifications_decision_check;
  END IF;
END $$;

ALTER TABLE block_verifications
  ADD CONSTRAINT block_verifications_decision_check
  CHECK (decision IS NULL OR decision IN ('approve', 'reject', 'rework', 'escalate'));


ALTER TABLE soil_library ADD COLUMN IF NOT EXISTS signature_maturity NUMERIC(8,4) NOT NULL DEFAULT 0;

ALTER TABLE house_maintenance ADD COLUMN IF NOT EXISTS role_weighted_confidence NUMERIC(8,4) NOT NULL DEFAULT 0;
ALTER TABLE house_maintenance ADD COLUMN IF NOT EXISTS weakest_critical_confidence NUMERIC(8,4) NOT NULL DEFAULT 0;
ALTER TABLE house_maintenance ADD COLUMN IF NOT EXISTS critical_constraint_role TEXT;

ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS verification_contradictions INTEGER NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_signature_maturity NUMERIC(8,3) NOT NULL DEFAULT 0;
ALTER TABLE metrics_samples ADD COLUMN IF NOT EXISTS avg_ttl_role_weighted_confidence NUMERIC(8,3) NOT NULL DEFAULT 0;

-- ============================================================
-- PHASE 1: Robot Runtime State Persistence (the amnesia fix)
-- ============================================================
CREATE TABLE IF NOT EXISTS robot_runtime_state (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  state_type TEXT NOT NULL CHECK (state_type IN ('fabricator', 'excavator', 'sealer', 'insertion_profile')),
  state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (house_id, state_type)
);

CREATE INDEX IF NOT EXISTS idx_robot_runtime_state_lookup ON robot_runtime_state (house_id, state_type);

-- ============================================================
-- PHASE 2: Contracted Telemetry Tables
-- ============================================================

-- Excavation records (excavator -> house brain)
CREATE TABLE IF NOT EXISTS excavation_records (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  robot_id INTEGER REFERENCES robots(id) ON DELETE SET NULL,
  zone_id TEXT NOT NULL,
  depth_m NUMERIC(8,3) NOT NULL,
  blade_load_score NUMERIC(8,4) NOT NULL,
  lift_flow_score NUMERIC(8,4) NOT NULL DEFAULT 0,
  good_stream_rate NUMERIC(8,4) NOT NULL DEFAULT 0,
  reject_stream_rate NUMERIC(8,4) NOT NULL DEFAULT 0,
  jam_state BOOLEAN NOT NULL DEFAULT FALSE,
  blade_wear_index NUMERIC(8,4) NOT NULL DEFAULT 0,
  container_fill_pct NUMERIC(8,3) NOT NULL DEFAULT 0,
  backpressure_state TEXT NOT NULL CHECK (backpressure_state IN ('nominal', 'elevated', 'critical')) DEFAULT 'nominal',
  expected_hard_layer_depth_m NUMERIC(8,3),
  actual_hard_layer_depth_m NUMERIC(8,3),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_excavation_records_house ON excavation_records (house_id, created_at DESC);

-- Fabrication records (fabricator -> house brain)
CREATE TABLE IF NOT EXISTS fabrication_records (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  robot_id INTEGER REFERENCES robots(id) ON DELETE SET NULL,
  part_id TEXT NOT NULL,
  recipe_id TEXT,
  batch_id TEXT NOT NULL,
  fill_volume_used_l NUMERIC(8,3) NOT NULL,
  fill_volume_target_l NUMERIC(8,3) NOT NULL,
  incoming_moisture_pct NUMERIC(8,3) NOT NULL,
  conditioning_applied TEXT NOT NULL CHECK (conditioning_applied IN ('none', 'water_dose', 'dry_pass')) DEFAULT 'none',
  anchor_seat_confidence NUMERIC(8,4) NOT NULL,
  compression_force_kn NUMERIC(8,3) NOT NULL DEFAULT 0,
  press_depth_mm NUMERIC(8,3) NOT NULL DEFAULT 0,
  ejection_dwell_seconds NUMERIC(8,3) NOT NULL,
  calibration_cycle_count INTEGER NOT NULL DEFAULT 0,
  block_type TEXT NOT NULL CHECK (block_type IN ('production', 'calibration')) DEFAULT 'production',
  convergence_limit_hit BOOLEAN NOT NULL DEFAULT FALSE,
  soil_signature TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fabrication_records_house ON fabrication_records (house_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fabrication_records_batch ON fabrication_records (batch_id, created_at DESC);

-- Fabricator feed quality records (fabricator -> excavator via house brain)
CREATE TABLE IF NOT EXISTS fabricator_feed_quality_records (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  batch_id TEXT NOT NULL,
  source_zone_id TEXT NOT NULL,
  calibration_cycles_to_convergence INTEGER NOT NULL DEFAULT 0,
  fill_volume_delta_from_nominal NUMERIC(8,4) NOT NULL DEFAULT 0,
  conditioning_applied TEXT NOT NULL CHECK (conditioning_applied IN ('none', 'water_dose', 'dry_pass')) DEFAULT 'none',
  batch_reject_rate NUMERIC(8,4) NOT NULL DEFAULT 0,
  convergence_limit_hit BOOLEAN NOT NULL DEFAULT FALSE,
  assessment TEXT NOT NULL CHECK (assessment IN ('gate_well_tuned', 'gate_slightly_permissive', 'gate_too_permissive', 'soil_flagged')) DEFAULT 'gate_well_tuned',
  soil_signature TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_feed_quality_records_house ON fabricator_feed_quality_records (house_id, created_at DESC);

-- Placement reports (assembly -> house brain)
CREATE TABLE IF NOT EXISTS placement_reports (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  robot_id INTEGER REFERENCES robots(id) ON DELETE SET NULL,
  cell_id BIGINT REFERENCES grid_cells(id) ON DELETE SET NULL,
  component_type TEXT NOT NULL CHECK (component_type IN ('foundation', 'wall', 'mep', 'roof')),
  insertion_result TEXT NOT NULL CHECK (insertion_result IN ('success', 'failed', 'escalated')),
  retries INTEGER NOT NULL DEFAULT 0,
  resistance_at_seat NUMERIC(8,4) NOT NULL DEFAULT 0,
  calibration_pass BOOLEAN NOT NULL DEFAULT FALSE,
  pre_scan_drift_detected BOOLEAN NOT NULL DEFAULT FALSE,
  post_seat_micro_load_passed BOOLEAN NOT NULL DEFAULT FALSE,
  work_seconds NUMERIC(8,3) NOT NULL DEFAULT 0,
  lane_travel_factor NUMERIC(8,4) NOT NULL DEFAULT 1,
  escalation_reason TEXT,
  neighbor_geometry_delta NUMERIC(8,4),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_placement_reports_house ON placement_reports (house_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_placement_reports_result ON placement_reports (insertion_result, created_at DESC);

-- Neighbor drift telemetry (assembly -> verification via house brain)
CREATE TABLE IF NOT EXISTS neighbor_drift_telemetry (
  id BIGSERIAL PRIMARY KEY,
  house_id INTEGER NOT NULL REFERENCES houses(id) ON DELETE CASCADE,
  robot_id INTEGER REFERENCES robots(id) ON DELETE SET NULL,
  zone_id TEXT NOT NULL,
  drift_detected BOOLEAN NOT NULL DEFAULT FALSE,
  drift_magnitude NUMERIC(8,4) NOT NULL DEFAULT 0,
  affected_cells INTEGER NOT NULL DEFAULT 0,
  escalated_to_house_brain BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_neighbor_drift_house ON neighbor_drift_telemetry (house_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_neighbor_drift_detected ON neighbor_drift_telemetry (drift_detected, created_at DESC);

-- ============================================================
-- Missing indexes for existing tables
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_block_verifications_house_mode_created
  ON block_verifications (house_id, verification_mode, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_site_surveys_house_notes
  ON site_surveys (house_id, surveyed_at DESC)
  WHERE COALESCE(notes, '') NOT LIKE 'scheduler:excavator_ground_truth%';

-- ============================================================
-- Toolhead pairing history (chassis ↔ toolhead swap tracking)
-- ============================================================
CREATE TABLE IF NOT EXISTS toolhead_pairings (
  id BIGSERIAL PRIMARY KEY,
  cluster_id INTEGER NOT NULL REFERENCES robot_clusters(id) ON DELETE CASCADE,
  chassis_id TEXT NOT NULL,
  toolhead_id TEXT NOT NULL,
  toolhead_type TEXT NOT NULL,
  paired_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  unpaired_at TIMESTAMPTZ,
  paired_by TEXT             -- event that triggered the swap (e.g. 'scheduler', 'manual')
);

CREATE INDEX IF NOT EXISTS idx_toolhead_pairings_chassis
  ON toolhead_pairings (chassis_id, paired_at DESC);
CREATE INDEX IF NOT EXISTS idx_toolhead_pairings_toolhead
  ON toolhead_pairings (toolhead_id, paired_at DESC);
CREATE INDEX IF NOT EXISTS idx_toolhead_pairings_active
  ON toolhead_pairings (chassis_id)
  WHERE unpaired_at IS NULL;
