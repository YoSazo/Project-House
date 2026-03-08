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
  sealing_progress NUMERIC(6,3) NOT NULL DEFAULT 0
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
  avg_ttl_days NUMERIC(10,3) NOT NULL DEFAULT 0
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
