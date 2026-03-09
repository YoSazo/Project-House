-- Seed 3 clusters with 3 robots each
INSERT INTO robot_clusters (name)
SELECT name
FROM (VALUES ('Cluster Alpha'), ('Cluster Beta'), ('Cluster Gamma')) AS v(name)
ON CONFLICT DO NOTHING;

INSERT INTO robots (cluster_id, name)
SELECT c.id, c.name || ' Robot ' || n.seq
FROM robot_clusters c
CROSS JOIN (VALUES (1), (2), (3)) AS n(seq)
WHERE NOT EXISTS (
  SELECT 1 FROM robots r WHERE r.cluster_id = c.id AND r.name = c.name || ' Robot ' || n.seq
);

-- Create 3 houses with staggered stages for initial visualization.
INSERT INTO houses (name, stage, started_at, survey_status, soil_signature, site_zone, sealing_complete, sealing_started_at, sealing_progress, reference_patch_x, reference_patch_y, reference_patch_protected, reference_patch_stamped_at)
SELECT name, stage, NOW() - (offset_mins || ' minutes')::interval, survey_status, soil_signature, site_zone, sealing_complete, sealing_started_at, sealing_progress, reference_patch_x, reference_patch_y, reference_patch_protected, reference_patch_stamped_at
FROM (VALUES
  ('House A', 'site_prep', 20, 'pending', NULL, 'BUILDABLE', FALSE, NULL, 0.0, NULL, NULL, FALSE, NULL),
  ('House B', 'foundation', 80, 'approved', 'clay42_sand33_silt25_org2_sal0.4', 'BUILDABLE', FALSE, NULL, 0.0, 1, 8, TRUE, NOW() - interval '90 minutes'),
  ('House C', 'finishing', 140, 'approved', 'clay35_sand46_silt19_org1_sal0.2', 'BUILDABLE', FALSE, NOW() - interval '5 minutes', 0.0, 8, 1, TRUE, NOW() - interval '150 minutes')
) AS h(name, stage, offset_mins, survey_status, soil_signature, site_zone, sealing_complete, sealing_started_at, sealing_progress, reference_patch_x, reference_patch_y, reference_patch_protected, reference_patch_stamped_at)
WHERE NOT EXISTS (SELECT 1 FROM houses);

-- Generate 10x10x5 grid = 500 cells per house.
WITH house_list AS (
  SELECT id, stage FROM houses
),
grid AS (
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
SELECT h.id, g.x, g.y, g.z, g.component_type,
  g.build_sequence,
  CASE
    WHEN h.stage IN ('site_prep', 'surveying', 'foundation') THEN 'empty'
    WHEN h.stage = 'framing' AND g.component_type = 'foundation' THEN 'filled'
    WHEN h.stage = 'mep' AND g.component_type IN ('foundation', 'wall') THEN 'filled'
    WHEN h.stage = 'finishing' AND g.component_type IN ('foundation', 'wall', 'mep') THEN 'filled'
    WHEN h.stage IN ('sealing', 'community_assembly', 'complete') THEN 'filled'
    ELSE 'empty'
  END,
  CASE
    WHEN h.stage = 'framing' AND g.component_type = 'foundation' THEN NOW()
    WHEN h.stage = 'mep' AND g.component_type IN ('foundation', 'wall') THEN NOW()
    WHEN h.stage = 'finishing' AND g.component_type IN ('foundation', 'wall', 'mep') THEN NOW()
    WHEN h.stage IN ('sealing', 'community_assembly', 'complete') THEN NOW()
    ELSE NULL
  END
FROM house_list h
CROSS JOIN grid g
WHERE NOT EXISTS (
  SELECT 1 FROM grid_cells existing
  WHERE existing.house_id = h.id
);

-- Generate terrain map (10x10) with realistic bumps and obstacles.
WITH house_list AS (
  SELECT id, stage FROM houses
),
terrain AS (
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
  h.id,
  t.x,
  t.y,
  t.target_grade,
  CASE
    WHEN h.stage = 'site_prep' THEN ROUND((
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
    WHEN h.stage = 'site_prep' THEN ROUND((CASE
      WHEN rv.progress < 0.35 THEN 0.14 + rv.compact_noise * 0.18
      WHEN rv.progress < 0.7 THEN 0.38 + rv.compact_noise * 0.24
      ELSE 0.66 + rv.compact_noise * 0.16
    END)::numeric, 3)
    ELSE ROUND((0.92 + rv.compact_noise * 0.08)::numeric, 3)
  END AS compaction_score,
  CASE
    WHEN h.stage = 'site_prep' THEN rv.obstacle_type
    ELSE 'none'
  END AS obstacle_type,
  CASE
    WHEN h.stage = 'site_prep' THEN (rv.obstacle_type = 'none' AND rv.progress > 0.72)
    ELSE TRUE
  END AS obstacle_cleared,
  CASE
    WHEN h.stage = 'site_prep' THEN CASE
      WHEN rv.obstacle_type <> 'none' AND rv.progress < 0.7 THEN 'raw'
      WHEN rv.progress < 0.45 THEN 'raw'
      WHEN rv.progress < 0.78 THEN 'grading'
      ELSE 'compacted'
    END
    ELSE 'ready'
  END AS status,
  NULL,
  NOW()
FROM house_list h
CROSS JOIN terrain t
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
WHERE NOT EXISTS (
  SELECT 1
  FROM terrain_cells existing
  WHERE existing.house_id = h.id
);

-- Seed logistics lane segments and baseline relay state.
WITH lane_source AS (
  SELECT
    h.id,
    h.stage,
    COALESCE(h.reference_patch_x, 1)::numeric AS ref_x,
    COALESCE(h.reference_patch_y, 8)::numeric AS ref_y
  FROM houses h
), segments AS (
  SELECT generate_series(0, 11) AS segment_index
)
INSERT INTO logistics_lane_segments (
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
  last_verified_at,
  updated_at
)
SELECT
  ls.id,
  'lane_main',
  s.segment_index,
  ROUND((ls.ref_x + (s.segment_index::numeric / 12.0) * (4.5 - ls.ref_x))::numeric, 3),
  ROUND((ls.ref_y + (s.segment_index::numeric / 12.0) * (4.5 - ls.ref_y))::numeric, 3),
  ROUND((ls.ref_x + ((s.segment_index + 1)::numeric / 12.0) * (4.5 - ls.ref_x))::numeric, 3),
  ROUND((ls.ref_y + ((s.segment_index + 1)::numeric / 12.0) * (4.5 - ls.ref_y))::numeric, 3),
  ROUND((CASE
    WHEN ls.stage IN ('foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete')
      THEN 0.55 + random() * 0.28
    ELSE 0.2 + random() * 0.22
  END)::numeric, 4),
  ROUND((CASE
    WHEN ls.stage IN ('foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete')
      THEN 0.58 + random() * 0.3
    ELSE 0.25 + random() * 0.25
  END)::numeric, 4),
  ROUND((38 + random() * 16)::numeric, 4),
  ROUND((CASE
    WHEN ls.stage IN ('foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete')
      THEN 0.62 + random() * 0.25
    ELSE 0.3 + random() * 0.25
  END)::numeric, 4),
  ROUND((CASE
    WHEN ls.stage IN ('foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete')
      THEN 0.64 + random() * 0.28
    ELSE 0.34 + random() * 0.3
  END)::numeric, 4),
  CASE
    WHEN ls.stage IN ('foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete')
      THEN FLOOR(18 + random() * 55)::int
    ELSE FLOOR(85 + random() * 150)::int
  END,
  CASE
    WHEN ls.stage IN ('foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete')
      THEN FLOOR(8 + random() * 24)::int
    ELSE FLOOR(random() * 8)::int
  END,
  CASE
    WHEN ls.stage IN ('foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete')
      THEN FLOOR(1 + random() * 6)::int
    ELSE FLOOR(random() * 3)::int
  END,
  CASE
    WHEN ls.stage IN ('foundation', 'framing', 'mep', 'finishing', 'sealing', 'community_assembly', 'complete') THEN 'conditioning'
    ELSE 'raw'
  END,
  s.segment_index = 0,
  FALSE,
  NOW() - ((random() * 8)::text || ' hours')::interval,
  NOW()
FROM lane_source ls
CROSS JOIN segments s
ON CONFLICT (house_id, lane_id, segment_index) DO NOTHING;
-- Seed soil library baselines from approved house signatures.
INSERT INTO soil_library (soil_signature, clay_pct, sand_pct, organic_pct, salinity, recipe, short_term_confidence, long_term_confidence, weathering_cycles_tested, erosion_score, total_blocks_verified)
SELECT
  h.soil_signature,
  (regexp_match(h.soil_signature, 'clay([0-9]+)'))[1]::int,
  (regexp_match(h.soil_signature, 'sand([0-9]+)'))[1]::int,
  (regexp_match(h.soil_signature, 'org([0-9]+)'))[1]::int,
  (regexp_match(h.soil_signature, 'sal([0-9]+(?:\.[0-9]+)?)'))[1]::numeric,
  '{"clay":45,"sand":40,"lime":10,"cement":5}'::jsonb,
  0.78,
  0.71,
  8,
  0.82,
  24
FROM houses h
WHERE h.soil_signature IS NOT NULL
ON CONFLICT (soil_signature) DO NOTHING;

-- Seed site survey probes for houses already approved.
WITH approved AS (
  SELECT id, soil_signature FROM houses WHERE survey_status = 'approved'
)
INSERT INTO site_surveys (
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
)
SELECT
  a.id,
  (g.idx % 5) * 5,
  FLOOR(g.idx / 5)::int * 5,
  ROUND((2 + random() * 1.5)::numeric, 3),
  COALESCE(a.soil_signature, 'clay40_sand35_silt25_org3_sal0.4'),
  CASE
    WHEN random() < 0.08 THEN 'REJECT'
    WHEN random() < 0.28 THEN 'MARGINAL'
    ELSE 'BUILDABLE'
  END,
  ROUND((90 + random() * 130)::numeric, 3),
  ROUND((8 + random() * 25)::numeric, 3),
  ROUND((random() * 9)::numeric, 3),
  ROUND((random() * 2.6)::numeric, 3),
  'seeded',
  NOW() - (g.idx || ' minutes')::interval
FROM approved a
CROSS JOIN generate_series(0, 19) AS g(idx)
WHERE NOT EXISTS (
  SELECT 1 FROM site_surveys s WHERE s.house_id = a.id
);

-- Seed verification records.
INSERT INTO block_verifications (house_id, soil_signature, penetration_resistance, moisture_pct, density, passed, retries, verification_mode, weathering_cycles)
SELECT
  h.id,
  h.soil_signature,
  ROUND((130 + random() * 90)::numeric, 3),
  ROUND((10 + random() * 12)::numeric, 3),
  ROUND((1720 + random() * 220)::numeric, 3),
  TRUE,
  0,
  'inline',
  0
FROM houses h
WHERE h.soil_signature IS NOT NULL
  AND NOT EXISTS (
    SELECT 1 FROM block_verifications b WHERE b.house_id = h.id
  );

-- Seed maintenance baseline for completed sealing houses (none initially, but safe for reruns).
INSERT INTO house_maintenance (house_id, coating_version, coating_applied_at, ttl_expires_at, alert_at, ttl_days, status, next_ttl_multiplier)
SELECT
  h.id,
  1,
  NOW() - interval '10 days',
  NOW() + interval '1800 days',
  NOW() + interval '1620 days',
  1800,
  'ok',
  2.0
FROM houses h
WHERE h.sealing_complete = TRUE
  AND NOT EXISTS (
    SELECT 1 FROM house_maintenance m WHERE m.house_id = h.id
  );

-- Seed community assembly kits for houses already at community_assembly stage.
INSERT INTO assembly_kits (house_id, kit_index, family_label, status, progress, retries, updated_at)
SELECT h.id, gs, CONCAT('Family ', ((h.id + gs) % 9) + 1), 'queued', 0, 0, NOW()
FROM houses h
CROSS JOIN generate_series(1, 3) gs
WHERE h.stage = 'community_assembly'
  AND NOT EXISTS (
    SELECT 1
    FROM assembly_kits ak
    WHERE ak.house_id = h.id
      AND ak.kit_index = gs
  );


