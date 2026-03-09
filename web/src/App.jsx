import { useEffect, useMemo, useRef, useState } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import { OrbitControls } from "@react-three/drei";
import * as THREE from "three";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8787";
const HOUSE_TARGET_OPTIONS = [3, 5, 10, 20];
const ROBOT_TARGET_OPTIONS = [9, 18, 36];

const colorMap = {
  empty: "#6b1d1d",
  reserved: "#b8860b",
  filled: "#0a7a43"
};

const robotColorMap = {
  idle: "#7f8ea3",
  waiting_component: "#f4d35e",
  placing: "#3ddc97",
  moving: "#7aa2ff"
};

const roleColorMap = {
  survey: "#56b4ff",
  excavator: "#c28f5c",
  fabricator: "#9f7aea",
  verification: "#f6d365",
  assembly: "#3ddc97",
  sealer: "#66d9ef",
  logistics: "#f4978e"
};

const robotRoleCatalog = [
  {
    id: "survey",
    label: "Survey",
    primaryJobs: "Probe grid, soil sample",
    requiredDof: "3 (XYZ)",
    forceNeeds: "200N penetration",
    dataInterface: "probe(x,y,depth) -> soil_signature",
    commands: ["goto(x,y)", "deploy_feet()", "equalize_load()", "seat_outer_sleeve(depth)", "probe_inner(depth)", "classify_point()", "densify_region(region_id)", "stamp_reference_patch()", "emit_heatmap(site_id)", "abort_probe(reason)"],
    emits: {
      table: "site_surveys",
      payload: {
        house_id: "uuid",
        probe_x: 10,
        probe_y: 5,
        depth_meters: 2.4,
        soil_signature: "clay44_sand33_silt23_org2_sal0.6",
        status: "BUILDABLE",
        surface_tilt_deg: 4.2,
        foot_balance_score: 0.93,
        sleeve_seat_score: 0.88,
        max_probe_depth_m: 2.4,
        confidence: 0.87,
        failure_tier: null,
        reference_patch_coords: { x: 1, y: 8 }
      }
    },
    c2a: "Constraint: expensive CPT rigs. Transmutation: lightweight repeated probes + Bayesian confidence over multiple probe points."
  },
  {
    id: "excavator",
    label: "Excavator",
    primaryJobs: "Grade, compact, dig",
    requiredDof: "4 (XYZ+tilt)",
    forceNeeds: "1kN compaction",
    dataInterface: "grade(area,slope), compact(target)",
    commands: ["grade(area,slope)", "compact(target)", "clearObstacle(type)"],
    emits: {
      table: "terrain_cells",
      payload: {
        house_id: "uuid",
        x: 4,
        y: 3,
        status: "ready",
        current_grade: 0.02,
        compaction_score: 0.91
      }
    },
    c2a: "Constraint: unstructured terrain. Transmutation: turn terrain prep into a measurable grid where each cell reaches grade+compaction thresholds."
  },
  {
    id: "fabricator",
    label: "Fabricator",
    primaryJobs: "Mix soil, form blocks",
    requiredDof: "3 (mixer+extrude)",
    forceNeeds: "500N pressure",
    dataInterface: "produce(signature) -> block_id",
    commands: ["readSoil(signature)", "selectRecipe(signature)", "produce(signature)"],
    emits: {
      table: "soil_library",
      payload: {
        soil_signature: "clay44_sand33_silt23_org2_sal0.6",
        recipe: { clay: 45, sand: 40, lime: 10, cement: 5 },
        short_term_confidence: 0.88,
        total_blocks_verified: 120
      }
    },
    c2a: "Constraint: fixed material recipe fails across soils. Transmutation: inline soil signature routing to adaptive recipe library."
  },
  {
    id: "verification",
    label: "Verification",
    primaryJobs: "QC test, weathering",
    requiredDof: "2 (test+move)",
    forceNeeds: "100N penetrometer",
    dataInterface: "qc(block_id) -> pass/metrics",
    commands: ["qc(block_id)", "runWeathering(signature)", "updateConfidence(signature)"],
    emits: {
      table: "block_verifications",
      payload: {
        house_id: "uuid",
        soil_signature: "clay44_sand33_silt23_org2_sal0.6",
        penetration_resistance: 162.4,
        moisture_pct: 13.7,
        density: 1810,
        passed: true,
        verification_mode: "inline"
      }
    },
    c2a: "Constraint: slow destructive testing. Transmutation: risk-based QC where known high-confidence soils skip full verification."
  },
  {
    id: "assembly",
    label: "Assembly",
    primaryJobs: "Place blocks, impedance",
    requiredDof: "4 (XYZ+rotate)",
    forceNeeds: "50N Z-force",
    dataInterface: "place(block_id,x,y,z) -> success",
    commands: ["claimCell(house_id)", "moveTo(x,y,z)", "place(block_id,x,y,z)"],
    emits: {
      table: "grid_cells",
      payload: {
        house_id: "uuid",
        x: 6,
        y: 2,
        z: 1,
        status: "filled",
        component_type: "wall",
        filled_at: "timestamp"
      }
    },
    c2a: "Constraint: no tactile sensors (numb fingers). Transmutation: use motor impedance deviation on Z trajectory as force proxy for insertion correction."
  },
  {
    id: "sealer",
    label: "Sealer",
    primaryJobs: "Coat exterior",
    requiredDof: "3 (XYZ spray)",
    forceNeeds: "10N pressure",
    dataInterface: "seal(surface) -> done",
    commands: ["spray(surface)", "verifyCoverage(house_id)", "completeSealing(house_id)"],
    emits: {
      table: "house_maintenance",
      payload: {
        house_id: "uuid",
        coating_version: 2,
        ttl_days: 3650,
        ttl_expires_at: "timestamp",
        status: "ok"
      }
    },
    c2a: "Constraint: seam-by-seam sealing bottleneck. Transmutation: blanket-seal all exterior surfaces in parallel with all robots."
  },
    {
    id: "logistics",
    label: "Logistics",
    primaryJobs: "Material cart + lane conditioning",
    requiredDof: "3 (XYZ cart)",
    forceNeeds: "500kg carry + light drag",
    dataInterface: "route(payload, lane) -> delivered",
    commands: [
      "goto(x,y)",
      "pickup(payload_id)",
      "dock(station_id)",
      "lower_body_gauge()",
      "grade_until_match(segment_id)",
      "relay_reference_state()",
      "emit_status()",
      "yield(robot_id)",
      "return_to_base()"
    ],
    emits: {
      table: "logistics_lane_segments",
      payload: {
        house_id: "uuid",
        lane_id: "lane_main",
        segment_index: 4,
        condition_score: 0.89,
        grade_match_score: 0.91,
        avg_drag_force: 0.42,
        body_gauge_match: 0.88,
        relay_consensus: 0.93,
        reference_relay_age_s: 14,
        verification_events: 7,
        status: "stable",
        restamp_required: false
      }
    },
    c2a: "Constraint: bad terrain and lane drift break delivery. Transmutation: body-as-gauge verification + relay truth + gradual lane conditioning per pass."
  }
];
const terrainColorMap = {
  raw: "#5e3e2d",
  grading: "#8b5a36",
  compacted: "#8a7d4a",
  ready: "#3a6f54"
};

const stageOrder = ["site_prep", "surveying", "foundation", "framing", "mep", "finishing", "sealing", "community_assembly", "complete"];

const stageLabels = {
  site_prep: "Site Prep",
  surveying: "Surveying",
  foundation: "Foundation",
  framing: "Framing",
  mep: "MEP",
  finishing: "Finishing",
  sealing: "Sealing",
  community_assembly: "Community Assembly",
  complete: "Complete"
};

const TERRAIN_HEIGHT_SCALE = 0.8;
const TERRAIN_BASE_Y = -0.55;

const obstacleColorMap = {
  root: "#9a6d3a",
  rock: "#6e7580",
  debris: "#8a5a44"
};

function GridScene({ cells }) {
  return (
    <Canvas camera={{ position: [16, 14, 20], fov: 55 }}>
      <ambientLight intensity={0.6} />
      <directionalLight position={[10, 20, 10]} intensity={0.8} />
      <group position={[-4.5, 0, -4.5]}>
        {cells.map((cell) => (
          <mesh key={cell.id} position={[cell.x, cell.z, cell.y]}>
            <boxGeometry args={[0.92, 0.92, 0.92]} />
            <meshStandardMaterial color={colorMap[cell.status] || "#444"} opacity={cell.status === "empty" ? 0.35 : 1} transparent />
          </mesh>
        ))}
      </group>
      <gridHelper args={[16, 16, "#666", "#333"]} />
      <OrbitControls makeDefault />
    </Canvas>
  );
}

function InstancedCells({ positions, color, opacity, size = [0.9, 0.9, 0.9] }) {
  const ref = useRef(null);

  useEffect(() => {
    if (!ref.current) return;
    const temp = new THREE.Object3D();

    positions.forEach((pos, index) => {
      temp.position.set(pos[0], pos[1], pos[2]);
      temp.updateMatrix();
      ref.current.setMatrixAt(index, temp.matrix);
    });

    ref.current.instanceMatrix.needsUpdate = true;
  }, [positions]);

  if (!positions.length) return null;

  return (
    <instancedMesh ref={ref} args={[null, null, positions.length]}>
      <boxGeometry args={size} />
      <meshStandardMaterial color={color} opacity={opacity} transparent />
    </instancedMesh>
  );
}

function AnimatedCarrier({ robot, start, end, colorOverride }) {
  const groupRef = useRef(null);
  const cargoRef = useRef(null);

  useFrame((state) => {
    if (!groupRef.current) return;

    const t = state.clock.getElapsedTime() * 0.42 + robot.id * 0.173;
    const shuttling = robot.status === "placing" || robot.status === "moving" || robot.status === "waiting_component";
    const waiting = robot.status === "waiting_component";

    let progress = 1;
    if (shuttling) {
      const loop = (t / (waiting ? 1.45 : 1)) % 1;
      const shuttle = loop < 0.5 ? loop * 2 : (1 - loop) * 2;
      progress = waiting ? (0.24 + shuttle * 0.58) : shuttle;
    } else if (robot.status === "idle") {
      progress = 0.12 + 0.08 * (0.5 + 0.5 * Math.sin(t * 1.9));
    }

    const clamped = Math.max(0, Math.min(1, progress));
    const x = start[0] + (end[0] - start[0]) * clamped;
    const z = start[2] + (end[2] - start[2]) * clamped;
    const lateral = Math.sin(t * 3.1) * (shuttling ? 0.0 : 0.18);
    const arc = shuttling ? Math.sin(Math.PI * clamped) * 1.2 : 0.3 + 0.08 * Math.sin(t * 5.2);
    const y = start[1] + (end[1] - start[1]) * clamped + arc;

    groupRef.current.position.set(x + lateral, y, z + (waiting ? lateral * 0.6 : 0));

    if (cargoRef.current) {
      cargoRef.current.visible = shuttling || waiting;
      cargoRef.current.rotation.y += waiting ? 0.035 : 0.02;
    }
  });

  return (
    <group ref={groupRef}>
      <mesh>
        <sphereGeometry args={[0.35, 14, 14]} />
        <meshStandardMaterial
          color={colorOverride || robotColorMap[robot.status] || "#ffffff"}
          emissive={colorOverride || robotColorMap[robot.status] || "#ffffff"}
          emissiveIntensity={0.35}
        />
      </mesh>
      <mesh ref={cargoRef} position={[0, 0.6, 0]}>
        <boxGeometry args={[0.45, 0.45, 0.45]} />
        <meshStandardMaterial color="#9fc6ff" emissive="#3d7fff" emissiveIntensity={0.2} />
      </mesh>
    </group>
  );
}

function SuburbScene({ houses, cells, terrain, robots, selectedHouse, roleByRobotId, showRoleOverlay }) {
  const layout = useMemo(() => {
    const ordered = [...houses].sort((a, b) => a.id - b.id);
    const cols = Math.max(1, Math.ceil(Math.sqrt(ordered.length || 1)));
    const spacing = 14;
    const houseMap = new Map();

    ordered.forEach((house, idx) => {
      const col = idx % cols;
      const row = Math.floor(idx / cols);
      houseMap.set(house.id, { x: col * spacing, z: row * spacing });
    });

    return {
      houseMap,
      cols,
      rows: Math.max(1, Math.ceil(ordered.length / cols)),
      spacing
    };
  }, [houses]);

  const houseStageById = useMemo(() => {
    return new Map(houses.map((house) => [house.id, house.stage]));
  }, [houses]);

  const groupedCells = useMemo(() => {
    const groups = {
      reserved: [],
      filled: []
    };

    cells.forEach((cell) => {
      const offset = layout.houseMap.get(cell.house_id);
      if (!offset) return;

      const position = [offset.x + cell.x, cell.z, offset.z + cell.y];
      if (cell.status === "reserved") groups.reserved.push(position);
      else if (cell.status === "filled") groups.filled.push(position);
    });

    return groups;
  }, [cells, layout.houseMap]);

  const groupedTerrain = useMemo(() => {
    const groups = {
      raw: [],
      grading: [],
      compacted: [],
      ready: [],
      root: [],
      rock: [],
      debris: []
    };

    terrain.forEach((cell) => {
      const offset = layout.houseMap.get(cell.house_id);
      if (!offset) return;

      const key = Object.hasOwn(groups, cell.status) ? cell.status : "raw";
      const y = TERRAIN_BASE_Y + Number(cell.current_grade || 0) * TERRAIN_HEIGHT_SCALE;
      const x = offset.x + cell.x;
      const z = offset.z + cell.y;

      groups[key].push([x, y, z]);

      const obstacleType = cell.obstacle_type;
      const obstacleCleared = Boolean(cell.obstacle_cleared);
      if (!obstacleCleared && Object.hasOwn(obstacleColorMap, obstacleType)) {
        groups[obstacleType].push([x, y + 0.2, z]);
      }
    });

    return groups;
  }, [terrain, layout.houseMap]);

  const terrainHeightByCell = useMemo(() => {
    const map = new Map();
    terrain.forEach((cell) => {
      map.set(`${cell.house_id}:${cell.x}:${cell.y}`, Number(cell.current_grade || 0));
    });
    return map;
  }, [terrain]);

  const worldWidth = Math.max(layout.cols * layout.spacing, layout.spacing);
  const worldDepth = Math.max(layout.rows * layout.spacing, layout.spacing);
  const bayCenter = [worldWidth / 2 - 2.5, 0.55, -7];

  const robotMarkers = useMemo(() => {
    return robots
      .filter((robot) => robot.active_house_id && layout.houseMap.has(robot.active_house_id))
      .map((robot) => {
        const offset = layout.houseMap.get(robot.active_house_id);
        const laneOffset = ((robot.id % 7) - 3) * 0.65;
        const stage = houseStageById.get(robot.active_house_id);
        const terrainGrade = terrainHeightByCell.get(`${robot.active_house_id}:${robot.pos_x}:${robot.pos_y}`) ?? 0;
        const terrainTopY = TERRAIN_BASE_Y + terrainGrade * TERRAIN_HEIGHT_SCALE + 0.1;
        const blockY = robot.pos_z + 0.9;

        return {
          id: robot.id,
          status: robot.status,
          role: roleByRobotId?.[robot.id] || "logistics",
          start: [bayCenter[0] + laneOffset, bayCenter[1], bayCenter[2]],
          end: [offset.x + robot.pos_x, stage === "site_prep" ? terrainTopY + 0.2 : blockY, offset.z + robot.pos_y]
        };
      });
  }, [robots, layout.houseMap, bayCenter, houseStageById, terrainHeightByCell]);

  const selectedOffset = selectedHouse ? layout.houseMap.get(selectedHouse) : null;
  const target = [worldWidth / 2 - 2.5, 0, worldDepth / 2 - 2.5];

  return (
    <Canvas camera={{ position: [worldWidth * 0.7, Math.max(20, worldWidth * 0.5), worldDepth * 1.05], fov: 55 }}>
      <ambientLight intensity={0.5} />
      <directionalLight position={[40, 50, 20]} intensity={0.9} />

      <group position={[bayCenter[0], 0, bayCenter[2]]}>
        <mesh position={[0, -0.4, 0]}>
          <boxGeometry args={[12.5, 0.35, 4.4]} />
          <meshStandardMaterial color="#2e4158" />
        </mesh>
        <mesh position={[0, 0.35, 0]}>
          <boxGeometry args={[11.8, 0.18, 3.4]} />
          <meshStandardMaterial color="#1a6e9c" emissive="#0b3f5a" emissiveIntensity={0.2} />
        </mesh>
        {Array.from({ length: 8 }).map((_, i) => (
          <mesh key={i} position={[-4 + (i % 4) * 2.7, 0.25 + Math.floor(i / 4) * 0.52, -0.6 + (i % 2) * 1.2]}>
            <boxGeometry args={[0.9, 0.45, 0.9]} />
            <meshStandardMaterial color="#9fc6ff" />
          </mesh>
        ))}
      </group>

      {Array.from(layout.houseMap.entries()).map(([houseId, offset]) => (
        <mesh key={houseId} position={[offset.x + 4.5, -0.72, offset.z + 4.5]}>
          <boxGeometry args={[10.2, 0.05, 10.2]} />
          <meshStandardMaterial color={selectedHouse === houseId ? "#1d3b57" : "#132234"} />
        </mesh>
      ))}

      <InstancedCells positions={groupedTerrain.raw} color={terrainColorMap.raw} opacity={0.92} size={[0.95, 0.16, 0.95]} />
      <InstancedCells positions={groupedTerrain.grading} color={terrainColorMap.grading} opacity={0.92} size={[0.95, 0.16, 0.95]} />
      <InstancedCells positions={groupedTerrain.compacted} color={terrainColorMap.compacted} opacity={0.92} size={[0.95, 0.16, 0.95]} />
      <InstancedCells positions={groupedTerrain.ready} color={terrainColorMap.ready} opacity={0.96} size={[0.95, 0.16, 0.95]} />

      <InstancedCells positions={groupedTerrain.root} color={obstacleColorMap.root} opacity={0.95} size={[0.24, 0.4, 0.24]} />
      <InstancedCells positions={groupedTerrain.rock} color={obstacleColorMap.rock} opacity={0.95} size={[0.42, 0.28, 0.42]} />
      <InstancedCells positions={groupedTerrain.debris} color={obstacleColorMap.debris} opacity={0.95} size={[0.3, 0.2, 0.3]} />

      <InstancedCells positions={groupedCells.reserved} color={colorMap.reserved} opacity={0.95} />
      <InstancedCells positions={groupedCells.filled} color={colorMap.filled} opacity={1} />

      {robotMarkers.map((robot) => (
        <mesh key={`target-${robot.id}`} position={robot.end}>
          <sphereGeometry args={[0.14, 10, 10]} />
          <meshStandardMaterial color="#9ec5ff" opacity={0.35} transparent />
        </mesh>
      ))}

      {robotMarkers.map((robot) => (
        <AnimatedCarrier
          key={robot.id}
          robot={robot}
          start={robot.start}
          end={robot.end}
          colorOverride={showRoleOverlay ? roleColorMap[robot.role] : null}
        />
      ))}

      {selectedOffset ? (
        <mesh position={[selectedOffset.x + 4.5, 2.6, selectedOffset.z + 4.5]}>
          <boxGeometry args={[10.8, 5.6, 10.8]} />
          <meshBasicMaterial color="#f4d35e" wireframe transparent opacity={0.8} />
        </mesh>
      ) : null}

      <gridHelper args={[Math.max(worldWidth, worldDepth) + 12, Math.max(layout.cols, layout.rows) * 12, "#4e6075", "#243245"]} position={target} />
      <OrbitControls makeDefault target={target} />
    </Canvas>
  );
}

function CurveChart({ points }) {
  const width = 420;
  const height = 170;
  const pad = 28;

  if (!points.length) {
    return <div className="chart-empty">No curve points yet. Run an experiment reset and test load points.</div>;
  }

  const xs = points.map((p) => Number(p.active_houses));
  const ys = points.map((p) => Number(p.avg_efficiency));
  const xMin = Math.min(...xs);
  const xMax = Math.max(...xs);
  const yMin = 0;
  const yMax = 100;

  const scaleX = (v) => (xMax === xMin ? width / 2 : pad + ((v - xMin) / (xMax - xMin)) * (width - pad * 2));
  const scaleY = (v) => height - pad - ((v - yMin) / (yMax - yMin)) * (height - pad * 2);

  const polyline = points
    .map((p) => `${scaleX(Number(p.active_houses))},${scaleY(Number(p.avg_efficiency))}`)
    .join(" ");

  return (
    <svg viewBox={`0 0 ${width} ${height}`} className="chart-svg" role="img" aria-label="Efficiency vs houses">
      <line x1={pad} y1={height - pad} x2={width - pad} y2={height - pad} stroke="#4d627c" />
      <line x1={pad} y1={pad} x2={pad} y2={height - pad} stroke="#4d627c" />
      <polyline fill="none" stroke="#45d483" strokeWidth="3" points={polyline} />
      {points.map((p) => {
        const x = scaleX(Number(p.active_houses));
        const y = scaleY(Number(p.avg_efficiency));
        return (
          <g key={`${p.active_houses}-${p.samples}`}>
            <circle cx={x} cy={y} r="4" fill="#ffd166" />
            <text x={x} y={height - 8} textAnchor="middle" fontSize="10" fill="#c7d7ea">{p.active_houses}</text>
            <text x={x + 6} y={y - 6} fontSize="10" fill="#c7d7ea">{Number(p.avg_efficiency).toFixed(0)}%</text>
          </g>
        );
      })}
      <text x={width / 2} y={height - 2} textAnchor="middle" fontSize="10" fill="#8da8c8">Active Houses</text>
      <text x={10} y={12} fontSize="10" fill="#8da8c8">Efficiency %</text>
    </svg>
  );
}

function SiteHeatmap({ probes }) {
  const ordered = [...probes]
    .sort((a, b) => Number(a.probe_y) - Number(b.probe_y) || Number(a.probe_x) - Number(b.probe_x))
    .slice(0, 20);

  const colorForStatus = (status) => {
    if (status === "BUILDABLE") return "#2e8b57";
    if (status === "MARGINAL") return "#c49a3a";
    return "#8f2c2c";
  };

  if (!ordered.length) {
    return <div className="chart-empty">No survey probes yet for this house.</div>;
  }

  return (
    <div className="heatmap-grid">
      {ordered.map((probe, idx) => (
        <div key={`${probe.id ?? idx}-${probe.probe_x}-${probe.probe_y}`} className="heatmap-cell" style={{ background: colorForStatus(probe.status), outline: probe.densified ? "2px solid #8fd3ff" : "none" }} title={`${probe.status} | conf=${Number(probe.confidence ?? 0).toFixed(2)} | tilt=${Number(probe.surface_tilt_deg ?? 0).toFixed(1)} | foot=${Number(probe.foot_balance_score ?? 0).toFixed(2)} | sleeve=${Number(probe.sleeve_seat_score ?? 0).toFixed(2)}`}>
          <span>{probe.status?.charAt(0) ?? "-"}</span>
          <small>{Number(probe.confidence ?? 0).toFixed(2)}</small>
        </div>
      ))}
    </div>
  );
}

function MaintenanceTimeline({ rows }) {
  if (!rows?.length) {
    return <div className="chart-empty">No maintenance records yet.</div>;
  }

  const now = Date.now();

  return (
    <div className="maintenance-list">
      {rows.map((item) => {
        const expires = item.ttl_expires_at ? new Date(item.ttl_expires_at).getTime() : null;
        const alert = item.alert_at ? new Date(item.alert_at).getTime() : null;
        const applied = item.coating_applied_at ? new Date(item.coating_applied_at).getTime() : null;

        const totalWindow = expires && applied ? Math.max(1, expires - applied) : 1;
        const elapsed = expires && applied ? Math.max(0, Math.min(totalWindow, now - applied)) : 0;
        const progressPct = Math.max(0, Math.min(100, 100 - (elapsed / totalWindow) * 100));

        const daysRemaining = expires ? Math.max(0, Math.round((expires - now) / 86400000)) : null;
        const alertIn = alert ? Math.round((alert - now) / 86400000) : null;

        return (
          <div key={`maint-${item.house_id}`} className="maintenance-row">
            <div className="maintenance-head">
              <strong>{item.house_name ?? `House ${item.house_id}`}</strong>
              <small>Coating v{item.coating_version}</small>
            </div>
            <div className="maintenance-bar">
              <div className="maintenance-fill" style={{ width: `${progressPct}%` }} />
            </div>
            <small>
              TTL: {daysRemaining ?? "-"} days remaining
              {alertIn !== null ? ` | Alert in ${alertIn} days` : ""}
              {item.status === "alert" ? " | ALERT" : ""}
            </small>
          </div>
        );
      })}
    </div>
  );
}
function RobotDesignModel({ role }) {
  const color = roleColorMap[role] || "#8aa4c8";

  if (role === "excavator") {
    return (
      <group>
        <mesh position={[0, 0.15, 0]}>
          <boxGeometry args={[1.4, 0.35, 0.85]} />
          <meshStandardMaterial color={color} />
        </mesh>
        <mesh position={[0.65, 0.32, 0]} rotation={[0, 0, -0.45]}>
          <boxGeometry args={[0.8, 0.12, 0.12]} />
          <meshStandardMaterial color="#d9e4f2" />
        </mesh>
      </group>
    );
  }

  if (role === "fabricator") {
    return (
      <group>
        <mesh>
          <cylinderGeometry args={[0.45, 0.45, 0.9, 16]} />
          <meshStandardMaterial color={color} />
        </mesh>
        <mesh position={[0.55, 0.1, 0]}>
          <boxGeometry args={[0.38, 0.24, 0.38]} />
          <meshStandardMaterial color="#d9e4f2" />
        </mesh>
      </group>
    );
  }

  if (role === "verification") {
    return (
      <group>
        <mesh>
          <sphereGeometry args={[0.42, 20, 20]} />
          <meshStandardMaterial color={color} />
        </mesh>
        <mesh position={[0, -0.44, 0]}>
          <cylinderGeometry args={[0.08, 0.08, 0.5, 12]} />
          <meshStandardMaterial color="#f7f7f7" />
        </mesh>
      </group>
    );
  }

  if (role === "assembly") {
    return (
      <group>
        <mesh>
          <boxGeometry args={[0.9, 0.34, 0.9]} />
          <meshStandardMaterial color={color} />
        </mesh>
        <mesh position={[0, 0.38, 0]}>
          <boxGeometry args={[0.18, 0.65, 0.18]} />
          <meshStandardMaterial color="#e4eef9" />
        </mesh>
      </group>
    );
  }

  if (role === "sealer") {
    return (
      <group>
        <mesh>
          <boxGeometry args={[1.0, 0.28, 0.7]} />
          <meshStandardMaterial color={color} />
        </mesh>
        <mesh position={[0.52, 0.22, 0]} rotation={[0, 0, -0.25]}>
          <boxGeometry args={[0.62, 0.07, 0.07]} />
          <meshStandardMaterial color="#d7ecff" emissive="#4fb7ff" emissiveIntensity={0.25} />
        </mesh>
      </group>
    );
  }

  if (role === "logistics") {
    return (
      <group>
        <mesh>
          <boxGeometry args={[1.1, 0.22, 0.85]} />
          <meshStandardMaterial color={color} />
        </mesh>
        <mesh position={[0, 0.27, 0]}>
          <boxGeometry args={[0.75, 0.25, 0.5]} />
          <meshStandardMaterial color="#e4eef9" />
        </mesh>
      </group>
    );
  }

  return (
    <group>
      <mesh>
        <boxGeometry args={[0.8, 0.3, 0.8]} />
        <meshStandardMaterial color={color} />
      </mesh>
      <mesh position={[0, 0.36, 0]}>
        <cylinderGeometry args={[0.2, 0.2, 0.45, 12]} />
        <meshStandardMaterial color="#d9e4f2" />
      </mesh>
    </group>
  );
}

function RobotDesignScene({ role }) {
  return (
    <div className="robot-design-wrap">
      <Canvas camera={{ position: [2.2, 1.9, 2.3], fov: 46 }}>
        <ambientLight intensity={0.6} />
        <directionalLight position={[4, 5, 3]} intensity={1.1} />
        <mesh rotation={[-Math.PI / 2, 0, 0]} position={[0, -0.45, 0]}>
          <circleGeometry args={[1.5, 48]} />
          <meshStandardMaterial color="#102235" />
        </mesh>
        <RobotDesignModel role={role} />
        <OrbitControls makeDefault autoRotate autoRotateSpeed={0.7} enableZoom={false} />
      </Canvas>
    </div>
  );
}

function RobotSpecTab({
  robots,
  houses,
  roleRuntime,
  selectedRoleId,
  setSelectedRoleId,
  roleTargets,
  setRoleTargets,
  onScaleRobots,
  isMutating,
  showRoleOverlay,
  setShowRoleOverlay,
  metrics
}) {
  const selectedRole = robotRoleCatalog.find((r) => r.id === selectedRoleId) || robotRoleCatalog[0];
  const [c2aOutput, setC2aOutput] = useState(selectedRole.c2a);

  useEffect(() => {
    setC2aOutput(selectedRole.c2a);
  }, [selectedRole]);

  const totalTarget = Object.values(roleTargets).reduce((sum, value) => sum + Number(value || 0), 0);

  return (
    <>
      <section className="panel">
        <h2>Robot Roles</h2>
        <table>
          <thead>
            <tr><th>Role</th><th>Primary Jobs</th><th>Required DOF</th><th>Force/Speed Needs</th><th>Data Interface</th><th>Active</th></tr>
          </thead>
          <tbody>
            {robotRoleCatalog.map((role) => (
              <tr key={role.id}>
                <td>{role.label}</td>
                <td>{role.primaryJobs}</td>
                <td>{role.requiredDof}</td>
                <td>{role.forceNeeds}</td>
                <td><code>{role.dataInterface}</code></td>
                <td>{roleRuntime[role.id]?.count ?? 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel">
        <h2>Simulation Integration</h2>
        <div className="robot-role-grid">
          {robotRoleCatalog.map((role) => (
            <button
              key={role.id}
              className={`role-chip ${selectedRole.id === role.id ? "active" : ""}`}
              onClick={() => setSelectedRoleId(role.id)}
            >
              <span className="dot" style={{ background: roleColorMap[role.id] }} />
              <strong>{role.label}</strong>
              <small>{roleRuntime[role.id]?.count ?? 0} active</small>
            </button>
          ))}
        </div>

        <div className="role-scaler">
          {robotRoleCatalog.map((role) => (
            <label key={`target-${role.id}`}>
              <span>{role.label}</span>
              <input
                type="number"
                min="0"
                max="120"
                value={Number(roleTargets[role.id] ?? 0)}
                onChange={(e) => setRoleTargets((prev) => ({ ...prev, [role.id]: Number(e.target.value || 0) }))}
              />
            </label>
          ))}
        </div>

        <div className="button-row">
          <button className="pill action" onClick={() => onScaleRobots(totalTarget)} disabled={isMutating}>Scale Robots ({totalTarget})</button>
          <button className={`pill ${showRoleOverlay ? "active" : ""}`} onClick={() => setShowRoleOverlay((v) => !v)}>
            {showRoleOverlay ? "Role Colors On" : "Role Colors Off"}
          </button>
        </div>
        <p className="muted-note">Role scaling rolls up to total robot count in the current sim. Runtime assignment is stage-driven.</p>
      </section>

      <section className="split robot-spec-split">
        <div className="panel">
          <h2>{selectedRole.label} Spec</h2>
          <div className="spec-grid">
            <article><span>Primary Jobs</span><strong>{selectedRole.primaryJobs}</strong></article>
            <article><span>Required DOF</span><strong>{selectedRole.requiredDof}</strong></article>
            <article><span>Force/Speed Needs</span><strong>{selectedRole.forceNeeds}</strong></article>
            <article><span>Data Interface</span><strong><code>{selectedRole.dataInterface}</code></strong></article>
            <article><span>Runtime Active</span><strong>{roleRuntime[selectedRole.id]?.count ?? 0}</strong></article>
            <article><span>Utilization</span><strong>{Number(roleRuntime[selectedRole.id]?.utilization ?? 0).toFixed(1)}%</strong></article>
          </div>

          <h3>Commands</h3>
          <ul className="queue">
            {selectedRole.commands.map((cmd) => (<li key={cmd}><code>{cmd}</code></li>))}
          </ul>

          <div className="button-row">
            <button className="pill" onClick={() => setC2aOutput(selectedRole.c2a)}>Run C2A on {selectedRole.label}</button>
          </div>
          <div className="causal-chain">
            <strong>C2A Output</strong>
            <small>{c2aOutput}</small>
          </div>

          <details className="contract-box" open>
            <summary>Interface Contract ({selectedRole.emits.table})</summary>
            <pre>{JSON.stringify(selectedRole.emits.payload, null, 2)}</pre>
          </details>
        </div>

        <div className="panel">
          <h2>{selectedRole.label} Design</h2>
          <RobotDesignScene role={selectedRole.id} />
          <p className="muted-note">Left side is the build contract. Right side is a stylized robot concept for the hardware cofounder.</p>
        </div>
      </section>

      <section className="panel">
        <h2>Per-Role Runtime Metrics</h2>
        <table>
          <thead>
            <tr><th>Role</th><th>Active</th><th>Idle</th><th>Moving</th><th>Placing</th><th>Waiting</th><th>Utilization</th></tr>
          </thead>
          <tbody>
            {robotRoleCatalog.map((role) => {
              const row = roleRuntime[role.id] || { count: 0, idle: 0, moving: 0, placing: 0, waiting_component: 0, utilization: 0 };
              return (
                <tr key={`metric-${role.id}`}>
                  <td>{role.label}</td>
                  <td>{row.count}</td>
                  <td>{row.idle}</td>
                  <td>{row.moving}</td>
                  <td>{row.placing}</td>
                  <td>{row.waiting_component}</td>
                  <td>{Number(row.utilization).toFixed(1)}%</td>
                </tr>
              );
            })}
          </tbody>
        </table>
        <small className="muted-note">Live context: {houses.length} houses, {robots.length} robots, throughput {Number(metrics?.throughput_cells_per_hour ?? 0).toFixed(1)} cells/h.</small>
      </section>
    </>
  );
}

export default function App() {
  const [state, setState] = useState(null);
  const [selectedHouse, setSelectedHouse] = useState(null);
  const [cells, setCells] = useState([]);
  const [suburbCells, setSuburbCells] = useState([]);
  const [suburbTerrain, setSuburbTerrain] = useState([]);
  const [error, setError] = useState("");
  const [targetHouses, setTargetHouses] = useState(3);
  const [targetRobots, setTargetRobots] = useState(9);
  const [activeTab, setActiveTab] = useState("mission");
  const [selectedRoleId, setSelectedRoleId] = useState("assembly");
  const [showRoleOverlay, setShowRoleOverlay] = useState(true);
  const [roleTargets, setRoleTargets] = useState(() => robotRoleCatalog.reduce((acc, role) => {
    acc[role.id] = 0;
    return acc;
  }, {}));
  const roleTargetsSeededRef = useRef(false);
  const [curve, setCurve] = useState([]);
  const [matrix, setMatrix] = useState([]);
  const [soilLibrary, setSoilLibrary] = useState([]);
  const [siteProbes, setSiteProbes] = useState([]);
  const [maintenanceAlerts, setMaintenanceAlerts] = useState([]);
  const [isMutating, setIsMutating] = useState(false);

  useEffect(() => {
    let isMounted = true;
    let wsConnected = false;

    const fetchState = async () => {
      try {
        const [stateResp, curveResp, matrixResp, soilResp, maintenanceResp] = await Promise.all([
          fetch(`${API_BASE}/api/state`),
          fetch(`${API_BASE}/api/metrics/curve`),
          fetch(`${API_BASE}/api/metrics/matrix`),
          fetch(`${API_BASE}/api/soil-library`),
          fetch(`${API_BASE}/api/maintenance-alerts`)
        ]);
        const data = await stateResp.json();
        const curveData = await curveResp.json();
        const matrixData = await matrixResp.json();
        const soilData = await soilResp.json();
        const maintenanceData = await maintenanceResp.json();

        if (!isMounted) return;
        setState(data);
        setCurve(curveData);
        setMatrix(matrixData);
        setSoilLibrary(Array.isArray(soilData) ? soilData : []);
        setMaintenanceAlerts(Array.isArray(maintenanceData) ? maintenanceData : []);
        setTargetHouses(data.houses?.length || 0);
        setTargetRobots(data.robots?.length || 0);

        setSelectedHouse((prev) => {
          const hasPrev = data.houses?.some((h) => h.id === prev);
          return prev && hasPrev ? prev : (data.houses?.[0]?.id ?? null);
        });

        if (!wsConnected) {
          setError("");
        }
      } catch (e) {
        if (!wsConnected && isMounted) {
          setError(e.message);
        }
      }
    };

    fetchState();
    const pollId = setInterval(fetchState, 5000);

    const ws = new WebSocket(API_BASE.replace(/^http/, "ws") + "/ws");
    ws.onopen = () => {
      wsConnected = true;
      if (isMounted) setError("");
    };
    ws.onmessage = (event) => {
      const payload = JSON.parse(event.data);
      if (payload.type === "state") setState(payload.data);
      if (payload.type === "error") setError(payload.data.message);
    };
    ws.onerror = () => {
      wsConnected = false;
      setError("WebSocket disconnected (polling fallback active)");
    };
    ws.onclose = () => {
      wsConnected = false;
      setError("WebSocket disconnected (polling fallback active)");
    };

    return () => {
      isMounted = false;
      clearInterval(pollId);
      ws.close();
    };
  }, []);

  useEffect(() => {
    if (!selectedHouse) return;

    Promise.all([
      fetch(`${API_BASE}/api/houses/${selectedHouse}/grid`),
      fetch(`${API_BASE}/api/site-heatmap/${selectedHouse}`)
    ])
      .then(async ([gridResp, heatmapResp]) => {
        const [gridData, heatmapData] = await Promise.all([gridResp.json(), heatmapResp.json()]);
        setCells(Array.isArray(gridData) ? gridData : []);
        setSiteProbes(Array.isArray(heatmapData) ? heatmapData : []);
      })
      .catch((e) => setError(e.message));
  }, [selectedHouse, state?.metrics?.sampled_at]);

  useEffect(() => {
    const activeHouseCount = state?.houses?.length ?? 0;
    if (!activeHouseCount) {
      setSuburbCells([]);
      setSuburbTerrain([]);
      return;
    }

    Promise.all([
      fetch(`${API_BASE}/api/suburb/grid?max_houses=${Math.max(activeHouseCount, 1)}`),
      fetch(`${API_BASE}/api/suburb/terrain?max_houses=${Math.max(activeHouseCount, 1)}`)
    ])
      .then(async ([gridResp, terrainResp]) => {
        const [gridData, terrainData] = await Promise.all([gridResp.json(), terrainResp.json()]);
        setSuburbCells(gridData);
        setSuburbTerrain(terrainData);
      })
      .catch((e) => setError(e.message));
  }, [state?.houses?.length, state?.metrics?.sampled_at]);

  const refreshCharts = async () => {
    const [curveResp, matrixResp, soilResp, maintenanceResp] = await Promise.all([
      fetch(`${API_BASE}/api/metrics/curve`),
      fetch(`${API_BASE}/api/metrics/matrix`),
      fetch(`${API_BASE}/api/soil-library`),
      fetch(`${API_BASE}/api/maintenance-alerts`)
    ]);
    setCurve(await curveResp.json());
    setMatrix(await matrixResp.json());
    setSoilLibrary(await soilResp.json());
    setMaintenanceAlerts(await maintenanceResp.json());
  };

  const applyHouses = async (value) => {
    setIsMutating(true);
    try {
      const response = await fetch(`${API_BASE}/api/pipeline/target`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target_houses: Number(value) })
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload?.error || "Failed to scale houses");
      setState(payload.state);
      setTargetHouses(payload.state?.houses?.length || Number(value));
      await refreshCharts();
    } catch (e) {
      setError(e.message);
    } finally {
      setIsMutating(false);
    }
  };

  const applyRobots = async (value) => {
    setIsMutating(true);
    try {
      const response = await fetch(`${API_BASE}/api/robots/target`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target_robots: Number(value) })
      });
      const payload = await response.json();
      if (!response.ok) throw new Error(payload?.error || "Failed to scale robots");
      setState(payload.state);
      setTargetRobots(payload.state?.robots?.length || Number(value));
      await refreshCharts();
    } catch (e) {
      setError(e.message);
    } finally {
      setIsMutating(false);
    }
  };

  const resetExperiment = async () => {
    setIsMutating(true);
    try {
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 20000);
      const response = await fetch(`${API_BASE}/api/experiment/reset`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ target_houses: targetHouses, target_robots: targetRobots }),
        signal: controller.signal
      });
      clearTimeout(timeoutId);

      const payload = await response.json();
      if (!response.ok) throw new Error(payload?.error || "Failed to reset experiment");

      setState(payload.state);
      setTargetHouses(payload.state?.houses?.length || Number(targetHouses));
      setTargetRobots(payload.state?.robots?.length || Number(targetRobots));
      setSelectedHouse(payload.state?.houses?.[0]?.id ?? null);
      await refreshCharts();
      setError("");
    } catch (e) {
      const msg = e?.name === "AbortError"
        ? "Reset timed out. Backend is likely busy; restart server to clear tick backlog."
        : e.message;
      setError(msg);
    } finally {
      setIsMutating(false);
    }
  };

  const metrics = state?.metrics;
  const houses = state?.houses ?? [];
  const robots = state?.robots ?? [];
  const laneSummary = state?.laneSummary ?? [];

  const houseStageMap = useMemo(() => {
    return houses.reduce((acc, house) => {
      acc[house.id] = house.stage;
      return acc;
    }, {});
  }, [houses]);

  const robotRoleById = useMemo(() => {
    const map = {};

    robots.forEach((robot) => {
      const stage = robot.active_house_id ? houseStageMap[robot.active_house_id] : null;

      if (stage === "site_prep") map[robot.id] = "excavator";
      else if (stage === "surveying") map[robot.id] = "survey";
      else if (stage === "sealing") map[robot.id] = "sealer";
      else if (stage && ["foundation", "framing", "mep", "finishing"].includes(stage)) map[robot.id] = "assembly";
      else if (robot.status === "waiting_component") map[robot.id] = "logistics";
      else if (robot.status === "placing") map[robot.id] = "assembly";
      else if (robot.status === "moving") map[robot.id] = "logistics";
      else if (robot.id % 3 === 0) map[robot.id] = "verification";
      else if (robot.id % 3 === 1) map[robot.id] = "fabricator";
      else map[robot.id] = "logistics";
    });

    return map;
  }, [robots, houseStageMap]);

  const roleRuntime = useMemo(() => {
    const base = robotRoleCatalog.reduce((acc, role) => {
      acc[role.id] = { count: 0, idle: 0, moving: 0, placing: 0, waiting_component: 0, utilization: 0 };
      return acc;
    }, {});

    robots.forEach((robot) => {
      const roleId = robotRoleById[robot.id] || "logistics";
      const row = base[roleId];
      row.count += 1;
      if (robot.status === "idle") row.idle += 1;
      if (robot.status === "moving") row.moving += 1;
      if (robot.status === "placing") row.placing += 1;
      if (robot.status === "waiting_component") row.waiting_component += 1;
    });

    Object.values(base).forEach((row) => {
      row.utilization = row.count > 0 ? ((row.count - row.idle) / row.count) * 100 : 0;
    });

    return base;
  }, [robots, robotRoleById]);

  useEffect(() => {
    if (roleTargetsSeededRef.current) return;
    if (!state || robots.length === 0) return;

    const next = robotRoleCatalog.reduce((acc, role) => {
      acc[role.id] = roleRuntime[role.id]?.count ?? 0;
      return acc;
    }, {});

    setRoleTargets(next);
    roleTargetsSeededRef.current = true;
  }, [state, robots.length, roleRuntime]);

  const houseStatus = useMemo(() => {
    if (!state?.cellCounts) return {};
    return state.cellCounts.reduce((acc, row) => {
      acc[row.house_id] ??= { empty: 0, reserved: 0, filled: 0 };
      acc[row.house_id][row.status] = Number(row.count);
      return acc;
    }, {});
  }, [state]);

  const terrainStatus = useMemo(() => {
    if (!state?.terrainCounts) return {};
    return state.terrainCounts.reduce((acc, row) => {
      acc[row.house_id] ??= {
        total: 0,
        ready: 0,
        avg_grade_error: Number(row.avg_grade_error ?? 0),
        avg_compaction: Number(row.avg_compaction ?? 0)
      };
      const count = Number(row.count);
      acc[row.house_id].total += count;
      if (row.status === "ready") {
        acc[row.house_id].ready += count;
      }
      acc[row.house_id].avg_grade_error = Number(row.avg_grade_error ?? acc[row.house_id].avg_grade_error ?? 0);
      acc[row.house_id].avg_compaction = Number(row.avg_compaction ?? acc[row.house_id].avg_compaction ?? 0);
      return acc;
    }, {});
  }, [state]);

  const terrainObstacleMap = useMemo(() => {
    if (!state?.terrainObstacles) return {};
    return state.terrainObstacles.reduce((acc, row) => {
      acc[row.house_id] = Number(row.obstacles_remaining ?? 0);
      return acc;
    }, {});
  }, [state]);

  const assemblyStatus = useMemo(() => {
    if (!state?.assemblyCounts) return {};
    return state.assemblyCounts.reduce((acc, row) => {
      acc[row.house_id] ??= {
        total: 0,
        activated: 0,
        failed: 0,
        in_progress: 0
      };

      const count = Number(row.count ?? 0);
      acc[row.house_id].total += count;

      if (row.status === "activated") {
        acc[row.house_id].activated += count;
      } else if (row.status === "failed") {
        acc[row.house_id].failed += count;
      } else {
        acc[row.house_id].in_progress += count;
      }

      return acc;
    }, {});
  }, [state]);

  const maintenanceByHouse = useMemo(() => {
    const rows = state?.maintenance ?? [];
    return rows.reduce((acc, row) => {
      acc[row.house_id] = row;
      return acc;
    }, {});
  }, [state]);

  const surveySummaryByHouse = useMemo(() => {
    const rows = state?.surveySummary ?? [];
    return rows.reduce((acc, row) => {
      acc[row.house_id] = row;
      return acc;
    }, {});
  }, [state]);


  const surveyRunsByHouse = useMemo(() => {
    const rows = state?.surveyRuns ?? [];
    return rows.reduce((acc, row) => {
      acc[row.house_id] = row;
      return acc;
    }, {});
  }, [state]);

  const laneAggregate = useMemo(() => {
    const totals = laneSummary.reduce((acc, row) => {
      const segments = Number(row.segments ?? 0);
      acc.segments += segments;
      acc.conditioned += Number(row.conditioned_segments ?? 0);
      acc.degraded += Number(row.degraded_segments ?? 0);
      acc.stale += Number(row.stale_segments ?? 0);
      acc.verificationEvents += Number(row.verification_events ?? 0);
      acc.weightedCondition += Number(row.avg_condition ?? 0) * segments;
      return acc;
    }, { segments: 0, conditioned: 0, degraded: 0, stale: 0, verificationEvents: 0, weightedCondition: 0 });

    return {
      segments: totals.segments,
      conditioned: totals.conditioned,
      degraded: totals.degraded,
      stale: totals.stale,
      verificationEvents: totals.verificationEvents,
      conditionedPct: totals.segments > 0 ? (totals.conditioned / totals.segments) * 100 : 0,
      avgCondition: totals.segments > 0 ? totals.weightedCondition / totals.segments : 0
    };
  }, [laneSummary]);

  const terrainOverall = useMemo(() => {
    const rows = state?.terrainCounts ?? [];
    return rows.reduce((acc, row) => {
      const count = Number(row.count ?? 0);
      acc.total += count;
      if (row.status === "ready") acc.ready += count;
      return acc;
    }, { total: 0, ready: 0 });
  }, [state]);

  const causalGlobal = state?.causalGlobal ?? {};

  const causalByHouse = useMemo(() => {
    const rows = state?.causalByHouse ?? [];
    return rows.reduce((acc, row) => {
      acc[row.house_id] = row;
      return acc;
    }, {});
  }, [state]);

  const latestMatrixPoint = matrix.length ? matrix[matrix.length - 1] : null;
  const matrixEfficiency = Number(latestMatrixPoint?.avg_efficiency ?? 0);
  const lifetimeQcTotal = Number(metrics?.blocks_verified ?? 0) + Number(metrics?.blocks_failed_qc ?? 0);
  const lifetimeQcPassRate = lifetimeQcTotal > 0 ? (Number(metrics?.blocks_verified ?? 0) / lifetimeQcTotal) * 100 : 0;

  const robotStatusBreakdown = useMemo(() => {
    const counts = { idle: 0, moving: 0, placing: 0, waiting_component: 0, other: 0 };
    robots.forEach((robot) => {
      if (Object.hasOwn(counts, robot.status)) counts[robot.status] += 1;
      else counts.other += 1;
    });

    const total = Math.max(1, robots.length);
    return {
      total,
      idlePct: (counts.idle / total) * 100,
      movingPct: (counts.moving / total) * 100,
      placingPct: (counts.placing / total) * 100,
      waitingPct: (counts.waiting_component / total) * 100,
      counts
    };
  }, [robots]);

  const bottleneck = useMemo(() => {
    if (!robots.length) {
      return {
        label: "No Active Robots",
        reason: "No robots currently available to classify.",
        recommendation: "Increase robot target or activate community kits."
      };
    }

    const terrainReadyPct = terrainOverall.total > 0 ? (terrainOverall.ready / terrainOverall.total) * 100 : 100;
    const hasSitePrep = houses.some((house) => house.stage === "site_prep");

    if (robotStatusBreakdown.waitingPct >= 30) {
      return {
        label: "Fabrication Limited",
        reason: `${robotStatusBreakdown.waitingPct.toFixed(1)}% of robots are waiting for components.`,
        recommendation: "Increase fabricator/logistics rate or reduce concurrent assembly demand."
      };
    }

    if (hasSitePrep && terrainReadyPct < 85) {
      return {
        label: "Site Prep Limited",
        reason: `Terrain ready is ${terrainReadyPct.toFixed(1)}% while houses are still in site prep.`,
        recommendation: "Allocate more excavator time and clear obstacles earlier."
      };
    }

    if (Number(metrics?.active_surveys ?? 0) > 0) {
      return {
        label: "Survey Gate Limited",
        reason: `${Number(metrics?.active_surveys ?? 0)} houses are held in surveying.`,
        recommendation: "Increase survey capacity or tune convergence thresholds."
      };
    }

    if (robotStatusBreakdown.movingPct >= 30) {
      return {
        label: "Logistics Limited",
        reason: `${robotStatusBreakdown.movingPct.toFixed(1)}% of robots are in transit.`,
        recommendation: "Reduce travel distance or add logistics staging near active houses."
      };
    }

    if (robotStatusBreakdown.placingPct >= 55 && robotStatusBreakdown.idlePct < 15) {
      return {
        label: "Assembly Saturated",
        reason: "Most robots are placing with low idle slack.",
        recommendation: "Scale robot count per cluster to lift throughput ceiling."
      };
    }

    return {
      label: "Balanced Flow",
      reason: "No single stage dominates wait/move/idle right now.",
      recommendation: "Run benchmark presets to identify the next limiting factor."
    };
  }, [robots.length, robotStatusBreakdown, terrainOverall, houses, metrics]);

  return (
    <div className="layout">
      <header>
        <h1>House Brain Mission Control</h1>
        <p>Neon + scheduler + stigmergic grid dispatch</p>
      </header>

      <section className="panel tab-strip">
        <div className="button-row">
          <button className={`pill ${activeTab === "mission" ? "active" : ""}`} onClick={() => setActiveTab("mission")}>Mission Control</button>
          <button className={`pill ${activeTab === "robot_spec" ? "active" : ""}`} onClick={() => setActiveTab("robot_spec")}>Robot Spec</button>
        </div>
      </section>

      {activeTab === "mission" ? (
      <>
      {error ? <div className="error">{error}</div> : null}

      <section className="controls panel">
        <h2>Experiment Controls</h2>

        <div className="control-block">
          <h3>Houses</h3>
          <div className="button-row">
            {HOUSE_TARGET_OPTIONS.map((n) => (
              <button key={n} className={`pill ${targetHouses === n ? "active" : ""}`} onClick={() => applyHouses(n)} disabled={isMutating}>
                {n}
              </button>
            ))}
          </div>
          <div className="slider-row">
            <input
              type="range"
              min="1"
              max="40"
              value={targetHouses}
              onChange={(e) => setTargetHouses(Number(e.target.value))}
              onMouseUp={(e) => applyHouses(Number(e.currentTarget.value))}
              onTouchEnd={(e) => applyHouses(Number(e.currentTarget.value))}
            />
            <span>{targetHouses}</span>
          </div>
        </div>

        <div className="control-block">
          <h3>Robots</h3>
          <div className="button-row">
            {ROBOT_TARGET_OPTIONS.map((n) => (
              <button key={n} className={`pill ${targetRobots === n ? "active" : ""}`} onClick={() => applyRobots(n)} disabled={isMutating}>
                {n}
              </button>
            ))}
          </div>
          <div className="slider-row">
            <input
              type="range"
              min="3"
              max="60"
              value={targetRobots}
              onChange={(e) => setTargetRobots(Number(e.target.value))}
              onMouseUp={(e) => applyRobots(Number(e.currentTarget.value))}
              onTouchEnd={(e) => applyRobots(Number(e.currentTarget.value))}
            />
            <span>{targetRobots}</span>
          </div>
        </div>

        <div className="button-row">
          <button className="pill action" onClick={resetExperiment} disabled={isMutating}>Reset Experiment Window</button>
        </div>

        <CurveChart points={curve} />
      </section>

      <section className="kpis">
        <article><span>Active Houses</span><strong>{houses.length}</strong></article>
        <article><span>Active Robots</span><strong>{robots.length}</strong></article>
        <article><span>Idle (Snapshot)</span><strong>{Number(metrics?.robot_idle_percent ?? 0).toFixed(1)}%</strong><small className="kpi-meta">latest sample</small></article>
        <article><span>Throughput (Snapshot)</span><strong>{Number(metrics?.throughput_cells_per_hour ?? 0).toFixed(1)} cells/h</strong><small className="kpi-meta">latest sample</small></article>
        <article><span>Efficiency (Snapshot)</span><strong>{Number(metrics?.pipeline_efficiency ?? 0).toFixed(1)}%</strong><small className="kpi-meta">latest sample</small></article>
        <article><span>Cells Filled</span><strong>{metrics?.cells_filled ?? 0} / {metrics?.total_cells ?? 0}</strong></article>
        <article><span>Efficiency (Matrix Avg)</span><strong>{matrixEfficiency.toFixed(1)}%</strong><small className="kpi-meta">aggregate window</small></article>
        <article><span>Terrain Cells Ready</span><strong>{terrainOverall.ready} / {terrainOverall.total}</strong></article>
        <article><span>Obstacles Left</span><strong>{Number(metrics?.obstacle_cells_remaining ?? 0)}</strong></article>
        <article><span>Avg Grade Error</span><strong>{Number(metrics?.avg_grade_error ?? 0).toFixed(3)}</strong></article>
        <article><span>Avg Compaction</span><strong>{Number(metrics?.avg_compaction ?? 0).toFixed(3)}</strong></article>
        <article><span>Kits Activated</span><strong>{Number(metrics?.community_kits_activated ?? 0)}</strong></article>
        <article><span>Kits Pending</span><strong>{Number(metrics?.community_kits_pending ?? 0)}</strong></article>
        <article><span>Kits Failed</span><strong>{Number(metrics?.community_kits_failed ?? 0)}</strong></article>
        <article><span>Active Surveys</span><strong>{Number(metrics?.active_surveys ?? 0)}</strong></article>
        <article><span>Soil Recipes Learned</span><strong>{Number(metrics?.soil_recipes_learned ?? 0)}</strong></article>
        <article><span>Blocks QC Passed</span><strong>{Number(metrics?.blocks_verified ?? 0)}</strong></article>
        <article><span>Blocks QC Failed</span><strong>{Number(metrics?.blocks_failed_qc ?? 0)}</strong></article>
        <article><span>Avg TTL (days)</span><strong>{Number(metrics?.avg_ttl_days ?? 0).toFixed(0)}</strong></article>
        <article><span>Maintenance Alerts</span><strong>{Number(metrics?.houses_in_maintenance ?? 0)}</strong></article>
        <article><span>Lane Condition</span><strong>{Number(metrics?.avg_lane_condition ?? laneAggregate.avgCondition ?? 0).toFixed(2)}</strong></article>
        <article><span>Conditioned Lanes</span><strong>{Number(metrics?.conditioned_lane_percent ?? laneAggregate.conditionedPct ?? 0).toFixed(1)}%</strong></article>
        <article><span>Degraded Segments</span><strong>{Number(metrics?.degraded_lane_segments ?? laneAggregate.degraded ?? 0)}</strong></article>
        <article><span>Stale Relay Segments</span><strong>{Number(metrics?.stale_relay_segments ?? laneAggregate.stale ?? 0)}</strong></article>
        <article><span>Lane Verifications</span><strong>{Number(metrics?.lane_verification_events ?? laneAggregate.verificationEvents ?? 0)}</strong></article>
        <article><span>Reference Patches</span><strong>{Number(metrics?.reference_patches_protected ?? 0)} / {Number(metrics?.reference_patches_total ?? 0)}</strong></article>
        <article><span>Corrections</span><strong>{Number(metrics?.placement_corrections ?? 0)}</strong></article>
        <article><span>Insert Fails</span><strong>{Number(metrics?.placement_failures ?? 0)}</strong></article>
        <article><span>Avg Retries</span><strong>{Number(metrics?.avg_retries_per_placement ?? 0).toFixed(2)}</strong></article>
      </section>

      <section className="panel">
        <h2>Flow Diagnostics</h2>
        <div className="bottleneck-grid">
          <article>
            <span>Detected Constraint</span>
            <strong>{bottleneck.label}</strong>
            <small>{bottleneck.reason}</small>
          </article>
          <article>
            <span>Robot State Mix</span>
            <strong>wait {robotStatusBreakdown.waitingPct.toFixed(1)}% | move {robotStatusBreakdown.movingPct.toFixed(1)}%</strong>
            <small>place {robotStatusBreakdown.placingPct.toFixed(1)}% | idle {robotStatusBreakdown.idlePct.toFixed(1)}%</small>
          </article>
          <article>
            <span>Recommended Action</span>
            <strong>{bottleneck.recommendation}</strong>
            <small>Use this to choose next tuning experiment.</small>
          </article>
        </div>
      </section>

      <section className="panel">
        <h2>Causal Chain</h2>
        <div className="causal-grid">
          <article>
            <span>Survey Approval</span>
            <strong>{Number(causalGlobal.approved_sites ?? 0)} / {Number(causalGlobal.surveyed_houses ?? 0)}</strong>
          </article>
          <article>
            <span>QC Pass Rate</span>
            <strong>{Number(causalGlobal.inline_total ?? 0) > 0 ? `${((Number(causalGlobal.inline_passed ?? 0) / Number(causalGlobal.inline_total ?? 1)) * 100).toFixed(1)}%` : `${lifetimeQcPassRate.toFixed(1)}%`}</strong>
            <small className="kpi-meta">{Number(causalGlobal.inline_total ?? 0) > 0 ? "inline window" : "lifetime fallback"}</small>
          </article>
          <article>
            <span>QC Retry Rate</span>
            <strong>{Number(causalGlobal.inline_avg_retries ?? 0).toFixed(2)}</strong>
          </article>
          <article>
            <span>Confidence (S/L)</span>
            <strong>{Number(causalGlobal.avg_short_confidence ?? 0).toFixed(2)} / {Number(causalGlobal.avg_long_confidence ?? 0).toFixed(2)}</strong>
          </article>
          <article>
            <span>TTL Spread</span>
            <strong>{Number(causalGlobal.min_ttl_days ?? 0)}-{Number(causalGlobal.max_ttl_days ?? 0)} d</strong>
          </article>
          <article>
            <span>Moisture Risk (Unsealed to Sealed)</span>
            <strong>{Number(causalGlobal.unsealed_moisture_risk ?? 0).toFixed(2)} to {Number(causalGlobal.sealed_moisture_risk ?? 0).toFixed(2)}</strong>
          </article>
        </div>
        {selectedHouse && causalByHouse[selectedHouse] ? (
          <div className="causal-chain">
            <strong>{causalByHouse[selectedHouse].name}</strong>
            <small>{`${causalByHouse[selectedHouse].survey_status} survey to QC ${Number(causalByHouse[selectedHouse].qc_passed ?? 0)}/${Number(causalByHouse[selectedHouse].qc_total ?? 0)} to conf ${Number(causalByHouse[selectedHouse].short_confidence ?? 0).toFixed(2)}/${Number(causalByHouse[selectedHouse].long_confidence ?? 0).toFixed(2)} to TTL ${causalByHouse[selectedHouse].ttl_days ?? "-"}d to coating v${causalByHouse[selectedHouse].coating_version ?? "-"} to moisture risk ${Number(causalByHouse[selectedHouse].moisture_risk ?? 0).toFixed(2)}`}</small>
          </div>
        ) : null}
      </section>

      <section className="panel">
        <h2>Experiment Matrix (Houses x Robots)</h2>
        <table>
          <thead>
            <tr><th>Houses</th><th>Robots</th><th>Efficiency</th><th>Idle</th><th>Throughput</th><th>Terrain Ready</th><th>Obstacles</th><th>Grade Err</th><th>Compaction</th><th>Kits Activated</th><th>Surveys</th><th>Soils</th><th>QC Pass</th><th>QC Fail</th><th>Maint</th><th>Avg TTL</th><th>Lane Cond</th><th>Lane %</th><th>Degr</th><th>Stale</th><th>Lane Verif</th><th>Ref P/T</th><th>Avg Retries</th><th>Avg Failures</th><th>Samples</th></tr>
          </thead>
          <tbody>
            {matrix.slice(-20).map((row) => (
              <tr key={`${row.active_houses}-${row.active_robots}`}>
                <td>{row.active_houses}</td>
                <td>{row.active_robots}</td>
                <td>{Number(row.avg_efficiency).toFixed(1)}%</td>
                <td>{Number(row.avg_idle).toFixed(1)}%</td>
                <td>{Number(row.avg_throughput).toFixed(1)}</td>
                <td>{Number(row.avg_terrain_ready ?? 0).toFixed(1)}%</td>
                <td>{Number(row.avg_obstacles_remaining ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_grade_error ?? 0).toFixed(3)}</td>
                <td>{Number(row.avg_compaction ?? 0).toFixed(3)}</td>
                <td>{Number(row.avg_kits_activated ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_active_surveys ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_soil_recipes ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_blocks_verified ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_blocks_failed ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_maintenance_houses ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_ttl_days ?? 0).toFixed(0)}</td>
                <td>{Number(row.avg_lane_condition ?? 0).toFixed(2)}</td>
                <td>{Number(row.avg_conditioned_lane_percent ?? 0).toFixed(1)}%</td>
                <td>{Number(row.avg_degraded_lane_segments ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_stale_relay_segments ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_lane_verification_events ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_reference_patches_protected ?? 0).toFixed(1)} / {Number(row.avg_reference_patches_total ?? 0).toFixed(1)}</td>
                <td>{Number(row.avg_retries ?? 0).toFixed(2)}</td>
                <td>{Number(row.avg_failures ?? 0).toFixed(2)}</td>
                <td>{row.samples}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="split">
        <div className="panel">
          <h2>Site Heatmap: House {selectedHouse ?? "-"}</h2>
          <SiteHeatmap probes={siteProbes} />
        </div>

        <div className="panel">
          <h2>Maintenance Timeline</h2>
          <MaintenanceTimeline rows={maintenanceAlerts.length ? maintenanceAlerts : (state?.maintenance ?? [])} />
        </div>
      </section>

      <section className="panel">
        <h2>Soil Library</h2>
        <table>
          <thead>
            <tr>
              <th>Signature</th>
              <th>Recipe</th>
              <th>Short Conf</th>
              <th>Long Conf</th>
              <th>Cycles</th>
              <th>Erosion</th>
              <th>Verified</th>
            </tr>
          </thead>
          <tbody>
            {soilLibrary.slice(0, 40).map((row) => (
              <tr key={row.soil_signature}>
                <td>{row.soil_signature}</td>
                <td>{row.recipe ? JSON.stringify(row.recipe) : "-"}</td>
                <td>{Number(row.short_term_confidence ?? 0).toFixed(2)}</td>
                <td>{Number(row.long_term_confidence ?? 0).toFixed(2)}</td>
                <td>{Number(row.weathering_cycles_tested ?? 0)}</td>
                <td>{Number(row.erosion_score ?? 0).toFixed(2)}</td>
                <td>{Number(row.total_blocks_verified ?? 0)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>

      <section className="panel">
        <h2>Logistics Lane Summary</h2>
        <table>
          <thead>
            <tr>
              <th>House</th>
              <th>Lane</th>
              <th>Segments</th>
              <th>Avg Condition</th>
              <th>Conditioned</th>
              <th>Degraded</th>
              <th>Stale Relay</th>
              <th>Verifications</th>
              <th>Restamp</th>
            </tr>
          </thead>
          <tbody>
            {laneSummary.length ? laneSummary.map((row) => (
              <tr key={`lane-${row.house_id}-${row.lane_id}`}>
                <td>House {row.house_id}</td>
                <td>{row.lane_id}</td>
                <td>{Number(row.segments ?? 0)}</td>
                <td>{Number(row.avg_condition ?? 0).toFixed(2)}</td>
                <td>{Number(row.conditioned_segments ?? 0)}</td>
                <td>{Number(row.degraded_segments ?? 0)}</td>
                <td>{Number(row.stale_segments ?? 0)}</td>
                <td>{Number(row.verification_events ?? 0)}</td>
                <td>{row.restamp_required ? "yes" : "no"}</td>
              </tr>
            )) : (
              <tr>
                <td colSpan={9}>No active lane segments yet.</td>
              </tr>
            )}
          </tbody>
        </table>
        <small className="muted-note">Aggregate: {laneAggregate.conditioned}/{laneAggregate.segments} conditioned ({laneAggregate.conditionedPct.toFixed(1)}%), avg condition {laneAggregate.avgCondition.toFixed(2)}.</small>
      </section>

      <section className="board">
        {stageOrder.map((stage) => (
          <div key={stage} className="column">
            <h3>{stageLabels[stage] ?? stage}</h3>
            {houses.filter((h) => h.stage === stage).map((house) => {
              const c = houseStatus[house.id] || { empty: 0, reserved: 0, filled: 0 };
              const total = c.empty + c.reserved + c.filled || 1;
              const fillPct = Math.round((c.filled / total) * 100);

              const terrain = terrainStatus[house.id] || { total: 0, ready: 0, avg_grade_error: 0, avg_compaction: 0 };
              const terrainPct = terrain.total > 0 ? Math.round((terrain.ready / terrain.total) * 100) : 0;
              const obstaclesRemaining = Number(terrainObstacleMap[house.id] ?? 0);

              const kits = assemblyStatus[house.id] || { total: 0, activated: 0, failed: 0, in_progress: 0 };
              const kitPct = kits.total > 0 ? Math.round((kits.activated / kits.total) * 100) : 0;

              const survey = surveySummaryByHouse[house.id] || { probes: 0, buildable: 0, marginal: 0, reject: 0 };
              const surveyPct = Number(survey.probes ?? 0) > 0
                ? Math.round((Number(survey.buildable ?? 0) / Number(survey.probes)) * 100)
                : 0;

              const sealingPct = Math.round(Math.max(0, Math.min(1, Number(house.sealing_progress ?? 0))) * 100);
              const maintenanceRow = maintenanceByHouse[house.id];

              const progressPct = stage === "site_prep"
                ? terrainPct
                : stage === "surveying"
                  ? surveyPct
                  : stage === "sealing"
                    ? sealingPct
                    : (stage === "community_assembly" ? kitPct : fillPct);

              const progressLabel = stage === "site_prep"
                ? "site-ready"
                : stage === "surveying"
                  ? "buildable"
                  : stage === "sealing"
                    ? "sealed"
                    : (stage === "community_assembly" ? "kits-activated" : "filled");

              const detailLine = stage === "community_assembly"
                ? `kits: ${kits.activated}/${kits.total || 0} | failed: ${kits.failed}`
                : stage === "surveying"
                  ? `probes: ${survey.probes || 0} | reject: ${survey.reject || 0}`
                  : stage === "sealing"
                    ? `sealing: ${sealingPct}% | status: ${house.sealing_complete ? "complete" : "in-progress"}`
                    : `grade err: ${Number(terrain.avg_grade_error ?? 0).toFixed(3)} | compact: ${Number(terrain.avg_compaction ?? 0).toFixed(3)}`;

              return (
                <button
                  key={house.id}
                  className={`card ${selectedHouse === house.id ? "active" : ""}`}
                  onClick={() => setSelectedHouse(house.id)}
                >
                  <strong>{house.name}</strong>
                  <small>{progressPct}% {progressLabel}</small>
                  <small>{detailLine}</small>
                  <small>survey: {house.survey_status ?? "pending"} | zone: {house.site_zone ?? "BUILDABLE"}</small>
                  <small>uncertainty: {Number(house.survey_uncertainty_remaining ?? 0).toFixed(2)} | probes: {house.survey_probe_count ?? 0}</small>
                  <small>soil: {house.soil_signature ?? "-"}</small>
                  <small>sealed: {house.sealing_complete ? "yes" : "no"} | cluster: {house.cluster_id ?? "unassigned"}</small>
                  <small>obstacles: {obstaclesRemaining} | ttl: {maintenanceRow?.ttl_days ?? "-"}d</small>
                </button>
              );
            })}
          </div>
        ))}
      </section>

      <section className="panel grid">
        <h2>Suburb 3D: All Active Houses + Robots</h2>
        <div className="canvas-wrap suburb-wrap">
          <div className="button-row overlay-row">
            <button className={`pill ${showRoleOverlay ? "active" : ""}`} onClick={() => setShowRoleOverlay((v) => !v)}>
              {showRoleOverlay ? "Role Colors Enabled" : "Role Colors Disabled"}
            </button>
          </div>
          <SuburbScene
            houses={houses}
            cells={suburbCells}
            terrain={suburbTerrain}
            robots={robots}
            selectedHouse={selectedHouse}
            roleByRobotId={robotRoleById}
            showRoleOverlay={showRoleOverlay}
          />
        </div>
      </section>

      <section className="split">
        <div className="panel grid">
          <h2>3D Grid: House {selectedHouse ?? "-"}</h2>
          <div className="canvas-wrap">
            <GridScene cells={cells} />
          </div>
        </div>

        <div className="panel">
          <h2>Robots</h2>
          <table>
            <thead>
              <tr><th>ID</th><th>Status</th><th>Role</th><th>House</th><th>Pos</th></tr>
            </thead>
            <tbody>
              {robots.map((r) => (
                <tr key={r.id}>
                  <td>{r.id}</td>
                  <td>{r.status}</td>
                  <td>{robotRoleById[r.id] ?? "-"}</td>
                  <td>{r.active_house_id ?? "-"}</td>
                  <td>{r.pos_x},{r.pos_y},{r.pos_z}</td>
                </tr>
              ))}
            </tbody>
          </table>

          <h2>Fabricator Queue</h2>
          <ul className="queue">
            {(state?.fabricatorQueue ?? []).map((q) => (
              <li key={q.status}>{q.status}: {q.count}</li>
            ))}
          </ul>

          <h2>Community Assembly Queue</h2>
          <ul className="queue">
            {(state?.assemblyQueue ?? []).map((q) => (
              <li key={`assembly-${q.status}`}>{q.status}: {q.count}</li>
            ))}
          </ul>
        </div>
      </section>
      </>
      ) : (
        <RobotSpecTab
          robots={robots}
          houses={houses}
          roleRuntime={roleRuntime}
          selectedRoleId={selectedRoleId}
          setSelectedRoleId={setSelectedRoleId}
          roleTargets={roleTargets}
          setRoleTargets={setRoleTargets}
          onScaleRobots={applyRobots}
          isMutating={isMutating}
          showRoleOverlay={showRoleOverlay}
          setShowRoleOverlay={setShowRoleOverlay}
          metrics={metrics}
        />
      )}
    </div>
  );
}

