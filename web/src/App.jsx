import { useEffect, useMemo, useRef, useState } from "react";
import { Canvas, useFrame } from "@react-three/fiber";
import { OrbitControls } from "@react-three/drei";
import * as THREE from "three";

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8787";
const HOUSE_TARGET_OPTIONS = [3, 5, 10, 20];
const ROBOT_TARGET_OPTIONS = [9, 18, 36];

/*  COLOR MAPS  */
const colorMap = { empty: "#6b1d1d", reserved: "#b8860b", filled: "#0a7a43" };
const robotColorMap = { idle: "#7f8ea3", waiting_component: "#f4d35e", placing: "#3ddc97", moving: "#7aa2ff" };
const roleColorMap = {
  survey: "#56b4ff", excavator: "#c28f5c", fabricator: "#9f7aea",
  verification: "#f6d365", assembly: "#3ddc97", sealer: "#66d9ef", logistics: "#f4978e"
};
const terrainColorMap = { raw: "#5e3e2d", grading: "#8b5a36", compacted: "#8a7d4a", ready: "#3a6f54" };
const obstacleColorMap = { root: "#9a6d3a", rock: "#6e7580", debris: "#8a5a44" };

const TERRAIN_HEIGHT_SCALE = 0.8;
const TERRAIN_BASE_Y = -0.55;

const stageOrder = ["site_prep","surveying","foundation","framing","mep","finishing","sealing","community_assembly","complete"];
const stageLabels = { site_prep:"Site Prep",surveying:"Surveying",foundation:"Foundation",framing:"Framing",mep:"MEP",finishing:"Finishing",sealing:"Sealing",community_assembly:"Community Asm.",complete:"Complete" };

const robotRoleCatalog = [
  { id:"survey",label:"Survey",primaryJobs:"Probe grid, soil sample",requiredDof:"3 (XYZ)",forceNeeds:"200N penetration",dataInterface:"probe(x,y,depth) -> soil_signature",commands:["goto(x,y)","deploy_feet()","equalize_load()","seat_outer_sleeve(depth)","probe_inner(depth)","classify_point()","densify_region(region_id)","stamp_reference_patch()","emit_heatmap(site_id)","abort_probe(reason)"],emits:{table:"site_surveys",payload:{}},c2a:"Constraint: expensive CPT rigs. Transmutation: lightweight repeated probes + Bayesian confidence." },
  { id:"excavator",label:"Excavator",primaryJobs:"Grade, compact, dig",requiredDof:"4 (XYZ+tilt)",forceNeeds:"1kN compaction",dataInterface:"grade(area,slope), compact(target)",commands:["grade(area,slope)","compact(target)","clearObstacle(type)"],emits:{table:"terrain_cells",payload:{}},c2a:"Constraint: unstructured terrain. Transmutation: measurable grid with grade+compaction thresholds." },
  { id:"fabricator",label:"Fabricator",primaryJobs:"Mix soil, form blocks",requiredDof:"3 (mixer+extrude)",forceNeeds:"500N pressure",dataInterface:"produce(signature) -> block_id",commands:["readSoil(signature)","selectRecipe(signature)","produce(signature)"],emits:{table:"soil_library",payload:{}},c2a:"Constraint: fixed recipe fails across soils. Transmutation: inline soil signature routing to adaptive recipe library." },
  { id:"verification",label:"Verification",primaryJobs:"QC test, weathering",requiredDof:"2 (test+move)",forceNeeds:"100N penetrometer",dataInterface:"qc(block_id) -> pass/metrics",commands:["qc(block_id)","runWeathering(signature)","updateConfidence(signature)"],emits:{table:"block_verifications",payload:{}},c2a:"Constraint: slow destructive testing. Transmutation: risk-based QC where high-confidence soils skip full verification." },
  { id:"assembly",label:"Assembly",primaryJobs:"Place blocks, impedance",requiredDof:"4 (XYZ+rotate)",forceNeeds:"50N Z-force",dataInterface:"place(block_id,x,y,z) -> success",commands:["claimCell(house_id)","moveTo(x,y,z)","place(block_id,x,y,z)"],emits:{table:"grid_cells",payload:{}},c2a:"Constraint: no tactile sensors. Transmutation: motor impedance deviation on Z trajectory as force proxy." },
  { id:"sealer",label:"Sealer",primaryJobs:"Coat exterior",requiredDof:"3 (XYZ spray)",forceNeeds:"10N pressure",dataInterface:"seal(surface) -> done",commands:["spray(surface)","verifyCoverage(house_id)","completeSealing(house_id)"],emits:{table:"house_maintenance",payload:{}},c2a:"Constraint: seam-by-seam bottleneck. Transmutation: blanket-seal all exterior surfaces in parallel." },
  { id:"logistics",label:"Logistics",primaryJobs:"Material cart + lane conditioning",requiredDof:"3 (XYZ cart)",forceNeeds:"500kg carry + light drag",dataInterface:"route(payload, lane) -> delivered",commands:["goto(x,y)","pickup(payload_id)","dock(station_id)","grade_until_match(segment_id)","relay_reference_state()","emit_status()"],emits:{table:"logistics_lane_segments",payload:{}},c2a:"Constraint: bad terrain and lane drift break delivery. Transmutation: body-as-gauge verification + relay truth." }
];

/*  3D HELPERS  */
function InstancedCells({ positions, color, opacity, size = [0.9,0.9,0.9] }) {
  const ref = useRef(null);
  useEffect(() => {
    if (!ref.current) return;
    const temp = new THREE.Object3D();
    positions.forEach((pos, i) => { temp.position.set(pos[0],pos[1],pos[2]); temp.updateMatrix(); ref.current.setMatrixAt(i, temp.matrix); });
    ref.current.instanceMatrix.needsUpdate = true;
  }, [positions]);
  if (!positions.length) return null;
  return <instancedMesh ref={ref} args={[null,null,positions.length]}><boxGeometry args={size}/><meshStandardMaterial color={color} opacity={opacity} transparent/></instancedMesh>;
}

function AnimatedCarrier({ robot, start, end, colorOverride }) {
  const groupRef = useRef(null);
  const cargoRef = useRef(null);
  useFrame((state) => {
    if (!groupRef.current) return;
    const t = state.clock.getElapsedTime() * 0.42 + robot.id * 0.173;
    const shuttling = ["placing","moving","waiting_component"].includes(robot.status);
    const waiting = robot.status === "waiting_component";
    let progress = 1;
    if (shuttling) { const loop = (t/(waiting?1.45:1))%1; const s=loop<0.5?loop*2:(1-loop)*2; progress=waiting?(0.24+s*0.58):s; }
    else if (robot.status==="idle") progress=0.12+0.08*(0.5+0.5*Math.sin(t*1.9));
    const clamped = Math.max(0,Math.min(1,progress));
    const x=start[0]+(end[0]-start[0])*clamped; const z=start[2]+(end[2]-start[2])*clamped;
    const lateral=Math.sin(t*3.1)*(shuttling?0:0.18);
    const arc=shuttling?Math.sin(Math.PI*clamped)*1.2:0.3+0.08*Math.sin(t*5.2);
    const y=start[1]+(end[1]-start[1])*clamped+arc;
    groupRef.current.position.set(x+lateral,y,z+(waiting?lateral*0.6:0));
    if (cargoRef.current) { cargoRef.current.visible=shuttling||waiting; cargoRef.current.rotation.y+=waiting?0.035:0.02; }
  });
  return (
    <group ref={groupRef}>
      <mesh><sphereGeometry args={[0.35,14,14]}/><meshStandardMaterial color={colorOverride||robotColorMap[robot.status]||"#fff"} emissive={colorOverride||robotColorMap[robot.status]||"#fff"} emissiveIntensity={0.35}/></mesh>
      <mesh ref={cargoRef} position={[0,0.6,0]}><boxGeometry args={[0.45,0.45,0.45]}/><meshStandardMaterial color="#9fc6ff" emissive="#3d7fff" emissiveIntensity={0.2}/></mesh>
    </group>
  );
}

function GridScene({ cells }) {
  return (
    <Canvas camera={{ position:[16,14,20], fov:55 }}>
      <ambientLight intensity={0.6}/>
      <directionalLight position={[10,20,10]} intensity={0.8}/>
      <group position={[-4.5,0,-4.5]}>
        {cells.map((cell) => (
          <mesh key={cell.id} position={[cell.x,cell.z,cell.y]}>
            <boxGeometry args={[0.92,0.92,0.92]}/>
            <meshStandardMaterial color={colorMap[cell.status]||"#444"} opacity={cell.status==="empty"?0.35:1} transparent/>
          </mesh>
        ))}
      </group>
      <gridHelper args={[16,16,"#666","#333"]}/>
      <OrbitControls makeDefault/>
    </Canvas>
  );
}

function SuburbScene({ houses, cells, terrain, robots, selectedHouse, roleByRobotId, showRoleOverlay }) {
  const layout = useMemo(() => {
    const ordered=[...houses].sort((a,b)=>a.id-b.id);
    const cols=Math.max(1,Math.ceil(Math.sqrt(ordered.length||1)));
    const spacing=14; const houseMap=new Map();
    ordered.forEach((house,idx)=>{ const col=idx%cols; const row=Math.floor(idx/cols); houseMap.set(house.id,{x:col*spacing,z:row*spacing}); });
    return { houseMap, cols, rows:Math.max(1,Math.ceil(ordered.length/cols)), spacing };
  }, [houses]);

  const houseStageById = useMemo(()=>new Map(houses.map(h=>[h.id,h.stage])),[houses]);

  const groupedCells = useMemo(()=>{
    const groups={reserved:[],filled:[]};
    cells.forEach(cell=>{ const offset=layout.houseMap.get(cell.house_id); if(!offset)return; const pos=[offset.x+cell.x,cell.z,offset.z+cell.y]; if(cell.status==="reserved")groups.reserved.push(pos); else if(cell.status==="filled")groups.filled.push(pos); });
    return groups;
  },[cells,layout.houseMap]);

  const groupedTerrain = useMemo(()=>{
    const groups={raw:[],grading:[],compacted:[],ready:[],root:[],rock:[],debris:[]};
    terrain.forEach(cell=>{ const offset=layout.houseMap.get(cell.house_id); if(!offset)return; const key=Object.hasOwn(groups,cell.status)?cell.status:"raw"; const y=TERRAIN_BASE_Y+Number(cell.current_grade||0)*TERRAIN_HEIGHT_SCALE; const x=offset.x+cell.x; const z=offset.z+cell.y; groups[key].push([x,y,z]); const ot=cell.obstacle_type; if(!Boolean(cell.obstacle_cleared)&&Object.hasOwn(obstacleColorMap,ot))groups[ot].push([x,y+0.2,z]); });
    return groups;
  },[terrain,layout.houseMap]);

  const terrainHeightByCell = useMemo(()=>{ const map=new Map(); terrain.forEach(c=>map.set(`${c.house_id}:${c.x}:${c.y}`,Number(c.current_grade||0))); return map; },[terrain]);

  const worldWidth=Math.max(layout.cols*layout.spacing,layout.spacing);
  const worldDepth=Math.max(layout.rows*layout.spacing,layout.spacing);
  const bayCenter=[worldWidth/2-2.5,0.55,-7];

  const robotMarkers = useMemo(()=>robots.filter(r=>r.active_house_id&&layout.houseMap.has(r.active_house_id)).map(robot=>{ const offset=layout.houseMap.get(robot.active_house_id); const laneOffset=((robot.id%7)-3)*0.65; const stage=houseStageById.get(robot.active_house_id); const terrainGrade=terrainHeightByCell.get(`${robot.active_house_id}:${robot.pos_x}:${robot.pos_y}`)??0; const terrainTopY=TERRAIN_BASE_Y+terrainGrade*TERRAIN_HEIGHT_SCALE+0.1; const blockY=robot.pos_z+0.9; return { id:robot.id, status:robot.status, role:roleByRobotId?.[robot.id]||"logistics", start:[bayCenter[0]+laneOffset,bayCenter[1],bayCenter[2]], end:[offset.x+robot.pos_x,stage==="site_prep"?terrainTopY+0.2:blockY,offset.z+robot.pos_y] }; }),[robots,layout.houseMap,bayCenter,houseStageById,terrainHeightByCell]);

  const target=[worldWidth/2-2.5,0,worldDepth/2-2.5];

  return (
    <Canvas camera={{ position:[worldWidth*0.7,Math.max(20,worldWidth*0.5),worldDepth*1.05], fov:55 }}>
      <ambientLight intensity={0.5}/><directionalLight position={[40,50,20]} intensity={0.9}/>
      <group position={[bayCenter[0],0,bayCenter[2]]}>
        <mesh position={[0,-0.4,0]}><boxGeometry args={[12.5,0.35,4.4]}/><meshStandardMaterial color="#2e4158"/></mesh>
        <mesh position={[0,0.35,0]}><boxGeometry args={[11.8,0.18,3.4]}/><meshStandardMaterial color="#1a6e9c" emissive="#0b3f5a" emissiveIntensity={0.2}/></mesh>
        {Array.from({length:8}).map((_,i)=>(<mesh key={i} position={[-4+(i%4)*2.7,0.25+Math.floor(i/4)*0.52,-0.6+(i%2)*1.2]}><boxGeometry args={[0.9,0.45,0.9]}/><meshStandardMaterial color="#9fc6ff"/></mesh>))}
      </group>
      {Array.from(layout.houseMap.entries()).map(([houseId,offset])=>(<mesh key={houseId} position={[offset.x+4.5,-0.72,offset.z+4.5]}><boxGeometry args={[10.2,0.05,10.2]}/><meshStandardMaterial color={selectedHouse===houseId?"#1d3b57":"#132234"}/></mesh>))}
      <InstancedCells positions={groupedTerrain.raw} color={terrainColorMap.raw} opacity={0.92} size={[0.95,0.16,0.95]}/>
      <InstancedCells positions={groupedTerrain.grading} color={terrainColorMap.grading} opacity={0.92} size={[0.95,0.16,0.95]}/>
      <InstancedCells positions={groupedTerrain.compacted} color={terrainColorMap.compacted} opacity={0.92} size={[0.95,0.16,0.95]}/>
      <InstancedCells positions={groupedTerrain.ready} color={terrainColorMap.ready} opacity={0.96} size={[0.95,0.16,0.95]}/>
      <InstancedCells positions={groupedTerrain.root} color={obstacleColorMap.root} opacity={0.95} size={[0.24,0.4,0.24]}/>
      <InstancedCells positions={groupedTerrain.rock} color={obstacleColorMap.rock} opacity={0.95} size={[0.42,0.28,0.42]}/>
      <InstancedCells positions={groupedTerrain.debris} color={obstacleColorMap.debris} opacity={0.95} size={[0.3,0.2,0.3]}/>
      <InstancedCells positions={groupedCells.reserved} color={colorMap.reserved} opacity={0.95}/>
      <InstancedCells positions={groupedCells.filled} color={colorMap.filled} opacity={1}/>
      {robotMarkers.map(robot=>(<mesh key={`t-${robot.id}`} position={robot.end}><sphereGeometry args={[0.14,10,10]}/><meshStandardMaterial color="#9ec5ff" opacity={0.35} transparent/></mesh>))}
      {robotMarkers.map(robot=>(<AnimatedCarrier key={robot.id} robot={robot} start={robot.start} end={robot.end} colorOverride={showRoleOverlay?roleColorMap[robot.role]:null}/>))}
      {selectedHouse&&layout.houseMap.get(selectedHouse)?(<mesh position={[layout.houseMap.get(selectedHouse).x+4.5,2.6,layout.houseMap.get(selectedHouse).z+4.5]}><boxGeometry args={[10.8,5.6,10.8]}/><meshBasicMaterial color="#00ff88" wireframe transparent opacity={0.6}/></mesh>):null}
      <gridHelper args={[Math.max(worldWidth,worldDepth)+12,Math.max(layout.cols,layout.rows)*12,"#4e6075","#243245"]} position={target}/>
      <OrbitControls makeDefault target={target}/>
    </Canvas>
  );
}

/*  UI COMPONENTS  */

function StatCard({ label, value, sub, accent, pulse }) {
  return (
    <div style={{ position:"relative", background:"rgba(0,255,136,0.03)", border:`1px solid ${accent||"rgba(0,255,136,0.15)"}`, borderRadius:8, padding:"12px 14px", display:"grid", gap:4, overflow:"hidden" }}>
      {pulse && <span style={{ position:"absolute", top:8, right:8, width:6, height:6, borderRadius:"50%", background:"#00ff88", boxShadow:"0 0 8px #00ff88", animation:"pulse 2s infinite" }}/>}
      <div style={{ fontSize:"0.7rem", letterSpacing:"0.08em", textTransform:"uppercase", color:"#4a7c6a", fontFamily:"'Courier New', monospace" }}>{label}</div>
      <div style={{ fontSize:"1.15rem", fontWeight:700, color:"#00ff88", fontFamily:"'Courier New', monospace", letterSpacing:"0.04em" }}>{value}</div>
      {sub && <div style={{ fontSize:"0.67rem", color:"#3d5c50", fontFamily:"'Courier New', monospace" }}>{sub}</div>}
    </div>
  );
}

function ScanlineBar({ pct, color="#00ff88", height=6 }) {
  return (
    <div style={{ background:"rgba(0,0,0,0.5)", border:"1px solid rgba(0,255,136,0.12)", borderRadius:3, height, overflow:"hidden" }}>
      <div style={{ width:`${Math.max(0,Math.min(100,pct))}%`, height:"100%", background:color, boxShadow:`0 0 6px ${color}`, transition:"width 0.6s ease" }}/>
    </div>
  );
}

function SiteHeatmap({ probes }) {
  const ordered=[...probes].sort((a,b)=>Number(a.probe_y)-Number(b.probe_y)||Number(a.probe_x)-Number(b.probe_x)).slice(0,25);
  const colorForStatus=(s)=>s==="BUILDABLE"?"#00ff88":s==="MARGINAL"?"#f4d35e":"#ff4455";
  if(!ordered.length) return <div style={{ color:"#3d5c50", fontFamily:"monospace", fontSize:"0.8rem", padding:"20px 0" }}>// NO SURVEY DATA YET</div>;
  return (
    <div style={{ display:"grid", gridTemplateColumns:"repeat(5,1fr)", gap:4 }}>
      {ordered.map((probe,idx)=>(
        <div key={`${probe.id??idx}`} title={`${probe.status} conf=${Number(probe.confidence??0).toFixed(2)}`} style={{ background:`${colorForStatus(probe.status)}18`, border:`1px solid ${colorForStatus(probe.status)}55`, outline:probe.densified?"1px solid #56b4ff":undefined, borderRadius:5, padding:"6px 4px", textAlign:"center", cursor:"default" }}>
          <div style={{ color:colorForStatus(probe.status), fontFamily:"monospace", fontSize:"0.75rem", fontWeight:700 }}>{probe.status?.charAt(0)??"-"}</div>
          <div style={{ color:"#4a7c6a", fontFamily:"monospace", fontSize:"0.6rem" }}>{Number(probe.confidence??0).toFixed(2)}</div>
        </div>
      ))}
    </div>
  );
}

function MaintenanceTimeline({ rows }) {
  if(!rows?.length) return <div style={{ color:"#3d5c50", fontFamily:"monospace", fontSize:"0.8rem" }}>// NO MAINTENANCE RECORDS</div>;
  const now=Date.now();
  return (
    <div style={{ display:"grid", gap:8 }}>
      {rows.map(item=>{
        const expires=item.ttl_expires_at?new Date(item.ttl_expires_at).getTime():null;
        const applied=item.coating_applied_at?new Date(item.coating_applied_at).getTime():null;
        const totalWindow=expires&&applied?Math.max(1,expires-applied):1;
        const elapsed=expires&&applied?Math.max(0,Math.min(totalWindow,now-applied)):0;
        const progressPct=Math.max(0,Math.min(100,100-(elapsed/totalWindow)*100));
        const daysRemaining=expires?Math.max(0,Math.round((expires-now)/86400000)):null;
        const isAlert=item.status==="alert";
        return (
          <div key={`maint-${item.house_id}`} style={{ background:"rgba(0,255,136,0.02)", border:`1px solid ${isAlert?"rgba(255,68,85,0.4)":"rgba(0,255,136,0.1)"}`, borderRadius:7, padding:"10px 12px", display:"grid", gap:6 }}>
            <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center" }}>
              <span style={{ fontFamily:"monospace", fontSize:"0.82rem", color:"#a0c8b8" }}>{item.house_name??`House ${item.house_id}`}</span>
              <span style={{ fontFamily:"monospace", fontSize:"0.7rem", color:"#3d5c50" }}>v{item.coating_version}</span>
            </div>
            <ScanlineBar pct={progressPct} color={isAlert?"#ff4455":"#00ff88"}/>
            <div style={{ fontFamily:"monospace", fontSize:"0.68rem", color:isAlert?"#ff6677":"#3d5c50" }}>
              {isAlert?" ALERT: ":""}{daysRemaining??"-"}d remaining
            </div>
          </div>
        );
      })}
    </div>
  );
}

function CurveChart({ points }) {
  const W=480, H=160, pad=36;
  if(!points.length) return <div style={{ color:"#3d5c50", fontFamily:"monospace", fontSize:"0.8rem", padding:"20px 0" }}>// NO CURVE DATA  RUN EXPERIMENT RESET TO POPULATE</div>;
  const xs=points.map(p=>Number(p.active_houses)), ys=points.map(p=>Number(p.avg_efficiency));
  const xMin=Math.min(...xs), xMax=Math.max(...xs);
  const scaleX=v=>xMax===xMin?W/2:pad+((v-xMin)/(xMax-xMin))*(W-pad*2);
  const scaleY=v=>H-pad-((v-0)/(100-0))*(H-pad*2);
  const polyline=points.map(p=>`${scaleX(Number(p.active_houses))},${scaleY(Number(p.avg_efficiency))}`).join(" ");
  const area=`${scaleX(Number(points[0].active_houses))},${H-pad} `+polyline+` ${scaleX(Number(points[points.length-1].active_houses))},${H-pad}`;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} style={{ width:"100%", height:H, background:"rgba(0,0,0,0.4)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:8 }}>
      <defs><linearGradient id="eff-grad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#00ff88" stopOpacity="0.3"/><stop offset="100%" stopColor="#00ff88" stopOpacity="0"/></linearGradient></defs>
      {[0,25,50,75,100].map(v=>(<line key={v} x1={pad} y1={scaleY(v)} x2={W-pad} y2={scaleY(v)} stroke="rgba(0,255,136,0.06)" strokeDasharray="4,4"/>))}
      <polygon fill="url(#eff-grad)" points={area} opacity={0.6}/>
      <polyline fill="none" stroke="#00ff88" strokeWidth="2" points={polyline} strokeLinejoin="round"/>
      {points.map(p=>(<circle key={p.active_houses} cx={scaleX(Number(p.active_houses))} cy={scaleY(Number(p.avg_efficiency))} r={4} fill="#00ff88" stroke="#001a0e" strokeWidth={2}/>))}
      {points.map(p=>(<text key={`lx-${p.active_houses}`} x={scaleX(Number(p.active_houses))} y={H-8} textAnchor="middle" fontSize={9} fill="#3d5c50" fontFamily="monospace">{p.active_houses}</text>))}
      <text x={10} y={pad-6} fontSize={9} fill="#3d5c50" fontFamily="monospace">EFF%</text>
      <text x={W/2} y={H-2} textAnchor="middle" fontSize={9} fill="#3d5c50" fontFamily="monospace">HOUSES</text>
    </svg>
  );
}

function SegmentedBar({ segments, total }) {
  if(!total) return null;
  return (
    <div style={{ display:"flex", gap:2, height:8, borderRadius:4, overflow:"hidden" }}>
      {segments.map((seg,i)=>(
        <div key={i} style={{ flex:seg.value, background:seg.color, opacity:0.85 }} title={`${seg.label}: ${seg.value}`}/>
      ))}
    </div>
  );
}

/*  MAIN APP  */
export default function App() {
  const [state,setState]=useState(null);
  const [selectedHouse,setSelectedHouse]=useState(null);
  const [cells,setCells]=useState([]);
  const [suburbCells,setSuburbCells]=useState([]);
  const [suburbTerrain,setSuburbTerrain]=useState([]);
  const [error,setError]=useState("");
  const [targetHouses,setTargetHouses]=useState(3);
  const [targetRobots,setTargetRobots]=useState(9);
  const [activeSection,setActiveSection]=useState("overview");
  const [showRoleOverlay,setShowRoleOverlay]=useState(true);
  const [curve,setCurve]=useState([]);
  const [matrix,setMatrix]=useState([]);
  const [soilLibrary,setSoilLibrary]=useState([]);
  const [siteProbes,setSiteProbes]=useState([]);
  const [maintenanceAlerts,setMaintenanceAlerts]=useState([]);
  const [isMutating,setIsMutating]=useState(false);

  useEffect(()=>{
    let isMounted=true, wsConnected=false, wsDisposed=false;
    const fetchState=async()=>{
      try {
        const [stateResp,curveResp,matrixResp,soilResp,maintenanceResp]=await Promise.all([
          fetch(`${API_BASE}/api/state`),fetch(`${API_BASE}/api/metrics/curve`),
          fetch(`${API_BASE}/api/metrics/matrix`),fetch(`${API_BASE}/api/soil-library`),
          fetch(`${API_BASE}/api/maintenance-alerts`)
        ]);
        const [data,curveData,matrixData,soilData,maintenanceData]=await Promise.all([stateResp.json(),curveResp.json(),matrixResp.json(),soilResp.json(),maintenanceResp.json()]);
        if(!isMounted)return;
        setState(data); setCurve(curveData); setMatrix(matrixData);
        setSoilLibrary(Array.isArray(soilData)?soilData:[]);
        setMaintenanceAlerts(Array.isArray(maintenanceData)?maintenanceData:[]);
        setTargetHouses(data.houses?.length||0); setTargetRobots(data.robots?.length||0);
        setSelectedHouse(prev=>{ const has=data.houses?.some(h=>h.id===prev); return prev&&has?prev:(data.houses?.[0]?.id??null); });
        if(!wsConnected)setError("");
      } catch(e) { if(!wsConnected&&isMounted)setError(e.message); }
    };
    fetchState();
    const pollId=setInterval(fetchState,5000);
    const ws=new WebSocket(API_BASE.replace(/^http/,"ws")+"/ws");
    ws.onopen=()=>{ if(wsDisposed){ws.close(); return;} wsConnected=true; if(isMounted)setError(""); };
    ws.onmessage=(e)=>{ if(!isMounted||wsDisposed)return; const p=JSON.parse(e.data); if(p.type==="state")setState(p.data); if(p.type==="error")setError(p.data.message); };
    ws.onerror=()=>{ if(!isMounted||wsDisposed)return; wsConnected=false; setError("WebSocket disconnected (polling fallback active)"); };
    ws.onclose=()=>{ if(!isMounted||wsDisposed)return; wsConnected=false; setError("WebSocket disconnected (polling fallback active)"); };
    return ()=>{ wsDisposed=true; isMounted=false; clearInterval(pollId); if(ws.readyState===WebSocket.OPEN){ ws.close(); } };
  },[]);

  useEffect(()=>{
    if(!selectedHouse)return;
    Promise.all([fetch(`${API_BASE}/api/houses/${selectedHouse}/grid`),fetch(`${API_BASE}/api/site-heatmap/${selectedHouse}`)])
      .then(async([g,h])=>{ const[gd,hd]=await Promise.all([g.json(),h.json()]); setCells(Array.isArray(gd)?gd:[]); setSiteProbes(Array.isArray(hd)?hd:[]); })
      .catch(e=>setError(e.message));
  },[selectedHouse,state?.metrics?.sampled_at]);

  useEffect(()=>{
    const count=state?.houses?.length??0;
    if(!count){setSuburbCells([]);setSuburbTerrain([]);return;}
    Promise.all([fetch(`${API_BASE}/api/suburb/grid?max_houses=${count}`),fetch(`${API_BASE}/api/suburb/terrain?max_houses=${count}`)])
      .then(async([g,t])=>{ const[gd,td]=await Promise.all([g.json(),t.json()]); setSuburbCells(gd); setSuburbTerrain(td); })
      .catch(e=>setError(e.message));
  },[state?.houses?.length,state?.metrics?.sampled_at]);

  const refreshCharts=async()=>{
    const[c,m,s,ma]=await Promise.all([fetch(`${API_BASE}/api/metrics/curve`),fetch(`${API_BASE}/api/metrics/matrix`),fetch(`${API_BASE}/api/soil-library`),fetch(`${API_BASE}/api/maintenance-alerts`)]);
    setCurve(await c.json()); setMatrix(await m.json()); setSoilLibrary(await s.json()); setMaintenanceAlerts(await ma.json());
  };

  const applyHouses=async(v)=>{ setIsMutating(true); try { const r=await fetch(`${API_BASE}/api/pipeline/target`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({target_houses:Number(v)})}); const p=await r.json(); if(!r.ok)throw new Error(p?.error); setState(p.state); setTargetHouses(p.state?.houses?.length||Number(v)); await refreshCharts(); } catch(e){setError(e.message);} finally{setIsMutating(false);} };
  const applyRobots=async(v)=>{ setIsMutating(true); try { const r=await fetch(`${API_BASE}/api/robots/target`,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({target_robots:Number(v)})}); const p=await r.json(); if(!r.ok)throw new Error(p?.error); setState(p.state); setTargetRobots(p.state?.robots?.length||Number(v)); await refreshCharts(); } catch(e){setError(e.message);} finally{setIsMutating(false);} };
  const resetExperiment=async()=>{
    setIsMutating(true);
    try {
      const r=await fetch(`${API_BASE}/api/experiment/reset`,{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({target_houses:targetHouses,target_robots:targetRobots})
      });
      const p=await r.json();
      if(!r.ok)throw new Error(p?.error||"Failed to reset experiment");
      setState(p.state);
      setTargetHouses(p.state?.houses?.length||Number(targetHouses));
      setTargetRobots(p.state?.robots?.length||Number(targetRobots));
      setSelectedHouse(p.state?.houses?.[0]?.id??null);
      await refreshCharts();
      setError("");
    } catch(e){
      setError(e?.message||"Reset failed.");
    } finally{
      setIsMutating(false);
    }
  };

  const metrics=state?.metrics;
  const houses=state?.houses??[];
  const robots=state?.robots??[];
  const laneSummary=state?.laneSummary??[];
  const causalGlobal=state?.causalGlobal??{};

  const houseStageMap=useMemo(()=>houses.reduce((acc,h)=>{acc[h.id]=h.stage;return acc;},{}),[houses]);
  const robotRoleById=useMemo(()=>{
    const map={};
    robots.forEach(r=>{
      const stage=r.active_house_id?houseStageMap[r.active_house_id]:null;
      if(stage==="site_prep")map[r.id]="excavator";
      else if(stage==="surveying")map[r.id]="survey";
      else if(stage==="sealing")map[r.id]="sealer";
      else if(stage&&["foundation","framing","mep","finishing"].includes(stage))map[r.id]="assembly";
      else if(r.status==="waiting_component")map[r.id]="logistics";
      else if(r.status==="placing")map[r.id]="assembly";
      else if(r.status==="moving")map[r.id]="logistics";
      else if(r.id%3===0)map[r.id]="verification";
      else if(r.id%3===1)map[r.id]="fabricator";
      else map[r.id]="logistics";
    });
    return map;
  },[robots,houseStageMap]);

  const robotStatusBreakdown=useMemo(()=>{
    const counts={idle:0,moving:0,placing:0,waiting_component:0,other:0};
    robots.forEach(r=>Object.hasOwn(counts,r.status)?counts[r.status]++:counts.other++);
    const total=Math.max(1,robots.length);
    return { total, idlePct:(counts.idle/total)*100, movingPct:(counts.moving/total)*100, placingPct:(counts.placing/total)*100, waitingPct:(counts.waiting_component/total)*100, counts };
  },[robots]);

  const houseStatus=useMemo(()=>{ if(!state?.cellCounts)return {}; return state.cellCounts.reduce((acc,row)=>{ acc[row.house_id]??={empty:0,reserved:0,filled:0}; acc[row.house_id][row.status]=Number(row.count); return acc; },{}); },[state]);
  const maintenanceByHouse=useMemo(()=>{ const rows=state?.maintenance??[]; return rows.reduce((acc,row)=>{ acc[row.house_id]=row; return acc; },{}); },[state]);
  const surveySummaryByHouse=useMemo(()=>{ const rows=state?.surveySummary??[]; return rows.reduce((acc,row)=>{ acc[row.house_id]=row; return acc; },{}); },[state]);
  const terrainStatus=useMemo(()=>{ if(!state?.terrainCounts)return {}; return state.terrainCounts.reduce((acc,row)=>{ acc[row.house_id]??={total:0,ready:0,avg_grade_error:0,avg_compaction:0}; const count=Number(row.count); acc[row.house_id].total+=count; if(row.status==="ready")acc[row.house_id].ready+=count; acc[row.house_id].avg_grade_error=Number(row.avg_grade_error??0); acc[row.house_id].avg_compaction=Number(row.avg_compaction??0); return acc; },{}); },[state]);
  const terrainObstacleMap=useMemo(()=>{ if(!state?.terrainObstacles)return {}; return state.terrainObstacles.reduce((acc,row)=>{ acc[row.house_id]=Number(row.obstacles_remaining??0); return acc; },{}); },[state]);
  const assemblyStatus=useMemo(()=>{ if(!state?.assemblyCounts)return {}; return state.assemblyCounts.reduce((acc,row)=>{ acc[row.house_id]??={total:0,activated:0,failed:0,in_progress:0}; const count=Number(row.count??0); acc[row.house_id].total+=count; if(row.status==="activated")acc[row.house_id].activated+=count; else if(row.status==="failed")acc[row.house_id].failed+=count; else acc[row.house_id].in_progress+=count; return acc; },{}); },[state]);
  const terrainOverall=useMemo(()=>{ const rows=state?.terrainCounts??[]; return rows.reduce((acc,row)=>{ const count=Number(row.count??0); acc.total+=count; if(row.status==="ready")acc.ready+=count; return acc; },{total:0,ready:0}); },[state]);

  const laneAggregate=useMemo(()=>{
    const t=laneSummary.reduce((acc,row)=>{ const s=Number(row.segments??0); acc.segments+=s; acc.conditioned+=Number(row.conditioned_segments??0); acc.degraded+=Number(row.degraded_segments??0); acc.stale+=Number(row.stale_segments??0); acc.verificationEvents+=Number(row.verification_events??0); acc.weightedCondition+=Number(row.avg_condition??0)*s; return acc; },{segments:0,conditioned:0,degraded:0,stale:0,verificationEvents:0,weightedCondition:0});
    return { ...t, conditionedPct:t.segments>0?(t.conditioned/t.segments)*100:0, avgCondition:t.segments>0?t.weightedCondition/t.segments:0 };
  },[laneSummary]);

  const latestMatrixPoint=matrix.length?matrix[matrix.length-1]:null;
  const lifetimeQcTotal=Number(metrics?.blocks_verified??0)+Number(metrics?.blocks_failed_qc??0);

  const bottleneck=useMemo(()=>{
    if(!robots.length)return {label:"NO ACTIVE ROBOTS",reason:"No robots available.",color:"#ff4455"};
    const terrainReadyPct=terrainOverall.total>0?(terrainOverall.ready/terrainOverall.total)*100:100;
    if(robotStatusBreakdown.waitingPct>=30)return {label:"FABRICATION BOTTLENECK",reason:`${robotStatusBreakdown.waitingPct.toFixed(1)}% waiting for components`,color:"#f4d35e"};
    if(houses.some(h=>h.stage==="site_prep")&&terrainReadyPct<85)return {label:"SITE PREP GATE",reason:`Terrain ${terrainReadyPct.toFixed(1)}% ready`,color:"#c28f5c"};
    if(Number(metrics?.active_surveys??0)>0)return {label:"SURVEY GATE",reason:`${Number(metrics?.active_surveys??0)} houses surveying`,color:"#56b4ff"};
    if(robotStatusBreakdown.movingPct>=30)return {label:"LOGISTICS LIMITED",reason:`${robotStatusBreakdown.movingPct.toFixed(1)}% in transit`,color:"#f4978e"};
    if(robotStatusBreakdown.placingPct>=55&&robotStatusBreakdown.idlePct<15)return {label:"ASSEMBLY SATURATED",reason:"Peak placement capacity",color:"#3ddc97"};
    return {label:"BALANCED FLOW",reason:"No dominant constraint",color:"#00ff88"};
  },[robots.length,robotStatusBreakdown,terrainOverall,houses,metrics]);

  const sections=["overview","pipeline","analysis","robots","soil","maintenance","3d-view"];

  /*  CSS-IN-JS (injected once)  */
  useEffect(()=>{
    const style=document.createElement("style");
    style.textContent=`
      @import url('https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Inter:wght@300;400;500;600&display=swap');
      *,*::before,*::after{box-sizing:border-box;margin:0;padding:0;}
      html,body{background:#020c08;color:#c4e8d4;font-family:'Inter',sans-serif;min-height:100vh;overflow-x:hidden;}
      @keyframes pulse{0%,100%{opacity:1}50%{opacity:0.3}}
      @keyframes scanline{0%{transform:translateY(-100%)}100%{transform:translateY(100vh)}}
      @keyframes blink{0%,100%{opacity:1}49%{opacity:1}50%{opacity:0}}
      @keyframes fadeIn{from{opacity:0;transform:translateY(8px)}to{opacity:1;transform:translateY(0)}}
      ::-webkit-scrollbar{width:4px;height:4px}
      ::-webkit-scrollbar-track{background:rgba(0,255,136,0.03)}
      ::-webkit-scrollbar-thumb{background:rgba(0,255,136,0.2);border-radius:2px}
      button{cursor:pointer;font-family:inherit}
      input[type=range]{accent-color:#00ff88}
    `;
    document.head.appendChild(style);
    return()=>document.head.removeChild(style);
  },[]);

  const mono={fontFamily:"'Space Mono','Courier New',monospace"};

  return (
    <div style={{ minHeight:"100vh", background:"#020c08", position:"relative", overflowX:"hidden" }}>
      {/* Scanline overlay */}
      <div style={{ position:"fixed", top:0,left:0,right:0,bottom:0, background:"repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,255,136,0.015) 2px,rgba(0,255,136,0.015) 4px)", pointerEvents:"none", zIndex:9999 }}/>
      {/* Radial glow */}
      <div style={{ position:"fixed", top:"-20%", left:"50%", transform:"translateX(-50%)", width:800, height:500, background:"radial-gradient(ellipse,rgba(0,255,136,0.04) 0%,transparent 70%)", pointerEvents:"none", zIndex:0 }}/>

      {/*  HEADER  */}
      <header style={{ position:"sticky", top:0, zIndex:100, background:"rgba(2,12,8,0.95)", backdropFilter:"blur(12px)", borderBottom:"1px solid rgba(0,255,136,0.12)", padding:"0 24px" }}>
        <div style={{ display:"flex", alignItems:"center", gap:20, height:56 }}>
          {/* Logo mark */}
          <div style={{ display:"flex", alignItems:"center", gap:10 }}>
            <div style={{ width:28, height:28, border:"2px solid #00ff88", borderRadius:4, display:"grid", placeItems:"center", position:"relative" }}>
              <div style={{ width:10, height:10, background:"#00ff88", borderRadius:2 }}/>
              <div style={{ position:"absolute", top:-4, right:-4, width:6, height:6, border:"1px solid #00ff88", borderRadius:"50%", background:"rgba(0,255,136,0.3)", animation:"pulse 2s infinite" }}/>
            </div>
            <div>
              <div style={{ ...mono, fontSize:"0.9rem", fontWeight:700, color:"#00ff88", letterSpacing:"0.1em", lineHeight:1 }}>HOUSE BRAIN</div>
              <div style={{ ...mono, fontSize:"0.55rem", color:"#2a5c40", letterSpacing:"0.2em" }}>MISSION CONTROL v2.0</div>
            </div>
          </div>

          {/* Nav */}
          <nav style={{ display:"flex", gap:4, marginLeft:8, flex:1 }}>
            {sections.map(s=>(
              <button key={s} onClick={()=>setActiveSection(s)} style={{ ...mono, fontSize:"0.65rem", letterSpacing:"0.1em", padding:"5px 12px", borderRadius:4, border:`1px solid ${activeSection===s?"rgba(0,255,136,0.5)":"rgba(0,255,136,0.1)"}`, background:activeSection===s?"rgba(0,255,136,0.08)":"transparent", color:activeSection===s?"#00ff88":"#3a6b50", textTransform:"uppercase", transition:"all 0.2s" }}>
                {s}
              </button>
            ))}
          </nav>

          {/* Status bar */}
          <div style={{ display:"flex", alignItems:"center", gap:16 }}>
            {error&&<div style={{ ...mono, fontSize:"0.65rem", color:"#ff4455", background:"rgba(255,68,85,0.1)", border:"1px solid rgba(255,68,85,0.3)", borderRadius:4, padding:"3px 8px" }}> {error}</div>}
            <div style={{ display:"flex", gap:10, ...mono, fontSize:"0.68rem", color:"#3a6b50" }}>
              <span style={{ color:"#00ff88" }}>{houses.length}</span> HOUSES
              <span style={{ color:"#56b4ff" }}>{robots.length}</span> ROBOTS
            </div>
            <div style={{ ...mono, fontSize:"0.65rem", color:"#3a6b50", display:"flex", alignItems:"center", gap:6 }}>
              <span style={{ width:6, height:6, borderRadius:"50%", background:"#00ff88", display:"inline-block", animation:"pulse 2s infinite" }}/>
              LIVE
            </div>
          </div>
        </div>
      </header>

      <main style={{ padding:"20px 24px", maxWidth:1800, margin:"0 auto", position:"relative", zIndex:1 }}>

        {/*  OVERVIEW  */}
        {activeSection==="overview"&&(
          <div style={{ animation:"fadeIn 0.3s ease" }}>
            {/* Constraint banner */}
            <div style={{ background:`rgba(${bottleneck.color==="#00ff88"?"0,255,136":"255,68,85"},0.05)`, border:`1px solid ${bottleneck.color}33`, borderRadius:8, padding:"12px 18px", marginBottom:16, display:"flex", alignItems:"center", gap:16 }}>
              <div style={{ width:8, height:8, borderRadius:"50%", background:bottleneck.color, boxShadow:`0 0 12px ${bottleneck.color}`, flexShrink:0, animation:"pulse 1.5s infinite" }}/>
              <div>
                <div style={{ ...mono, fontSize:"0.75rem", letterSpacing:"0.12em", color:bottleneck.color }}>{bottleneck.label}</div>
                <div style={{ ...mono, fontSize:"0.65rem", color:"#3a6b50", marginTop:2 }}>{bottleneck.reason}</div>
              </div>
              <div style={{ marginLeft:"auto", display:"flex", gap:20, ...mono, fontSize:"0.68rem", color:"#3a6b50" }}>
                <span>WAIT <span style={{ color:"#f4d35e" }}>{robotStatusBreakdown.waitingPct.toFixed(1)}%</span></span>
                <span>MOVE <span style={{ color:"#7aa2ff" }}>{robotStatusBreakdown.movingPct.toFixed(1)}%</span></span>
                <span>PLACE <span style={{ color:"#3ddc97" }}>{robotStatusBreakdown.placingPct.toFixed(1)}%</span></span>
                <span>IDLE <span style={{ color:"#7f8ea3" }}>{robotStatusBreakdown.idlePct.toFixed(1)}%</span></span>
              </div>
            </div>

            {/* Primary KPI grid */}
            <div style={{ display:"grid", gridTemplateColumns:"repeat(auto-fill,minmax(160px,1fr))", gap:8, marginBottom:16 }}>
              <StatCard label="Efficiency" value={`${Number(metrics?.pipeline_efficiency??0).toFixed(1)}%`} sub="latest sample" pulse/>
              <StatCard label="Throughput" value={`${Number(metrics?.throughput_cells_per_hour??0).toFixed(1)}`} sub="cells / hr"/>
              <StatCard label="Cells Filled" value={`${metrics?.cells_filled??0}`} sub={`of ${metrics?.total_cells??0} total`}/>
              <StatCard label="Robot Idle" value={`${Number(metrics?.robot_idle_percent??0).toFixed(1)}%`} sub="latest sample" accent={Number(metrics?.robot_idle_percent??0)>50?"rgba(255,68,85,0.4)":undefined}/>
              <StatCard label="Matrix Eff" value={`${Number(latestMatrixPoint?.avg_efficiency??0).toFixed(1)}%`} sub="aggregate window"/>
              <StatCard label="Terrain Ready" value={`${terrainOverall.ready}`} sub={`of ${terrainOverall.total}`}/>
              <StatCard label="Obstacles" value={`${Number(metrics?.obstacle_cells_remaining??0)}`} accent={Number(metrics?.obstacle_cells_remaining??0)>0?"rgba(244,211,94,0.3)":undefined}/>
              <StatCard label="QC Passed" value={`${Number(metrics?.blocks_verified??0)}`} sub={`${Number(metrics?.blocks_failed_qc??0)} failed`}/>
              <StatCard label="Avg TTL" value={`${Number(metrics?.avg_ttl_days??0).toFixed(0)}d`} sub="coating lifetime"/>
              <StatCard label="Lane Cond." value={`${Number(metrics?.avg_lane_condition??0).toFixed(2)}`} sub={`${Number(metrics?.conditioned_lane_percent??0).toFixed(0)}% conditioned`}/>
              <StatCard label="Soil Recipes" value={`${Number(metrics?.soil_recipes_learned??0)}`} sub="learned"/>
              <StatCard label="Active Surveys" value={`${Number(metrics?.active_surveys??0)}`} accent={Number(metrics?.active_surveys??0)>0?"rgba(86,180,255,0.3)":undefined}/>
              <StatCard label="Kits Activated" value={`${Number(metrics?.community_kits_activated??0)}`}/>
              <StatCard label="Maint. Alerts" value={`${Number(metrics?.houses_in_maintenance??0)}`} accent={Number(metrics?.houses_in_maintenance??0)>0?"rgba(255,68,85,0.4)":undefined}/>
              <StatCard label="Stale Relays" value={`${Number(metrics?.stale_relay_segments??0)}`} accent={Number(metrics?.stale_relay_segments??0)>5?"rgba(244,211,94,0.3)":undefined}/>
              <StatCard label="Ref Patches" value={`${Number(metrics?.reference_patches_protected??0)}/${Number(metrics?.reference_patches_total??0)}`}/>
            </div>

            {/* Causal chain strip */}
            <div style={{ background:"rgba(0,0,0,0.4)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:8, padding:"14px 18px", marginBottom:16 }}>
              <div style={{ ...mono, fontSize:"0.65rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:10 }}>// CAUSAL CHAIN OVERVIEW</div>
              <div style={{ display:"grid", gridTemplateColumns:"repeat(6,1fr)", gap:8 }}>
                {[
                  {k:"Survey Approval",v:`${Number(causalGlobal.approved_sites??0)}/${Number(causalGlobal.surveyed_houses??0)}`},
                  {k:"QC Pass Rate",v:Number(causalGlobal.inline_total??0)>0?`${((Number(causalGlobal.inline_passed??0)/Number(causalGlobal.inline_total??1))*100).toFixed(1)}%`:`${lifetimeQcTotal>0?((Number(metrics?.blocks_verified??0)/lifetimeQcTotal)*100).toFixed(1):0}%`},
                  {k:"Confidence S/L",v:`${Number(causalGlobal.avg_short_confidence??0).toFixed(2)} / ${Number(causalGlobal.avg_long_confidence??0).toFixed(2)}`},
                  {k:"TTL Spread",v:`${Number(causalGlobal.min_ttl_days??0)}${Number(causalGlobal.max_ttl_days??0)}d`},
                  {k:"Moisture Risk",v:`${Number(causalGlobal.unsealed_moisture_risk??0).toFixed(2)}  ${Number(causalGlobal.sealed_moisture_risk??0).toFixed(2)}`},
                  {k:"QC Retry Rate",v:Number(causalGlobal.inline_avg_retries??0).toFixed(2)},
                ].map(item=>(
                  <div key={item.k} style={{ background:"rgba(0,255,136,0.03)", border:"1px solid rgba(0,255,136,0.08)", borderRadius:6, padding:"8px 10px" }}>
                    <div style={{ ...mono, fontSize:"0.62rem", color:"#2a5c40", marginBottom:4, letterSpacing:"0.06em" }}>{item.k}</div>
                    <div style={{ ...mono, fontSize:"0.88rem", color:"#00ff88" }}>{item.v}</div>
                  </div>
                ))}
              </div>
            </div>

            {/* Controls */}
            <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:12, marginBottom:16 }}>
              {[{label:"PIPELINE TARGETS (HOUSES)",opts:HOUSE_TARGET_OPTIONS,value:targetHouses,apply:applyHouses,set:setTargetHouses,min:1,max:40},{label:"ROBOT FLEET SIZE",opts:ROBOT_TARGET_OPTIONS,value:targetRobots,apply:applyRobots,set:setTargetRobots,min:3,max:60}].map(ctrl=>(
                <div key={ctrl.label} style={{ background:"rgba(0,0,0,0.4)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:8, padding:"14px 16px" }}>
                  <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:10 }}>// {ctrl.label}</div>
                  <div style={{ display:"flex", gap:6, flexWrap:"wrap", marginBottom:10 }}>
                    {ctrl.opts.map(n=>(
                      <button key={n} onClick={()=>ctrl.apply(n)} disabled={isMutating} style={{ ...mono, fontSize:"0.72rem", padding:"5px 14px", borderRadius:4, border:`1px solid ${ctrl.value===n?"#00ff88":"rgba(0,255,136,0.2)"}`, background:ctrl.value===n?"rgba(0,255,136,0.1)":"transparent", color:ctrl.value===n?"#00ff88":"#3a6b50", transition:"all 0.15s" }}>
                        {n}
                      </button>
                    ))}
                  </div>
                  <div style={{ display:"flex", alignItems:"center", gap:12 }}>
                    <input type="range" min={ctrl.min} max={ctrl.max} value={ctrl.value} onChange={e=>ctrl.set(Number(e.target.value))} onMouseUp={e=>ctrl.apply(Number(e.currentTarget.value))} style={{ flex:1, accentColor:"#00ff88" }}/>
                    <span style={{ ...mono, fontSize:"0.9rem", color:"#00ff88", minWidth:28, textAlign:"right" }}>{ctrl.value}</span>
                  </div>
                </div>
              ))}
            </div>

            <div style={{ display:"flex", gap:8, marginBottom:16 }}>
              <button onClick={resetExperiment} disabled={isMutating} style={{ ...mono, fontSize:"0.72rem", letterSpacing:"0.1em", padding:"9px 20px", borderRadius:5, border:"1px solid rgba(0,255,136,0.4)", background:"rgba(0,255,136,0.06)", color:"#00ff88" }}>
                {isMutating?" EXECUTING...":" RESET EXPERIMENT WINDOW"}
              </button>
            </div>

            {/* Efficiency curve */}
            <div style={{ background:"rgba(0,0,0,0.4)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:8, padding:"14px 16px" }}>
              <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:10 }}>// EFFICIENCY CURVE  HOUSES VS PIPELINE EFFICIENCY</div>
              <CurveChart points={curve}/>
            </div>
          </div>
        )}

        {/*  PIPELINE (kanban board)  */}
        {activeSection==="pipeline"&&(
          <div style={{ animation:"fadeIn 0.3s ease" }}>
            <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:12 }}>// PIPELINE STATUS  {houses.length} ACTIVE HOUSES</div>
            <div style={{ display:"grid", gridTemplateColumns:"repeat(9,1fr)", gap:6, overflowX:"auto", paddingBottom:8 }}>
              {stageOrder.map(stage=>{
                const stageHouses=houses.filter(h=>h.stage===stage);
                const stageColor=stage==="complete"?"#00ff88":stage==="sealing"?"#66d9ef":stage==="community_assembly"?"#3ddc97":"rgba(0,255,136,0.6)";
                return (
                  <div key={stage}>
                    <div style={{ ...mono, fontSize:"0.6rem", letterSpacing:"0.1em", color:"#2a5c40", marginBottom:6, padding:"4px 6px", borderBottom:"1px solid rgba(0,255,136,0.1)", display:"flex", justifyContent:"space-between" }}>
                      <span style={{ color:stageHouses.length?stageColor:"#1a3d28" }}>{stageLabels[stage]?.toUpperCase()}</span>
                      <span style={{ color:stageColor }}>{stageHouses.length}</span>
                    </div>
                    {stageHouses.map(house=>{
                      const c=houseStatus[house.id]||{empty:0,reserved:0,filled:0};
                      const total=c.empty+c.reserved+c.filled||1;
                      const fillPct=(c.filled/total)*100;
                      const terrain=terrainStatus[house.id]||{total:0,ready:0};
                      const terrainPct=terrain.total>0?(terrain.ready/terrain.total)*100:0;
                      const sealingPct=Number(house.sealing_progress??0)*100;
                      const survey=surveySummaryByHouse[house.id]||{};
                      const surveyPct=Number(survey.probes??0)>0?(Number(survey.buildable??0)/Number(survey.probes))*100:0;
                      const kits=assemblyStatus[house.id]||{total:0,activated:0};
                      const kitPct=kits.total>0?(kits.activated/kits.total)*100:0;
                      const pct=stage==="site_prep"?terrainPct:stage==="surveying"?surveyPct:stage==="sealing"?sealingPct:stage==="community_assembly"?kitPct:fillPct;
                      const isSelected=selectedHouse===house.id;
                      return (
                        <button key={house.id} onClick={()=>setSelectedHouse(house.id)} style={{ width:"100%", textAlign:"left", background:isSelected?"rgba(0,255,136,0.07)":"rgba(0,0,0,0.35)", border:`1px solid ${isSelected?"rgba(0,255,136,0.4)":"rgba(0,255,136,0.1)"}`, borderRadius:6, padding:"8px 8px 6px", marginBottom:5, display:"grid", gap:4 }}>
                          <div style={{ ...mono, fontSize:"0.7rem", color:isSelected?"#00ff88":"#7ab898" }}>H{house.id}</div>
                          <ScanlineBar pct={pct} color={stageColor} height={4}/>
                          <div style={{ ...mono, fontSize:"0.58rem", color:"#2a5c40" }}>{pct.toFixed(0)}%</div>
                          {house.soil_signature&&<div style={{ ...mono, fontSize:"0.5rem", color:"#1a3d28", overflow:"hidden", textOverflow:"ellipsis", whiteSpace:"nowrap" }}>{house.soil_signature}</div>}
                        </button>
                      );
                    })}
                  </div>
                );
              })}
            </div>

            {/* Selected house detail */}
            {selectedHouse&&(()=>{
              const house=houses.find(h=>h.id===selectedHouse);
              if(!house)return null;
              const c=houseStatus[house.id]||{empty:0,reserved:0,filled:0};
              const terrain=terrainStatus[house.id]||{total:0,ready:0,avg_grade_error:0,avg_compaction:0};
              const survey=surveySummaryByHouse[house.id]||{};
              const kits=assemblyStatus[house.id]||{total:0,activated:0,failed:0};
              const maintenance=maintenanceByHouse[house.id];
              const obstaclesLeft=terrainObstacleMap[house.id]??0;
              return (
                <div style={{ marginTop:12, background:"rgba(0,255,136,0.03)", border:"1px solid rgba(0,255,136,0.2)", borderRadius:8, padding:"14px 16px" }}>
                  <div style={{ ...mono, fontSize:"0.65rem", letterSpacing:"0.12em", color:"#00ff88", marginBottom:12 }}>// {house.name}  STAGE: {house.stage?.toUpperCase()}</div>
                  <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:12 }}>
                    <div>
                      <div style={{ ...mono, fontSize:"0.6rem", color:"#2a5c40", marginBottom:8 }}>GRID STATUS</div>
                      <SegmentedBar segments={[{value:c.filled,color:"#00ff88",label:"filled"},{value:c.reserved,color:"#f4d35e",label:"reserved"},{value:c.empty,color:"#1a3d28",label:"empty"}]} total={c.filled+c.reserved+c.empty}/>
                      <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr 1fr", gap:6, marginTop:8 }}>
                        {[{l:"Filled",v:c.filled,c:"#00ff88"},{l:"Reserved",v:c.reserved,c:"#f4d35e"},{l:"Empty",v:c.empty,c:"#3a6b50"}].map(i=>(<div key={i.l}><div style={{ ...mono, fontSize:"0.58rem", color:"#2a5c40" }}>{i.l}</div><div style={{ ...mono, fontSize:"0.82rem", color:i.c }}>{i.v}</div></div>))}
                      </div>
                    </div>
                    <div>
                      <div style={{ ...mono, fontSize:"0.6rem", color:"#2a5c40", marginBottom:8 }}>SURVEY</div>
                      {[{l:"Status",v:house.survey_status??"-"},{l:"Zone",v:house.site_zone??"-"},{l:"Probes",v:survey.probes??0},{l:"Buildable",v:survey.buildable??0},{l:"Reject",v:survey.reject??0},{l:"Uncertainty",v:Number(house.survey_uncertainty_remaining??0).toFixed(2)}].map(i=>(<div key={i.l} style={{ display:"flex", justifyContent:"space-between", ...mono, fontSize:"0.68rem", color:"#7ab898", padding:"2px 0", borderBottom:"1px solid rgba(0,255,136,0.05)" }}><span style={{ color:"#2a5c40" }}>{i.l}</span>{i.v}</div>))}
                    </div>
                    <div>
                      <div style={{ ...mono, fontSize:"0.6rem", color:"#2a5c40", marginBottom:8 }}>TERRAIN / MAINTENANCE</div>
                      {[{l:"Terrain Ready",v:`${terrain.ready}/${terrain.total}`},{l:"Obstacles",v:obstaclesLeft},{l:"Grade Err",v:Number(terrain.avg_grade_error??0).toFixed(3)},{l:"Compaction",v:Number(terrain.avg_compaction??0).toFixed(3)},{l:"Sealing",v:house.sealing_complete?"COMPLETE":`${(Number(house.sealing_progress??0)*100).toFixed(0)}%`},{l:"TTL",v:maintenance?.ttl_days?`${maintenance.ttl_days}d`:"-"}].map(i=>(<div key={i.l} style={{ display:"flex", justifyContent:"space-between", ...mono, fontSize:"0.68rem", color:"#7ab898", padding:"2px 0", borderBottom:"1px solid rgba(0,255,136,0.05)" }}><span style={{ color:"#2a5c40" }}>{i.l}</span>{i.v}</div>))}
                    </div>
                  </div>
                  <div style={{ marginTop:12 }}>
                    <div style={{ ...mono, fontSize:"0.6rem", color:"#2a5c40", marginBottom:6 }}>SITE HEATMAP (PROBES)</div>
                    <SiteHeatmap probes={siteProbes}/>
                  </div>
                </div>
              );
            })()}
          </div>
        )}

        {/*  ANALYSIS  */}
        {activeSection==="analysis"&&(
          <div style={{ animation:"fadeIn 0.3s ease" }}>
            <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:12 }}>// EXPERIMENT MATRIX (HOUSES  ROBOTS)</div>
            <div style={{ overflowX:"auto" }}>
              <table style={{ width:"100%", borderCollapse:"collapse", ...mono, fontSize:"0.68rem" }}>
                <thead>
                  <tr style={{ borderBottom:"1px solid rgba(0,255,136,0.2)" }}>
                    {["Houses","Robots","Efficiency","Idle","Throughput","Terrain%","Obstacles","Grade Err","Compact","Kits","Surveys","Soils","QC Pass","QC Fail","Maint","TTL","Lane","Lane%","Degr","Stale","Verif","Ref","Retries","Fails","Samples"].map(h=>(
                      <th key={h} style={{ padding:"7px 10px", color:"#2a5c40", textAlign:"left", letterSpacing:"0.06em", whiteSpace:"nowrap" }}>{h}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {matrix.slice(-20).map((row,i)=>(
                    <tr key={`${row.active_houses}-${row.active_robots}`} style={{ borderBottom:"1px solid rgba(0,255,136,0.05)", background:i%2===0?"rgba(0,255,136,0.01)":"transparent" }}>
                      {[row.active_houses,row.active_robots,`${Number(row.avg_efficiency).toFixed(1)}%`,`${Number(row.avg_idle).toFixed(1)}%`,Number(row.avg_throughput).toFixed(1),`${Number(row.avg_terrain_ready??0).toFixed(1)}%`,Number(row.avg_obstacles_remaining??0).toFixed(1),Number(row.avg_grade_error??0).toFixed(3),Number(row.avg_compaction??0).toFixed(3),Number(row.avg_kits_activated??0).toFixed(1),Number(row.avg_active_surveys??0).toFixed(1),Number(row.avg_soil_recipes??0).toFixed(1),Number(row.avg_blocks_verified??0).toFixed(1),Number(row.avg_blocks_failed??0).toFixed(1),Number(row.avg_maintenance_houses??0).toFixed(1),Number(row.avg_ttl_days??0).toFixed(0),Number(row.avg_lane_condition??0).toFixed(2),`${Number(row.avg_conditioned_lane_percent??0).toFixed(1)}%`,Number(row.avg_degraded_lane_segments??0).toFixed(1),Number(row.avg_stale_relay_segments??0).toFixed(1),Number(row.avg_lane_verification_events??0).toFixed(1),`${Number(row.avg_reference_patches_protected??0).toFixed(1)}/${Number(row.avg_reference_patches_total??0).toFixed(1)}`,Number(row.avg_retries??0).toFixed(2),Number(row.avg_failures??0).toFixed(2),row.samples].map((v,j)=>(
                        <td key={j} style={{ padding:"6px 10px", color:j===2?"#00ff88":j===3&&parseFloat(String(v))>50?"#ff4455":"#7ab898", whiteSpace:"nowrap" }}>{v}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/*  ROBOTS  */}
        {activeSection==="robots"&&(
          <div style={{ animation:"fadeIn 0.3s ease" }}>
            <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:12, marginBottom:12 }}>
              <div style={{ background:"rgba(0,0,0,0.4)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:8, padding:"14px 16px" }}>
                <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:10 }}>// ROBOT FLEET STATUS</div>
                <div style={{ display:"grid", gridTemplateColumns:"repeat(4,1fr)", gap:6, marginBottom:12 }}>
                  {[{l:"PLACING",v:robotStatusBreakdown.counts.placing,c:"#3ddc97"},{l:"MOVING",v:robotStatusBreakdown.counts.moving,c:"#7aa2ff"},{l:"WAITING",v:robotStatusBreakdown.counts.waiting_component,c:"#f4d35e"},{l:"IDLE",v:robotStatusBreakdown.counts.idle,c:"#7f8ea3"}].map(i=>(
                    <div key={i.l} style={{ background:"rgba(0,0,0,0.3)", border:`1px solid ${i.c}22`, borderRadius:6, padding:"8px", textAlign:"center" }}>
                      <div style={{ ...mono, fontSize:"0.6rem", color:"#2a5c40", letterSpacing:"0.1em" }}>{i.l}</div>
                      <div style={{ ...mono, fontSize:"1.3rem", color:i.c, marginTop:2 }}>{i.v}</div>
                    </div>
                  ))}
                </div>
                <div style={{ height:6, display:"flex", borderRadius:3, overflow:"hidden", gap:1 }}>
                  <div style={{ flex:robotStatusBreakdown.counts.placing, background:"#3ddc97" }}/>
                  <div style={{ flex:robotStatusBreakdown.counts.moving, background:"#7aa2ff" }}/>
                  <div style={{ flex:robotStatusBreakdown.counts.waiting_component, background:"#f4d35e" }}/>
                  <div style={{ flex:robotStatusBreakdown.counts.idle, background:"#2a3a4a" }}/>
                </div>
              </div>

              <div style={{ background:"rgba(0,0,0,0.4)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:8, padding:"14px 16px" }}>
                <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:10 }}>// ROLE DISTRIBUTION</div>
                {robotRoleCatalog.map(role=>{
                  const count=Object.values(robotRoleById).filter(r=>r===role.id).length;
                  const pct=robots.length>0?(count/robots.length)*100:0;
                  return (
                    <div key={role.id} style={{ display:"grid", gridTemplateColumns:"80px 28px 1fr 36px", alignItems:"center", gap:8, marginBottom:5 }}>
                      <div style={{ ...mono, fontSize:"0.65rem", color:"#3a6b50" }}>{role.label}</div>
                      <div style={{ width:8, height:8, borderRadius:"50%", background:roleColorMap[role.id], marginLeft:4 }}/>
                      <ScanlineBar pct={pct} color={roleColorMap[role.id]}/>
                      <div style={{ ...mono, fontSize:"0.65rem", color:roleColorMap[role.id], textAlign:"right" }}>{count}</div>
                    </div>
                  );
                })}
              </div>
            </div>

            <div style={{ background:"rgba(0,0,0,0.4)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:8, padding:"14px 16px" }}>
              <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:10 }}>// ROBOT MANIFEST ({robots.length} UNITS)</div>
              <div style={{ overflowY:"auto", maxHeight:400 }}>
                <table style={{ width:"100%", borderCollapse:"collapse", ...mono, fontSize:"0.68rem" }}>
                  <thead>
                    <tr style={{ borderBottom:"1px solid rgba(0,255,136,0.15)" }}>
                      {["ID","STATUS","ROLE","HOUSE","POS X","POS Y","POS Z"].map(h=>(<th key={h} style={{ padding:"5px 10px", color:"#2a5c40", textAlign:"left", letterSpacing:"0.08em" }}>{h}</th>))}
                    </tr>
                  </thead>
                  <tbody>
                    {robots.map(r=>{
                      const statusColor=robotColorMap[r.status]||"#7f8ea3";
                      return (
                        <tr key={r.id} style={{ borderBottom:"1px solid rgba(0,255,136,0.04)" }}>
                          <td style={{ padding:"5px 10px", color:"#3a6b50" }}>{r.id}</td>
                          <td style={{ padding:"5px 10px", color:statusColor }}>{r.status}</td>
                          <td style={{ padding:"5px 10px" }}><span style={{ color:roleColorMap[robotRoleById[r.id]]||"#3a6b50", fontSize:"0.62rem", padding:"2px 6px", border:`1px solid ${roleColorMap[robotRoleById[r.id]]||"#2a5c40"}44`, borderRadius:3 }}>{robotRoleById[r.id]??"-"}</span></td>
                          <td style={{ padding:"5px 10px", color:"#3a6b50" }}>{r.active_house_id??"-"}</td>
                          <td style={{ padding:"5px 10px", color:"#4a7c6a" }}>{r.pos_x}</td>
                          <td style={{ padding:"5px 10px", color:"#4a7c6a" }}>{r.pos_y}</td>
                          <td style={{ padding:"5px 10px", color:"#4a7c6a" }}>{r.pos_z}</td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            </div>

            <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:12, marginTop:12 }}>
              {[{title:"FABRICATOR QUEUE",data:state?.fabricatorQueue??[]},{title:"COMMUNITY ASSEMBLY QUEUE",data:state?.assemblyQueue??[]}].map(q=>(
                <div key={q.title} style={{ background:"rgba(0,0,0,0.4)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:8, padding:"14px 16px" }}>
                  <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:10 }}>// {q.title}</div>
                  {q.data.map(item=>(<div key={item.status} style={{ display:"flex", justifyContent:"space-between", ...mono, fontSize:"0.72rem", color:"#7ab898", padding:"4px 0", borderBottom:"1px solid rgba(0,255,136,0.05)" }}><span style={{ color:"#2a5c40" }}>{item.status}</span><span style={{ color:"#00ff88" }}>{item.count}</span></div>))}
                </div>
              ))}
            </div>
          </div>
        )}

        {/*  SOIL  */}
        {activeSection==="soil"&&(
          <div style={{ animation:"fadeIn 0.3s ease" }}>
            <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:12 }}>// SOIL LIBRARY  {soilLibrary.length} SIGNATURES LEARNED</div>
            <div style={{ overflowX:"auto" }}>
              <table style={{ width:"100%", borderCollapse:"collapse", ...mono, fontSize:"0.68rem" }}>
                <thead>
                  <tr style={{ borderBottom:"1px solid rgba(0,255,136,0.2)" }}>
                    {["Soil Signature","Recipe","Short Conf","Long Conf","Cycles","Erosion","Verified"].map(h=>(<th key={h} style={{ padding:"7px 10px", color:"#2a5c40", textAlign:"left", letterSpacing:"0.06em", whiteSpace:"nowrap" }}>{h}</th>))}
                  </tr>
                </thead>
                <tbody>
                  {soilLibrary.slice(0,40).map((row,i)=>{
                    const shortConf=Number(row.short_term_confidence??0);
                    const longConf=Number(row.long_term_confidence??0);
                    return (
                      <tr key={row.soil_signature} style={{ borderBottom:"1px solid rgba(0,255,136,0.04)", background:i%2===0?"rgba(0,255,136,0.01)":"transparent" }}>
                        <td style={{ padding:"7px 10px", color:"#56b4ff", fontSize:"0.62rem" }}>{row.soil_signature}</td>
                        <td style={{ padding:"7px 10px", color:"#3a6b50", fontSize:"0.6rem" }}>{row.recipe?JSON.stringify(row.recipe):"-"}</td>
                        <td style={{ padding:"7px 10px" }}>
                          <div style={{ display:"flex", alignItems:"center", gap:6 }}>
                            <div style={{ width:36, height:4, background:"rgba(0,255,136,0.1)", borderRadius:2, overflow:"hidden" }}><div style={{ width:`${shortConf*100}%`, height:"100%", background:shortConf>0.8?"#00ff88":shortConf>0.5?"#f4d35e":"#ff4455" }}/></div>
                            <span style={{ color:shortConf>0.8?"#00ff88":shortConf>0.5?"#f4d35e":"#ff4455" }}>{shortConf.toFixed(2)}</span>
                          </div>
                        </td>
                        <td style={{ padding:"7px 10px" }}>
                          <div style={{ display:"flex", alignItems:"center", gap:6 }}>
                            <div style={{ width:36, height:4, background:"rgba(0,255,136,0.1)", borderRadius:2, overflow:"hidden" }}><div style={{ width:`${longConf*100}%`, height:"100%", background:longConf>0.8?"#00ff88":longConf>0.5?"#f4d35e":"#ff4455" }}/></div>
                            <span style={{ color:longConf>0.8?"#00ff88":longConf>0.5?"#f4d35e":"#ff4455" }}>{longConf.toFixed(2)}</span>
                          </div>
                        </td>
                        <td style={{ padding:"7px 10px", color:"#4a7c6a" }}>{Number(row.weathering_cycles_tested??0)}</td>
                        <td style={{ padding:"7px 10px", color:"#4a7c6a" }}>{Number(row.erosion_score??0).toFixed(2)}</td>
                        <td style={{ padding:"7px 10px", color:"#00ff88" }}>{Number(row.total_blocks_verified??0)}</td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/*  MAINTENANCE  */}
        {activeSection==="maintenance"&&(
          <div style={{ animation:"fadeIn 0.3s ease" }}>
            <div style={{ display:"grid", gridTemplateColumns:"1fr 1fr", gap:12 }}>
              <div>
                <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:12 }}>// MAINTENANCE TIMELINE</div>
                <MaintenanceTimeline rows={maintenanceAlerts.length?maintenanceAlerts:(state?.maintenance??[])}/>
              </div>
              <div>
                <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:12 }}>// LOGISTICS LANE SUMMARY</div>
                <div style={{ display:"grid", gap:6 }}>
                  {laneSummary.length?laneSummary.map(row=>(
                    <div key={`${row.house_id}-${row.lane_id}`} style={{ background:"rgba(0,0,0,0.35)", border:"1px solid rgba(0,255,136,0.1)", borderRadius:7, padding:"10px 12px" }}>
                      <div style={{ display:"flex", justifyContent:"space-between", marginBottom:6 }}>
                        <span style={{ ...mono, fontSize:"0.7rem", color:"#7ab898" }}>House {row.house_id} / {row.lane_id}</span>
                        <span style={{ ...mono, fontSize:"0.65rem", color:Number(row.avg_condition??0)>0.7?"#00ff88":Number(row.avg_condition??0)>0.45?"#f4d35e":"#ff4455" }}>{Number(row.avg_condition??0).toFixed(2)}</span>
                      </div>
                      <ScanlineBar pct={Number(row.avg_condition??0)*100} color={Number(row.avg_condition??0)>0.7?"#00ff88":Number(row.avg_condition??0)>0.45?"#f4d35e":"#ff4455"}/>
                      <div style={{ display:"flex", gap:12, marginTop:5, ...mono, fontSize:"0.6rem", color:"#2a5c40" }}>
                        <span>conditioned: <span style={{ color:"#00ff88" }}>{row.conditioned_segments}</span></span>
                        <span>degraded: <span style={{ color:"#ff4455" }}>{row.degraded_segments}</span></span>
                        <span>stale: <span style={{ color:"#f4d35e" }}>{row.stale_segments}</span></span>
                        <span>verif: <span style={{ color:"#56b4ff" }}>{row.verification_events}</span></span>
                        {row.restamp_required&&<span style={{ color:"#ff4455" }}> RESTAMP</span>}
                      </div>
                    </div>
                  )):<div style={{ ...mono, fontSize:"0.8rem", color:"#2a5c40" }}>// NO ACTIVE LANE SEGMENTS</div>}
                </div>
                <div style={{ marginTop:8, ...mono, fontSize:"0.65rem", color:"#2a5c40" }}>
                  AGG: {laneAggregate.conditioned}/{laneAggregate.segments} conditioned ({laneAggregate.conditionedPct.toFixed(1)}%)  avg cond {laneAggregate.avgCondition.toFixed(2)}
                </div>
              </div>
            </div>
          </div>
        )}

        {/*  3D VIEW  */}
        {activeSection==="3d-view"&&(
          <div style={{ animation:"fadeIn 0.3s ease" }}>
            <div style={{ display:"grid", gridTemplateColumns:"2fr 1fr", gap:12 }}>
              <div>
                <div style={{ display:"flex", justifyContent:"space-between", alignItems:"center", marginBottom:8 }}>
                  <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40" }}>// SUBURB 3D  {houses.length} ACTIVE SITES</div>
                  <button onClick={()=>setShowRoleOverlay(v=>!v)} style={{ ...mono, fontSize:"0.65rem", padding:"4px 12px", borderRadius:4, border:`1px solid ${showRoleOverlay?"rgba(0,255,136,0.5)":"rgba(0,255,136,0.15)"}`, background:showRoleOverlay?"rgba(0,255,136,0.08)":"transparent", color:showRoleOverlay?"#00ff88":"#3a6b50" }}>
                    ROLE COLORS {showRoleOverlay?"ON":"OFF"}
                  </button>
                </div>
                <div style={{ height:560, background:"#060f0a", borderRadius:10, overflow:"hidden", border:"1px solid rgba(0,255,136,0.1)" }}>
                  <SuburbScene houses={houses} cells={suburbCells} terrain={suburbTerrain} robots={robots} selectedHouse={selectedHouse} roleByRobotId={robotRoleById} showRoleOverlay={showRoleOverlay}/>
                </div>
                <div style={{ display:"flex", gap:10, marginTop:6, flexWrap:"wrap" }}>
                  {Object.entries(roleColorMap).map(([role,color])=>(<span key={role} style={{ ...mono, fontSize:"0.6rem", color, display:"flex", alignItems:"center", gap:4 }}><span style={{ width:6, height:6, borderRadius:"50%", background:color, display:"inline-block" }}/>{role}</span>))}
                </div>
              </div>
              <div>
                <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:8 }}>// HOUSE {selectedHouse??"-"} GRID</div>
                <div style={{ height:320, background:"#060f0a", borderRadius:10, overflow:"hidden", border:"1px solid rgba(0,255,136,0.1)", marginBottom:8 }}>
                  <GridScene cells={cells}/>
                </div>
                <div style={{ ...mono, fontSize:"0.62rem", letterSpacing:"0.15em", color:"#2a5c40", marginBottom:6 }}>// SELECT HOUSE</div>
                <div style={{ display:"flex", flexWrap:"wrap", gap:4, maxHeight:200, overflowY:"auto" }}>
                  {houses.map(h=>(
                    <button key={h.id} onClick={()=>setSelectedHouse(h.id)} style={{ ...mono, fontSize:"0.65rem", padding:"4px 8px", borderRadius:4, border:`1px solid ${selectedHouse===h.id?"rgba(0,255,136,0.5)":"rgba(0,255,136,0.12)"}`, background:selectedHouse===h.id?"rgba(0,255,136,0.08)":"transparent", color:selectedHouse===h.id?"#00ff88":"#3a6b50" }}>
                      H{h.id}
                    </button>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}


