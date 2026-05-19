import React, { useEffect, useMemo, useState, useCallback, useRef } from "react";
import { MapContainer, TileLayer, Polyline, Popup, Marker, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import "leaflet.heat";
import edgesData from "./data/edges_schema.json";
import { io } from "socket.io-client";

// Trong K8s: Vite dev server proxy /socket.io → backend service
// Fallback dùng VITE_API_URL hoặc rỗng (dùng proxy của Vite)
const SOCKET_URL = import.meta.env.VITE_API_URL || "";

const ROUTE_PALETTE = [
  "#e11d48",
  "#2563eb",
  "#16a34a",
  "#ca8a04",
  "#9333ea",
  "#0891b2",
  "#ea580c",
  "#4f46e5",
  "#db2777",
  "#0d9488",
  "#65a30d",
  "#7c3aed",
];

function hashHue(vehicleId) {
  let h = 0;
  for (let i = 0; i < vehicleId.length; i += 1) {
    h = (h * 31 + vehicleId.charCodeAt(i)) | 0;
  }
  return ROUTE_PALETTE[Math.abs(h) % ROUTE_PALETTE.length];
}

function compareTruckIds(a, b) {
  const na = parseInt(String(a).replace(/\D/g, ""), 10) || 0;
  const nb = parseInt(String(b).replace(/\D/g, ""), 10) || 0;
  if (na !== nb) return na - nb;
  return String(a).localeCompare(String(b));
}

function formatEtaMinutes(time) {
  if (time == null || Number.isNaN(Number(time))) return "—";
  const min = Number(time) / 60;
  if (min >= 10000) return `${(min / 60).toFixed(1)} giờ`;
  return `${min.toFixed(1)} phút`;
}

function pathToLatLngs(path, edgeLookup) {
  // Trả về MẢNG CÁC SEGMENTS (mỗi segment là 1 polyline liên tục)
  // Tránh vẽ đường chim bay khi edges không nối tiếp nhau
  const segments = [];
  let currentSegment = [];

  path.forEach((id, index) => {
    const edge = edgeLookup[id];
    if (!edge) return;

    const start = [edge.start_node.lat, edge.start_node.lon];
    const end = [edge.end_node.lat, edge.end_node.lon];

    if (currentSegment.length === 0) {
      // Bắt đầu segment mới
      currentSegment.push(start, end);
    } else {
      // Kiểm tra edge hiện tại có nối tiếp edge trước không
      const lastPoint = currentSegment[currentSegment.length - 1];
      const isContinuous =
        Math.abs(lastPoint[0] - start[0]) < 0.0001 &&
        Math.abs(lastPoint[1] - start[1]) < 0.0001;

      if (isContinuous) {
        currentSegment.push(end);
      } else {
        // Ngắt segment, bắt đầu segment mới
        if (currentSegment.length >= 2) segments.push(currentSegment);
        currentSegment = [start, end];
      }
    }
  });

  if (currentSegment.length >= 2) segments.push(currentSegment);
  return segments;
}

// Giữ hàm cũ cho traffic edges (1 edge = 1 polyline, không cần segment)
function edgeToLatLngs(edge) {
  return [
    [edge.start_node.lat, edge.start_node.lon],
    [edge.end_node.lat, edge.end_node.lon],
  ];
}

/**
 * Tìm index của edge trong path gần nhất với vị trí GPS (lat, lon).
 * Dùng để chia route thành phần đã đi và phần phía trước.
 */
function findCurrentEdgeIndex(path, edgeLookup, lat, lon) {
  let bestIdx = 0;
  let bestDist = Infinity;
  for (let i = 0; i < path.length; i++) {
    const edge = edgeLookup[path[i]];
    if (!edge) continue;
    // Khoảng cách tới midpoint của edge
    const midLat = (edge.start_node.lat + edge.end_node.lat) / 2;
    const midLon = (edge.start_node.lon + edge.end_node.lon) / 2;
    const d = (midLat - lat) ** 2 + (midLon - lon) ** 2;
    if (d < bestDist) {
      bestDist = d;
      bestIdx = i;
    }
  }
  return bestIdx;
}

/**
 * Chia route path thành 2 phần: đã đi qua (traversed) và phía trước (upcoming)
 * dựa trên vị trí GPS hiện tại của xe.
 */
function splitRouteAtVehicle(path, edgeLookup, lat, lon) {
  if (!path || path.length === 0 || lat == null || lon == null) {
    return { traversed: [], upcoming: path || [] };
  }
  const idx = findCurrentEdgeIndex(path, edgeLookup, lat, lon);
  return {
    traversed: path.slice(0, idx),
    upcoming: path.slice(idx),
  };
}

/**
 * Zoom fit lộ trình 1 LẦN khi chọn xe (không re-fit khi xe di chuyển).
 * Chỉ zoom lại khi selectedVehicleId thay đổi.
 */
function MapFitRoute({ routeSegments, vehiclePos, selectedId }) {
  const map = useMap();
  const lastFitId = useRef(null);

  useEffect(() => {
    // Chỉ fit 1 lần khi selectedId thay đổi
    if (!selectedId || lastFitId.current === selectedId) return;
    lastFitId.current = selectedId;

    const points = [];
    if (vehiclePos) points.push(vehiclePos);
    if (routeSegments) {
      for (const seg of routeSegments) for (const pt of seg) points.push(pt);
    }
    if (points.length === 0) return;

    const bounds = L.latLngBounds(points);
    map.fitBounds(bounds, { padding: [50, 50], maxZoom: 17, duration: 0.6 });
  }, [map, selectedId]); // eslint-disable-line react-hooks/exhaustive-deps

  return null;
}

// Giữ MapFlyTo cho fallback khi chưa có route
function MapFlyTo({ lat, lon, zoom = 16 }) {
  const map = useMap();
  useEffect(() => {
    if (lat == null || lon == null) return;
    map.flyTo([lat, lon], zoom, { duration: 0.6 });
  }, [map, lat, lon, zoom]);
  return null;
}

// ─── Heatmap Layer: hiển thị mật độ tắc đường ────────────────────────
function HeatmapLayer({ trafficData, edgeLookup }) {
  const map = useMap();
  const heatLayerRef = useRef(null);

  useEffect(() => {
    // Tạo heatmap data: mỗi edge có tắc đường → thêm điểm nóng
    const points = [];
    for (const [edgeId, avgSpeed] of Object.entries(trafficData)) {
      const edge = edgeLookup[edgeId];
      if (!edge) continue;
      // Intensity: tốc độ thấp → nóng hơn (max intensity khi speed ≤ 5)
      const intensity = Math.max(0, 1 - avgSpeed / 30); // 0→30 km/h map to 1→0
      if (intensity > 0.1) {
        const midLat = (edge.start_node.lat + edge.end_node.lat) / 2;
        const midLon = (edge.start_node.lon + edge.end_node.lon) / 2;
        points.push([midLat, midLon, intensity]);
      }
    }

    if (heatLayerRef.current) map.removeLayer(heatLayerRef.current);
    if (points.length > 0) {
      heatLayerRef.current = L.heatLayer(points, { radius: 25, blur: 15, maxZoom: 17, max: 1.0 }).addTo(map);
    }

    return () => { if (heatLayerRef.current) map.removeLayer(heatLayerRef.current); };
  }, [map, trafficData, edgeLookup]);

  return null;
}

// ─── Traffic Edges: chỉ render edges CÓ data Redis (tối ưu performance) ──
const TrafficEdges = React.memo(function TrafficEdges({ trafficData, edgeLookup, dimmed }) {
  // Render TẤT CẢ edges: xám mặc định, tô màu khi có data Redis
  const edgesToRender = useMemo(() => {
    const result = [];
    for (const edge of edgesData) {
      const edgeId = edge.edge_id;
      const avgSpeed = trafficData[edgeId];
      let color = "#d1d5db"; // xám nhạt mặc định (chưa có data)
      let weight = 2;
      if (avgSpeed !== undefined) {
        color = avgSpeed <= 5 ? "#ef4444" : avgSpeed <= 15 ? "#f97316" : "#22c55e";
        weight = 3;
      }
      result.push({
        key: edgeId,
        positions: [
          [edge.start_node.lat, edge.start_node.lon],
          [edge.end_node.lat, edge.end_node.lon],
        ],
        color,
        weight,
      });
    }
    return result;
  }, [trafficData, edgeLookup]);

  return (
    <>
      {edgesToRender.map((e) => (
        <Polyline
          key={e.key}
          positions={e.positions}
          pathOptions={{ color: e.color, weight: e.weight, opacity: dimmed ? 0.2 : 0.7 }}
        />
      ))}
    </>
  );
});

function TruckMarker({ vehicle, routeColor, hasRoute, isSelected, onSelect }) {
  const rawSpd = Number(vehicle.speed) || 0;
  const spd = rawSpd > 0 ? Math.max(3, Math.round(rawSpd)) : 0;
  const shortId = String(vehicle.id).replace(/^Truck_/i, "");
  const borderColor = routeColor || "#94a3b8";

  const icon = useMemo(
    () =>
      L.divIcon({
        className: `truck-marker-root${isSelected ? " truck-marker-selected" : ""}`,
        html:
          `<div class="truck-marker-label${hasRoute ? " has-route" : ""}" style="border-left-color:${borderColor}">${shortId}</div>` +
          `<div class="truck-marker-icon" aria-hidden="true">🚚</div>` +
          `<div class="truck-marker-speed">${spd} km/h</div>`,
        iconSize: [80, 58],
        iconAnchor: [40, 52],
      }),
    [shortId, spd, borderColor, hasRoute, isSelected]
  );

  return (
    <Marker
      position={[vehicle.lat, vehicle.lon]}
      icon={icon}
      eventHandlers={{ click: () => onSelect(vehicle.id) }}
    >
      <Popup>
        <strong>{vehicle.id}</strong>
        <br />
        Tốc độ: {spd} km/h
        {hasRoute && (
          <>
            <br />
            <span style={{ color: routeColor }}>●</span> Có lộ trình GA (viền nhãn cùng màu đường)
          </>
        )}
      </Popup>
    </Marker>
  );
}

function App() {
  const [trafficData, setTrafficData] = useState({});
  const [vehicles, setVehicles] = useState({});
  const [routesByVehicle, setRoutesByVehicle] = useState({});
  const [selectedVehicleId, setSelectedVehicleId] = useState(null);
  const [routeFilter, setRouteFilter] = useState("");
  const [showHeatmap, setShowHeatmap] = useState(true);
  const socketRef = useRef(null);

  const edgeLookup = useMemo(() => {
    const map = {};
    edgesData.forEach((edge) => {
      map[edge.edge_id] = edge;
    });
    return map;
  }, []);

  const mergeRoutes = useCallback((list) => {
    setRoutesByVehicle((prev) => {
      const next = { ...prev };
      for (const item of list || []) {
        const vid = item.vehicle_id;
        if (!vid) continue;
        const path = item.path || [];
        if (path.length === 0) {
          delete next[vid];
          continue;
        }
        next[vid] = { path, old_path: item.old_path || [], new_path: item.new_path || [], time: item.time };
      }
      return next;
    });
  }, []);

  useEffect(() => {
    const socket = io(SOCKET_URL, { transports: ["polling", "websocket"] });
    socketRef.current = socket;

    // Legacy: từng event đơn lẻ (tương thích ngược)
    socket.on("traffic_update", (data) => {
      setTrafficData((prev) => ({ ...prev, [data.edge_id]: data.avg_speed }));
    });

    socket.on("vehicle_update", (data) => {
      setVehicles((prev) => ({ ...prev, [data.id]: data }));
    });

    // Batch events (tối ưu mới)
    socket.on("traffic_batch", (batch) => {
      setTrafficData((prev) => {
        const next = { ...prev };
        for (const item of batch) { next[item.edge_id] = item.avg_speed; }
        return next;
      });
    });

    socket.on("vehicle_batch", (batch) => {
      setVehicles((prev) => {
        const next = { ...prev };
        for (const v of batch) { next[v.id] = v; }
        return next;
      });
    });

    socket.on("routes_snapshot", (payload) => {
      mergeRoutes(payload);
    });

    socket.on("route_optimized", (data) => {
      mergeRoutes([data]);
    });

    // Nhận route_result từ backend PathFinder
    socket.on("route_result", (data) => {
      if (data.path && data.path.length > 0) {
        mergeRoutes([{ vehicle_id: data.vehicle_id, path: data.path, time: data.time }]);
      }
    });

    return () => {
      socket.disconnect();
      socketRef.current = null;
    };
  }, [mergeRoutes]);

  const routeEntries = useMemo(() => {
    const entries = Object.entries(routesByVehicle).filter(([, r]) => r.path?.length);
    entries.sort(([a], [b]) => compareTruckIds(a, b));
    return entries;
  }, [routesByVehicle]);

  const filteredRouteEntries = useMemo(() => {
    const q = routeFilter.trim().toLowerCase();
    if (!q) return routeEntries;
    return routeEntries.filter(([vid]) => vid.toLowerCase().includes(q));
  }, [routeEntries, routeFilter]);

  const truckList = useMemo(
    () => Object.values(vehicles).sort((a, b) => compareTruckIds(a.id, b.id)),
    [vehicles]
  );

  const selectedRoute = selectedVehicleId ? routesByVehicle[selectedVehicleId] : null;
  const selectedVehicle = selectedVehicleId ? vehicles[selectedVehicleId] : null;
  const selectedColor = selectedVehicleId ? hashHue(selectedVehicleId) : null;

  const selectVehicle = useCallback((vid) => {
    setSelectedVehicleId((prev) => {
      const newVid = prev === vid ? null : vid;

      // Khi chọn xe → request tính route từ vị trí hiện tại qua các customer
      if (newVid && socketRef.current) {
        const v = vehicles[newVid];
        if (v && v.lat && v.lon) {
          socketRef.current.emit("request_route", {
            vehicle_id: newVid,
            lat: v.lat,
            lon: v.lon,
          });
        }
      }

      return newVid;
    });
  }, [vehicles]);

  return (
    <div style={{ height: "100vh", width: "100vw", position: "relative" }}>
      <aside
        className="dash-panel"
        style={{
          position: "absolute",
          top: 12,
          left: 12,
          zIndex: 1000,
          width: 280,
          maxHeight: "55vh",
          overflow: "auto",
          padding: "12px 14px",
          borderRadius: 10,
          background: "rgba(255,255,255,0.94)",
          boxShadow: "0 4px 20px rgba(0,0,0,0.12)",
          fontSize: 13,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 8, color: "#111" }}>Xe tải & lộ trình</div>
        <div style={{ color: "#444", marginBottom: 8 }}>
          Đang hiển thị: <strong>{truckList.length}</strong> / 100 xe (Kafka)
        </div>
        <p style={{ fontSize: 11, color: "#666", margin: "0 0 10px", lineHeight: 1.4 }}>
          Nhãn trên map: <strong>001</strong> = Truck_001. Bấm xe hoặc mục trong danh sách để xem đường GA (nét đậm).
        </p>

        <div style={{ fontWeight: 600, marginBottom: 6, color: "#333" }}>
          Tối ưu theo xe ({routeEntries.length})
        </div>
        <input
          type="search"
          placeholder="Tìm Truck_001..."
          value={routeFilter}
          onChange={(e) => setRouteFilter(e.target.value)}
          style={{
            width: "100%",
            boxSizing: "border-box",
            padding: "6px 8px",
            marginBottom: 8,
            borderRadius: 6,
            border: "1px solid #ccc",
            fontSize: 12,
          }}
        />

        {filteredRouteEntries.length === 0 ? (
          <p style={{ margin: 0, color: "#666" }}>
            {routeEntries.length === 0
              ? "Chưa có lộ trình. Chạy seed + đợi GA ghi MongoDB."
              : "Không khớp tìm kiếm."}
          </p>
        ) : (
          <ul style={{ listStyle: "none", margin: 0, padding: 0 }}>
            {filteredRouteEntries.map(([vid, r]) => {
              const color = hashHue(vid);
              const active = vid === selectedVehicleId;
              return (
                <li key={vid} style={{ marginBottom: 6 }}>
                  <button
                    type="button"
                    onClick={() => selectVehicle(vid)}
                    style={{
                      width: "100%",
                      textAlign: "left",
                      padding: "8px 10px",
                      borderRadius: 8,
                      border: active ? `2px solid ${color}` : "1px solid #ddd",
                      background: active ? "rgba(124, 58, 237, 0.08)" : "#fff",
                      cursor: "pointer",
                      fontSize: 12,
                    }}
                  >
                    <span
                      style={{
                        display: "inline-block",
                        width: 10,
                        height: 10,
                        borderRadius: 2,
                        background: color,
                        marginRight: 8,
                        verticalAlign: "middle",
                      }}
                    />
                    <strong>{vid}</strong>
                    <div style={{ color: "#555", marginTop: 2, display: "block" }}>
                      ETA: {formatEtaMinutes(r.time)} · {r.path.length} cạnh
                    </div>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </aside>

      <div
        style={{
          position: "absolute",
          top: 12,
          right: 12,
          zIndex: 1000,
          background: "rgba(255,255,255,0.94)",
          padding: "10px 12px",
          borderRadius: 10,
          boxShadow: "0 4px 20px rgba(0,0,0,0.12)",
          fontSize: 12,
          maxWidth: 240,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 6 }}>Màu tuyến (Redis)</div>
        <div><span style={{ color: "#d1d5db", fontWeight: 700 }}>■</span> Chưa có dữ liệu</div>
        <div><span style={{ color: "#22c55e", fontWeight: 700 }}>■</span> &gt; 15 km/h</div>
        <div><span style={{ color: "#f97316", fontWeight: 700 }}>■</span> 5–15 km/h</div>
        <div><span style={{ color: "#ef4444", fontWeight: 700 }}>■</span> ≤ 5 km/h</div>

        <div style={{ marginTop: 8 }}>
          <label style={{ cursor: "pointer", display: "flex", alignItems: "center", gap: 6 }}>
            <input
              type="checkbox"
              checked={showHeatmap}
              onChange={(e) => setShowHeatmap(e.target.checked)}
            />
            <span style={{ fontWeight: 600 }}>🔥 Heatmap tắc đường</span>
          </label>
        </div>

        <div style={{ marginTop: 8, fontWeight: 700 }}>Lộ trình (khi chọn xe)</div>
        {selectedVehicleId && selectedColor ? (
          <div style={{ marginTop: 4 }}>
            <strong>{selectedVehicleId}</strong>
            <br />
            ETA: {selectedRoute ? formatEtaMinutes(selectedRoute.time) : "—"}
            <br />
            <span style={{ color: "#2563eb", fontWeight: 700 }}>- - -</span> Đường cũ (trước GA)
            <br />
            <span style={{ color: "#ef4444", fontWeight: 700 }}>━━</span> Đường mới (sau GA)
          </div>
        ) : (
          <div style={{ color: "#666" }}>Chọn Truck_001… ở panel trái hoặc bấm xe trên map.</div>
        )}
      </div>

      <MapContainer center={[21.0262, 105.8375]} zoom={15} style={{ height: "100%", width: "100%" }} preferCanvas>
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />

        {/* Heatmap layer - bản đồ nhiệt mật độ tắc đường */}
        {showHeatmap && <HeatmapLayer trafficData={trafficData} edgeLookup={edgeLookup} />}

        {/* Zoom fit route khi chọn xe */}
        {selectedVehicleId && selectedRoute && (() => {
          const allSegs = pathToLatLngs(selectedRoute.path, edgeLookup);
          const vPos = selectedVehicle ? [selectedVehicle.lat, selectedVehicle.lon] : null;
          return <MapFitRoute routeSegments={allSegs} vehiclePos={vPos} selectedId={selectedVehicleId} />;
        })()}
        {/* Fallback: chọn xe chưa có route → fly tới vị trí xe */}
        {selectedVehicleId && !selectedRoute && selectedVehicle && (
          <MapFlyTo lat={selectedVehicle.lat} lon={selectedVehicle.lon} />
        )}

        {/* Traffic edges — chỉ render edges CÓ dữ liệu Redis (tiết kiệm render) */}
        <TrafficEdges trafficData={trafficData} edgeLookup={edgeLookup} dimmed={!!selectedVehicleId} />

        {/* Route rendering: xe được chọn hiện kiểu Google Maps, xe khác mờ */}
        {/* Route xe được chọn: XANH DƯƠNG = đường cũ, ĐỎ = đường mới (sau GA) */}
        {selectedVehicleId && selectedRoute && (() => {
          const r = selectedRoute;
          const oldSegs = pathToLatLngs(r.old_path, edgeLookup);
          const newSegs = pathToLatLngs(r.new_path.length > 0 ? r.new_path : r.path, edgeLookup);

          return (
            <>
              {/* Đường cũ (assigned_route) — XANH DƯƠNG, nét đứt */}
              {oldSegs.map((seg, idx) => (
                <Polyline
                  key={`old-${idx}`}
                  positions={seg}
                  pathOptions={{
                    color: "#2563eb",
                    weight: 5,
                    opacity: 0.7,
                    dashArray: "10 6",
                  }}
                />
              ))}
              {/* Đường mới (new_assigned_route sau GA) — ĐỎ, nét liền đậm */}
              {newSegs.map((seg, idx) => (
                <Polyline
                  key={`new-${idx}`}
                  positions={seg}
                  pathOptions={{
                    color: "#ef4444",
                    weight: 6,
                    opacity: 0.9,
                  }}
                >
                  {idx === 0 && (
                    <Popup>
                      <strong>{selectedVehicleId}</strong>
                      <br />
                      ETA: {formatEtaMinutes(r.time)}
                      <br />
                      <span style={{color:"#2563eb"}}>━━</span> Đường cũ ({r.old_path.length} cạnh)
                      <br />
                      <span style={{color:"#ef4444"}}>━━</span> Đường tối ưu ({(r.new_path.length || r.path.length)} cạnh)
                    </Popup>
                  )}
                </Polyline>
              ))}
            </>
          );
        })()}
                  <br />
                  ETA: {formatEtaMinutes(r.time)}
                </Popup>
              )}
            </Polyline>
          ));
        })}

        {truckList.map((v) => (
          <TruckMarker
            key={v.id}
            vehicle={v}
            routeColor={routesByVehicle[v.id] ? hashHue(v.id) : null}
            hasRoute={Boolean(routesByVehicle[v.id]?.path?.length)}
            isSelected={v.id === selectedVehicleId}
            onSelect={selectVehicle}
          />
        ))}
      </MapContainer>
    </div>
  );
}

export default App;
