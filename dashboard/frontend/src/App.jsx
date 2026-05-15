import React, { useEffect, useMemo, useState, useCallback } from "react";
import { MapContainer, TileLayer, Polyline, Popup, Marker, useMap } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import edgesData from "./data/edges_schema.json";
import { io } from "socket.io-client";

const SOCKET_URL = import.meta.env.VITE_API_URL || "http://localhost:4000";

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
  const points = [];
  path.forEach((id, index) => {
    const edge = edgeLookup[id];
    if (!edge) return;
    if (index === 0) points.push([edge.start_node.lat, edge.start_node.lon]);
    points.push([edge.end_node.lat, edge.end_node.lon]);
  });
  return points;
}

function MapFlyTo({ lat, lon, zoom = 16 }) {
  const map = useMap();
  useEffect(() => {
    if (lat == null || lon == null) return;
    map.flyTo([lat, lon], zoom, { duration: 0.6 });
  }, [map, lat, lon, zoom]);
  return null;
}

function TruckMarker({ vehicle, routeColor, hasRoute, isSelected, onSelect }) {
  const spd = Math.round(Number(vehicle.speed) || 0);
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
        next[vid] = { path, time: item.time };
      }
      return next;
    });
  }, []);

  useEffect(() => {
    const socket = io(SOCKET_URL, { transports: ["websocket", "polling"] });

    socket.on("traffic_update", (data) => {
      setTrafficData((prev) => ({ ...prev, [data.edge_id]: data.avg_speed }));
    });

    socket.on("vehicle_update", (data) => {
      setVehicles((prev) => ({ ...prev, [data.id]: data }));
    });

    socket.on("routes_snapshot", (payload) => {
      mergeRoutes(payload);
    });

    socket.on("route_optimized", (data) => {
      mergeRoutes([data]);
    });

    return () => socket.disconnect();
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
    setSelectedVehicleId((prev) => (prev === vid ? null : vid));
  }, []);

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
        <div><span style={{ color: "#3b82f6", fontWeight: 700 }}>■</span> Chưa có dữ liệu</div>
        <div><span style={{ color: "#22c55e", fontWeight: 700 }}>■</span> &gt; 15 km/h</div>
        <div><span style={{ color: "#f97316", fontWeight: 700 }}>■</span> 5–15 km/h</div>
        <div><span style={{ color: "#ef4444", fontWeight: 700 }}>■</span> ≤ 5 km/h</div>
        <div style={{ marginTop: 8, fontWeight: 700 }}>Lộ trình GA (đang chọn)</div>
        {selectedVehicleId && selectedColor ? (
          <div style={{ marginTop: 4 }}>
            <span style={{ color: selectedColor, fontWeight: 700 }}>━━</span> {selectedVehicleId}
            <br />
            ETA: {selectedRoute ? formatEtaMinutes(selectedRoute.time) : "—"}
            <br />
            <span style={{ color: "#666" }}>Các tuyến khác mờ khi đã chọn xe.</span>
          </div>
        ) : (
          <div style={{ color: "#666" }}>Chọn Truck_001… ở panel trái hoặc bấm xe trên map.</div>
        )}
      </div>

      <MapContainer center={[21.0262, 105.8375]} zoom={15} style={{ height: "100%", width: "100%" }} preferCanvas>
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />

        {selectedVehicle && <MapFlyTo lat={selectedVehicle.lat} lon={selectedVehicle.lon} />}

        {edgesData.map((edge) => {
          const speed = trafficData[edge.edge_id];
          const roadColor =
            speed !== undefined ? (speed <= 5 ? "#ef4444" : speed <= 15 ? "#f97316" : "#22c55e") : "#3b82f6";
          return (
            <Polyline
              key={edge.edge_id}
              positions={[
                [edge.start_node.lat, edge.start_node.lon],
                [edge.end_node.lat, edge.end_node.lon],
              ]}
              pathOptions={{ color: roadColor, weight: 3, opacity: selectedVehicleId ? 0.35 : 1 }}
            />
          );
        })}

        {routeEntries.map(([vid, r]) => {
          const positions = pathToLatLngs(r.path, edgeLookup);
          if (positions.length === 0) return null;
          const color = hashHue(vid);
          const isSel = vid === selectedVehicleId;
          const dimOthers = selectedVehicleId && !isSel;
          return (
            <Polyline
              key={vid}
              positions={positions}
              pathOptions={{
                color,
                weight: isSel ? 9 : 4,
                opacity: dimOthers ? 0.12 : isSel ? 1 : 0.75,
              }}
            >
              <Popup>
                {vid}
                <br />
                ETA: {formatEtaMinutes(r.time)}
              </Popup>
            </Polyline>
          );
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
