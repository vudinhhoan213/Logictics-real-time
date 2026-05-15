import React, { useEffect, useMemo, useState, useCallback } from "react";
import { MapContainer, TileLayer, Polyline, Popup, Marker } from "react-leaflet";
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

function TruckMarker({ vehicle }) {
  const spd = Math.round(Number(vehicle.speed) || 0);
  const icon = useMemo(
    () =>
      L.divIcon({
        className: "truck-marker-root",
        html: `<div class="truck-marker-icon" aria-hidden="true">🚚</div><div class="truck-marker-speed">${spd} km/h</div>`,
        iconSize: [72, 44],
        iconAnchor: [36, 40],
      }),
    [spd]
  );

  return (
    <Marker position={[vehicle.lat, vehicle.lon]} icon={icon}>
      <Popup>
        Xe: {vehicle.id}
        <br />
        Tốc độ: {spd} km/h
      </Popup>
    </Marker>
  );
}

function App() {
  const [trafficData, setTrafficData] = useState({});
  const [vehicles, setVehicles] = useState({});
  /** @type {Record<string, { path: string[], time?: number }>} */
  const [routesByVehicle, setRoutesByVehicle] = useState({});
  const [selectedVehicleId, setSelectedVehicleId] = useState(null);

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
    entries.sort(([a], [b]) => {
      if (a === selectedVehicleId) return 1;
      if (b === selectedVehicleId) return -1;
      return a.localeCompare(b);
    });
    return entries;
  }, [routesByVehicle, selectedVehicleId]);

  const truckList = useMemo(() => Object.values(vehicles).sort((a, b) => String(a.id).localeCompare(String(b.id))), [vehicles]);

  const selectedRoute = selectedVehicleId ? routesByVehicle[selectedVehicleId] : null;

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
          maxHeight: "42vh",
          overflow: "auto",
          padding: "12px 14px",
          borderRadius: 10,
          background: "rgba(255,255,255,0.94)",
          boxShadow: "0 4px 20px rgba(0,0,0,0.12)",
          fontSize: 13,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 8, color: "#111" }}>Xe tải & lộ trình</div>
        <div style={{ color: "#444", marginBottom: 10 }}>
          Đang hiển thị: <strong>{truckList.length}</strong> / 100 xe (Kafka)
        </div>
        <div style={{ fontWeight: 600, marginBottom: 6, color: "#333" }}>Tối ưu theo xe (MongoDB)</div>
        {routeEntries.length === 0 ? (
          <p style={{ margin: 0, color: "#666" }}>Chưa có lộ trình. Khi optimizer cập nhật <code>assigned_routes</code>, từng xe sẽ xuất hiện ở đây.</p>
        ) : (
          <ul style={{ listStyle: "none", margin: 0, padding: 0 }}>
            {routeEntries.map(([vid, r]) => {
              const color = hashHue(vid);
              const active = vid === selectedVehicleId;
              const etaMin = r.time != null ? (Number(r.time) / 60).toFixed(1) : "—";
              return (
                <li key={vid} style={{ marginBottom: 6 }}>
                  <button
                    type="button"
                    onClick={() => setSelectedVehicleId(active ? null : vid)}
                    style={{
                      width: "100%",
                      textAlign: "left",
                      padding: "8px 10px",
                      borderRadius: 8,
                      border: active ? `2px solid ${color}` : "1px solid #ddd",
                      background: active ? "rgba(0,0,0,0.04)" : "#fff",
                      cursor: "pointer",
                      fontSize: 12,
                    }}
                  >
                    <span style={{ display: "inline-block", width: 10, height: 10, borderRadius: 2, background: color, marginRight: 8, verticalAlign: "middle" }} />
                    <strong>{vid}</strong>
                    <div style={{ color: "#555", marginTop: 2 }}>ETA: {etaMin} phút · {r.path.length} cạnh</div>
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
          maxWidth: 220,
        }}
      >
        <div style={{ fontWeight: 700, marginBottom: 6 }}>Màu tuyến (Redis)</div>
        <div><span style={{ color: "#3b82f6", fontWeight: 700 }}>■</span> Chưa có dữ liệu</div>
        <div><span style={{ color: "#22c55e", fontWeight: 700 }}>■</span> &gt; 15 km/h</div>
        <div><span style={{ color: "#f97316", fontWeight: 700 }}>■</span> 5–15 km/h</div>
        <div><span style={{ color: "#ef4444", fontWeight: 700 }}>■</span> ≤ 5 km/h</div>
        <div style={{ marginTop: 8, fontWeight: 700 }}>Lộ trình tối ưu</div>
        <div>Mỗi xe một màu; bấm xe ở panel trái để làm nổi tuyến.</div>
        {selectedRoute && selectedVehicleId && (
          <div style={{ marginTop: 8, paddingTop: 8, borderTop: "1px solid #eee" }}>
            <strong>{selectedVehicleId}</strong>
            <br />
            ETA: {selectedRoute.time != null ? `${(Number(selectedRoute.time) / 60).toFixed(1)} phút` : "—"}
          </div>
        )}
      </div>

      <MapContainer center={[21.0262, 105.8375]} zoom={15} style={{ height: "100%", width: "100%" }} preferCanvas>
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />

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
              pathOptions={{ color: roadColor, weight: 3 }}
            />
          );
        })}

        {routeEntries.map(([vid, r]) => {
          const positions = pathToLatLngs(r.path, edgeLookup);
          if (positions.length === 0) return null;
          const color = hashHue(vid);
          const isSel = vid === selectedVehicleId;
          return (
            <Polyline
              key={vid}
              positions={positions}
              pathOptions={{
                color,
                weight: isSel ? 7 : 4,
                opacity: isSel ? 1 : 0.82,
              }}
            >
              <Popup>
                {vid}
                <br />
                ETA: {r.time != null ? `${(Number(r.time) / 60).toFixed(1)} phút` : "—"}
              </Popup>
            </Polyline>
          );
        })}

        {truckList.map((v) => (
          <TruckMarker key={v.id} vehicle={v} />
        ))}
      </MapContainer>
    </div>
  );
}

export default App;
