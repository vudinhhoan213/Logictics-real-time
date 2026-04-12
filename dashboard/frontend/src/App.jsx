import React, { useEffect, useState, useMemo } from 'react';
import { MapContainer, TileLayer, Polyline, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import edgesData from './data/edges_schema.json'; 
import { io } from 'socket.io-client';

function App() {
  const [trafficData, setTrafficData] = useState({});
  const [gaRoute, setGaRoute] = useState([]); 
  const [eta, setEta] = useState(null);

  // lookup nhanh O(1)
  const edgeLookup = useMemo(() => {
    const map = {};
    edgesData.forEach(edge => {
      map[edge.edge_id] = edge;
    });
    return map;
  }, []);

  useEffect(() => {
    const socket = io('http://localhost:4000');

    // 🔥 DEBUG: xem tất cả event
    socket.onAny((event, ...args) => {
      console.log("📡 EVENT:", event, args);
    });

    socket.on('traffic_update', (data) => {
      setTrafficData(prev => ({
        ...prev,
        [data.edge_id]: data.avg_speed
      }));
    });

    socket.on('route_optimized', (data) => {
      console.log("🔥 RAW DATA:", data);

      if (data.path && data.path.length > 0) {
        setGaRoute(data.path);
      }

      if (data.time !== undefined) {
        setEta(data.time);
      }
    });

    return () => {
      socket.off('traffic_update');
      socket.off('route_optimized'); // ✅ FIX
      socket.disconnect();
    };
  }, []);

  const gaPolylinePoints = useMemo(() => {
    const points = [];
  
    gaRoute.forEach((id, index) => {
      const edge = edgeLookup[id];
      if (!edge) return;
  
      // chỉ thêm start node của edge đầu tiên
      if (index === 0) {
        points.push([edge.start_node.lat, edge.start_node.lon]);
      }
  
      // luôn thêm end node
      points.push([edge.end_node.lat, edge.end_node.lon]);
    });
  
    console.log("🧭 Route:", gaRoute);
    console.log("📍 FIXED Points:", points);
  
    return points;
  }, [gaRoute, edgeLookup]);
  return (
    <div style={{ height: "100vh", width: "100vw", position: "relative" }}>
      {eta && (
        <div style={{ 
          position: "absolute", top: 20, right: 20, zIndex: 1000, 
          background: "rgba(255, 255, 255, 0.9)", padding: "15px", 
          borderRadius: "8px", boxShadow: "0 4px 15px rgba(0,0,0,0.3)",
          border: "2px solid #FFD700"
        }}>
           <strong>⏱ ETA: {(eta/60).toFixed(1)} phút</strong>
        </div>
      )}

      <MapContainer 
        center={[21.0262385, 105.8375869]} 
        zoom={15} 
        style={{ height: "100%", width: "100%" }}
        preferCanvas={true}
      >
        <TileLayer url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />

        {/* Nền giao thông */}
        {edgesData.map((edge) => {
          const speed = trafficData[edge.edge_id];
          let roadColor = "#ccc";

          if (speed !== undefined) {
            roadColor = speed < 15 ? "red" : "green";
          }

          return (
            <Polyline 
              key={edge.edge_id}
              positions={[
                [edge.start_node.lat, edge.start_node.lon],
                [edge.end_node.lat, edge.end_node.lon]
              ]}
              pathOptions={{ color: roadColor, weight: 3, opacity: 0.4 }}
            />
          );
        })}

        {/* Route GA */}
        {gaPolylinePoints.length > 0 && (
          <Polyline 
            positions={gaPolylinePoints}
            pathOptions={{ color: "#FFD700", weight: 8 }}
          >
            <Popup>
              ETA: {(eta/60).toFixed(1)} phút
            </Popup>
          </Polyline>
        )}
      </MapContainer>
    </div>
  );
}

export default App;