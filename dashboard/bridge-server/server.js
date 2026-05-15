const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const mongoose = require("mongoose");
const redis = require("redis");
const { Kafka } = require("kafkajs");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

// --- CẤU HÌNH KẾT NỐI ---
const MONGO_URI = process.env.MONGO_URI || "mongodb://mongodb:27017/traffic_system?replicaSet=rs0";
const REDIS_URL = process.env.REDIS_HOST ? `redis://${process.env.REDIS_HOST}:6379` : "redis://redis:6379";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "kafka:29092";

// 1. Kết nối Redis [cite: 18, 55]
const redisClient = redis.createClient({ url: REDIS_URL });
redisClient.connect().then(() => console.log("✅ Đã kết nối Redis"));

// 2. Kết nối MongoDB [cite: 18, 55]
mongoose.connect(MONGO_URI).then(() => console.log("✅ Đã kết nối MongoDB"));

// 3. Kết nối Kafka - Lấy vị trí 100 xe tải [cite: 3, 5, 18]
const kafka = new Kafka({ clientId: "dashboard-backend", brokers: [KAFKA_BROKER] });
const consumer = kafka.consumer({ groupId: "dashboard-group" });

const runKafka = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic: "gps_stream", fromBeginning: false });
  await consumer.run({
    eachMessage: async ({ message }) => {
      try {
        const data = JSON.parse(message.value.toString());
        // Lọc đúng 100 xe tải để hiển thị [cite: 3, 18]
        if (data.entity_type === "Truck") {
          io.emit("vehicle_update", {
            id: data.entity_id,
            lat: data.latitude,
            lon: data.longitude,
            speed: data.speed
          });
        }
      } catch (e) { console.error("Lỗi parse Kafka:", e); }
    },
  });
};
runKafka().catch(console.error);

// --- MONGODB CHANGE STREAMS (Lộ trình GA) [cite: 12, 18, 21] ---
const RouteModel = mongoose.model("Route", new mongoose.Schema({
  vehicle_id: String,
  new_assigned_route: [String],
  estimated_total_travel_time: Number,
}), "assigned_routes");

RouteModel.watch([], { fullDocument: "updateLookup" }).on("change", (change) => {
  const updatedData = change.fullDocument;
  if (updatedData) {
    io.emit("route_optimized", {
      vehicle_id: updatedData.vehicle_id,
      path: updatedData.new_assigned_route,
      time: updatedData.estimated_total_travel_time,
    });
  }
});

async function emitRoutesSnapshot(socket) {
  try {
    const docs = await RouteModel.find({
      vehicle_id: { $exists: true, $nin: [null, ""] },
      "new_assigned_route.0": { $exists: true },
    })
      .select("vehicle_id new_assigned_route estimated_total_travel_time")
      .lean();
    const payload = docs
      .filter((d) => d.vehicle_id && Array.isArray(d.new_assigned_route) && d.new_assigned_route.length)
      .map((d) => ({
        vehicle_id: d.vehicle_id,
        path: d.new_assigned_route,
        time: d.estimated_total_travel_time,
      }));
    socket.emit("routes_snapshot", payload);
  } catch (e) {
    console.error("routes_snapshot:", e);
  }
}

// --- REAL-TIME TRAFFIC (Redis Polling) [cite: 18, 20, 57] ---
io.on("connection", (socket) => {
  console.log("📡 Dashboard connected: " + socket.id);

  emitRoutesSnapshot(socket);

  const trafficInterval = setInterval(async () => {
    try {
      const keys = await redisClient.keys("edge:*");
      for (const key of keys) {
        const rawData = await redisClient.get(key);
        if (rawData) {
          const payload = JSON.parse(rawData);
          socket.emit("traffic_update", {
            edge_id: key.replace("edge:", ""),
            avg_speed: payload.avg_speed || 0,
          });
        }
      }
    } catch (err) { console.error("Lỗi lấy Redis:", err); }
  }, 2000);

  socket.on("disconnect", () => clearInterval(trafficInterval));
});

// Sửa lại dòng lỗi này:
server.listen(4000, () => console.log(`🚀 SERVER RUNNING ON PORT 4000`));