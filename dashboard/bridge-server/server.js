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

// --- CẤU HÌNH KẾT NỐI (K8s Service names) ---
const MONGO_URI = process.env.MONGO_URI || "mongodb://mongodb:27017/traffic_system?replicaSet=rs0";
const REDIS_URL = process.env.REDIS_HOST ? `redis://${process.env.REDIS_HOST}:6379` : "redis://redis.default.svc.cluster.local:6379";
const KAFKA_BROKER = process.env.KAFKA_BROKER || "kafka:9092";

// --- Trạng thái kết nối ---
let redisReady = false;
let mongoReady = false;

// 1. Kết nối Redis - RETRY khi service chưa sẵn sàng
const redisClient = redis.createClient({
  url: REDIS_URL,
  socket: {
    reconnectStrategy: (retries) => {
      console.log(`⏳ Redis reconnect attempt ${retries}...`);
      return Math.min(retries * 1000, 10000); // max 10s giữa các lần retry
    }
  }
});
redisClient.on("error", (err) => {
  if (redisReady) console.error("⚠️ Redis error:", err.message);
  redisReady = false;
});
redisClient.on("ready", () => {
  redisReady = true;
  console.log("✅ Đã kết nối Redis");
});
redisClient.connect().catch((err) => {
  console.warn("⚠️ Redis chưa sẵn sàng:", err.message, "- Sẽ tự retry...");
});

// 2. Kết nối MongoDB - RETRY khi service chưa sẵn sàng
function connectMongo() {
  mongoose.connect(MONGO_URI, {
    serverSelectionTimeoutMS: 10000,
  })
    .then(() => {
      mongoReady = true;
      console.log("✅ Đã kết nối MongoDB");
      setupChangeStreams();
    })
    .catch((err) => {
      console.warn("⚠️ MongoDB chưa sẵn sàng:", err.message, "- Retry sau 5s...");
      setTimeout(connectMongo, 5000);
    });
}
connectMongo();

// 3. Kết nối Kafka - Retry vô hạn
const kafka = new Kafka({
  clientId: "dashboard-backend",
  brokers: [KAFKA_BROKER],
  retry: { initialRetryTime: 3000, retries: 10 }
});

async function runKafkaForever() {
  while (true) {
    const consumer = kafka.consumer({ groupId: "dashboard-group" });
    try {
      console.log(`Đang kết nối Kafka (${KAFKA_BROKER})...`);
      await consumer.connect();
      await consumer.subscribe({ topic: "gps_stream", fromBeginning: false });
      console.log("✅ Kafka: đã subscribe topic gps_stream");

      // Buffer để batch emit vehicle_update mỗi 1s
      let vehicleBuffer = {};
      setInterval(() => {
        const batch = Object.values(vehicleBuffer);
        if (batch.length > 0) {
          io.emit("vehicle_batch", batch);
          vehicleBuffer = {};
        }
      }, 1000);

      await consumer.run({
        eachMessage: async ({ message }) => {
          try {
            const data = JSON.parse(message.value.toString());
            if (data.entity_type === "Truck") {
              // Buffer thay vì emit từng cái
              vehicleBuffer[data.entity_id] = {
                id: data.entity_id,
                lat: data.latitude,
                lon: data.longitude,
                speed: data.speed
              };
            }
          } catch (e) { console.error("Lỗi parse Kafka:", e.message); }
        },
      });
      // Nếu consumer chạy thành công, dừng vòng lặp retry
      break;
    } catch (e) {
      console.warn("⚠️ Kafka consumer:", e.message || e, "- Retry sau 10s...");
      try { await consumer.disconnect(); } catch (_) { /* ignore */ }
      await new Promise((r) => setTimeout(r, 10000));
    }
  }
}
runKafkaForever();

// --- MONGODB CHANGE STREAMS (Lộ trình GA) ---
const RouteModel = mongoose.model("Route", new mongoose.Schema({
  vehicle_id: String,
  new_assigned_route: [String],
  estimated_total_travel_time: Number,
}), "assigned_routes");

function setupChangeStreams() {
  try {
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
    console.log("✅ MongoDB Change Streams đã sẵn sàng");
  } catch (e) {
    console.error("⚠️ Change Streams error:", e.message);
  }
}

async function emitRoutesSnapshot(socket) {
  if (!mongoReady) return;
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
    console.error("routes_snapshot:", e.message);
  }
}

// --- REAL-TIME TRAFFIC (Redis Polling) ---
io.on("connection", (socket) => {
  console.log("📡 Dashboard connected: " + socket.id);

  emitRoutesSnapshot(socket);

  const trafficInterval = setInterval(async () => {
    if (!redisReady) return; // Bỏ qua nếu Redis chưa sẵn sàng
    try {
      // Dùng SCAN thay vì KEYS (non-blocking) + MGET batch
      const keys = [];
      for await (const key of redisClient.scanIterator({ MATCH: "edge:*", COUNT: 200 })) {
        keys.push(key);
      }
      if (keys.length > 0) {
        const values = await redisClient.mGet(keys);
        const trafficBatch = [];
        for (let i = 0; i < keys.length; i++) {
          if (values[i]) {
            const payload = JSON.parse(values[i]);
            trafficBatch.push({ edge_id: keys[i].replace("edge:", ""), avg_speed: payload.avg_speed || 0 });
          }
        }
        if (trafficBatch.length > 0) {
          socket.emit("traffic_batch", trafficBatch);
        }
      }
    } catch (err) { console.error("Lỗi lấy Redis:", err.message); }
  }, 2000);

  socket.on("disconnect", () => {
    console.log("❌ Dashboard disconnected: " + socket.id);
    clearInterval(trafficInterval);
  });
});

// --- Xử lý lỗi không crash process ---
process.on("unhandledRejection", (err) => {
  console.error("⚠️ Unhandled rejection:", err.message);
});
process.on("uncaughtException", (err) => {
  console.error("⚠️ Uncaught exception:", err.message);
});

// --- START SERVER (luôn khởi động bất kể service khác) ---
const PORT = process.env.PORT || 30000;
server.listen(PORT, () => console.log(`🚀 SERVER RUNNING ON PORT ${PORT}`));
