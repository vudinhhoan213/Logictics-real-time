const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const mongoose = require("mongoose");
const redis = require("redis");
const { Kafka } = require("kafkajs");
const PathFinder = require("./pathfinder");

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] },
});

// --- CẤU HÌNH KẾT NỐI (K8s Service names) ---
// --- PATHFINDER (tính route shortest-path on-demand) ---
const pathfinder = new PathFinder();
const EDGES_PATH = process.env.EDGES_JSON || "/app/data/edges_schema.json";
try {
  pathfinder.load(EDGES_PATH);
} catch (err) {
  console.warn("⚠️ PathFinder: Không load được edges:", err.message);
}

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
  assigned_route: [String],
  new_assigned_route: [String],
  estimated_total_travel_time: Number,
}), "assigned_routes");

function setupChangeStreams() {
  try {
    RouteModel.watch([], { fullDocument: "updateLookup" }).on("change", (change) => {
      const updatedData = change.fullDocument;
      if (updatedData) {
        const path = (updatedData.new_assigned_route && updatedData.new_assigned_route.length > 0)
          ? updatedData.new_assigned_route
          : updatedData.assigned_route || [];
        io.emit("route_optimized", {
          vehicle_id: updatedData.vehicle_id,
          path,
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
    // Query tất cả xe có route (new_assigned_route HOẶC assigned_route)
    const docs = await RouteModel.find({
      vehicle_id: { $exists: true, $nin: [null, ""] },
      $or: [
        { "new_assigned_route.0": { $exists: true } },
        { "assigned_route.0": { $exists: true } },
      ]
    })
      .select("vehicle_id assigned_route new_assigned_route estimated_total_travel_time")
      .lean();
    const payload = docs
      .filter((d) => d.vehicle_id)
      .map((d) => ({
        vehicle_id: d.vehicle_id,
        old_path: d.assigned_route || [],
        new_path: d.new_assigned_route || [],
        path: (d.new_assigned_route && d.new_assigned_route.length > 0)
          ? d.new_assigned_route : (d.assigned_route || []),
        time: d.estimated_total_travel_time,
      }))
      .filter((d) => d.path.length > 0);
    socket.emit("routes_snapshot", payload);
  } catch (e) {
    console.error("routes_snapshot:", e.message);
  }
}

// --- REAL-TIME TRAFFIC (Redis Polling) ---
io.on("connection", (socket) => {
  console.log("📡 Dashboard connected: " + socket.id);

  emitRoutesSnapshot(socket);

  // --- REQUEST ROUTE ON-DEMAND ---
  // Frontend gửi event khi user chọn xe → tính shortest path realtime
  socket.on("request_route", async (data) => {
    try {
      const { vehicle_id, lat, lon } = data;
      if (!vehicle_id || lat === undefined || lon === undefined) {
        socket.emit("route_result", { vehicle_id, error: "Missing lat/lon" });
        return;
      }

      // Lấy remaining_customers từ MongoDB
      let customers = data.customers || [];
      if (customers.length === 0 && mongoReady) {
        const doc = await RouteModel.findOne({ vehicle_id }).lean();
        if (doc && doc.remaining_customers) {
          customers = doc.remaining_customers;
        }
      }

      if (customers.length === 0) {
        socket.emit("route_result", { vehicle_id, path: [], time: 0, error: "No customers" });
        return;
      }

      if (!pathfinder.loaded) {
        socket.emit("route_result", { vehicle_id, path: [], time: 0, error: "PathFinder not loaded" });
        return;
      }

      // Tính shortest path liên tục từ vị trí xe → qua tất cả customer
      const result = pathfinder.buildRoute(lat, lon, customers);

      socket.emit("route_result", {
        vehicle_id,
        path: result.path,
        time: Math.round(result.totalCost), // seconds
        customers,
      });
      console.log(`🗺️ Route calculated for ${vehicle_id}: ${result.path.length} edges, ${Math.round(result.totalCost)}s`);
    } catch (err) {
      console.error("request_route error:", err.message);
      socket.emit("route_result", { vehicle_id: data?.vehicle_id, path: [], error: err.message });
    }
  });

  const trafficInterval = setInterval(async () => {
    if (!redisReady) return; // Bỏ qua nếu Redis chưa sẵn sàng
    try {
      // Dùng KEYS + GET từng key (ổn định, tránh lỗi Buffer với scanIterator/mGet)
      const keys = await redisClient.keys("edge:*");
      if (keys && keys.length > 0) {
        const trafficBatch = [];
        for (const key of keys) {
          try {
            const rawData = await redisClient.get(String(key));
            if (rawData) {
              const payload = JSON.parse(rawData);
              trafficBatch.push({ edge_id: String(key).replace("edge:", ""), avg_speed: payload.avg_speed || 0 });
            }
          } catch (_) { /* skip */ }
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
