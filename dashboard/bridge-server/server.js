const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const cors = require("cors");
const mongoose = require("mongoose");
const redis = require("redis"); // 1. Thêm thư viện Redis

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: "http://localhost:5173", methods: ["GET", "POST"] },
});

// --- CẤU HÌNH KẾT NỐI ---
// Khi chạy trong Docker, ta dùng tên service 'mongodb' và 'redis'
const MONGO_URI =
  process.env.MONGO_URI ||
  "mongodb://127.0.0.1:27017/traffic_system?directConnection=true";
const REDIS_URL = process.env.REDIS_HOST
  ? `redis://${process.env.REDIS_HOST}:6379`
  : "redis://127.0.0.1:6379";

// 2. Kết nối Redis
const redisClient = redis.createClient({ url: REDIS_URL });
redisClient.on("error", (err) => console.error("Lỗi Redis:", err));
(async () => {
  await redisClient.connect();
  console.log("Đã kết nối Redis thành công!");
})();

// Kết nối MongoDB
mongoose
  .connect(MONGO_URI)
  .then(() => console.log("Đã kết nối MongoDB thành công!"))
  .catch((err) => console.error("Lỗi kết nối MongoDB:", err.message));

// --- MONGODB CHANGE STREAMS (Lộ trình tối ưu) ---
const RouteSchema = new mongoose.Schema({
  vehicle_id: String,
  new_assigned_route: [String],
  estimated_total_travel_time: Number,
});
const RouteModel = mongoose.model("Route", RouteSchema, "assigned_routes");

RouteModel.watch([], { fullDocument: "updateLookup" }).on(
  "change",
  (change) => {
    const updatedData = change.fullDocument;
    if (updatedData) {
      io.emit("route_optimized", {
        path: updatedData.new_assigned_route,
        time: updatedData.estimated_total_travel_time,
      });
    }
  },
);

// --- REAL-TIME TRAFFIC (Lấy từ Redis) ---
io.on("connection", (socket) => {
  console.log("--- Dashboard connected: " + socket.id + " ---");

  // Thay vì dùng dữ liệu giả, ta sẽ quét Redis định kỳ
  const trafficInterval = setInterval(async () => {
    try {
      // 1. Sửa pattern thành "edge:*" cho khớp với redis_manager.py
      const keys = await redisClient.keys("edge:*");

      for (const key of keys) {
        const rawData = await redisClient.get(key);
        if (!rawData) continue;

        try {
          // 2. Vì dữ liệu là JSON string, ta phải parse nó ra
          const payload = JSON.parse(rawData);
          const edgeId = key.replace("edge:", ""); // Lấy ID gốc

          socket.emit("traffic_update", {
            edge_id: edgeId,
            avg_speed: payload.avg_speed || 0, // Lấy trường avg_speed từ JSON
          });
        } catch (e) {
          console.error("Lỗi parse JSON từ Redis:", e);
        }
      }
    } catch (err) {
      console.error("Lỗi lấy Redis:", err);
    }
  }, 2000);

  socket.on("disconnect", () => {
    clearInterval(trafficInterval);
  });
});

server.listen(4000, () => console.log(`SERVER RUNNING ON PORT 4000`));
