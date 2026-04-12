const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const mongoose = require('mongoose');

const app = express();
app.use(cors());

const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "http://localhost:5173", methods: ["GET", "POST"] }
});

// Kết nối MongoDB
mongoose.connect('mongodb://127.0.0.1:27017/traffic_system?directConnection=true')
  .then(() => {
    console.log('Đã kết nối MongoDB thành công!');
  })
  .catch(err => {
    console.error('Lỗi kết nối MongoDB:', err.message);
  });

// Schema
const RouteSchema = new mongoose.Schema({
    vehicle_id: String,
    new_assigned_route: [String],
    estimated_total_travel_time: Number
});

const RouteModel = mongoose.model('Route', RouteSchema, 'assigned_routes');

// FIX QUAN TRỌNG: thêm fullDocument: 'updateLookup'
const watchRoutes = () => {
    const changeStream = RouteModel.watch([], {
        fullDocument: 'updateLookup'
    });

    changeStream.on('change', (change) => {
        console.log("CHANGE:", change);

        const updatedData = change.fullDocument;

        if (!updatedData) {
            console.log("Không có fullDocument");
            return;
        }

        console.log("EMIT:", {
            path: updatedData.new_assigned_route,
            time: updatedData.estimated_total_travel_time
        });

        io.emit('route_optimized', {
            path: updatedData.new_assigned_route,
            time: updatedData.estimated_total_travel_time
        });
    });
};

watchRoutes();

// Giả lập Redis (traffic)
const sampleEdgeIds = [
  "E_6658453755_12568521746",
  "E_12568521746_12951168004",
  "E_12951168004_12951168005",
  "E_12951168005_8234749117",
  "E_8234749117_8234749153",
  "E_8234749153_8234749157",
  "E_8234749157_6657957501",
  "E_6657957501_81804024",
  "E_81804024_12584288413",
  "E_12584288413_11861155229",
  "E_11861155229_941531824",
  "E_941531824_7202796991",
  "E_7202796991_11124244869",
  "E_11124244869_11861399707",
  "E_11861399707_81804025",
  "E_81804025_1497544849",
  ];

io.on('connection', (socket) => {
    console.log('--- Dashboard connected: ' + socket.id + ' ---');

    const simulation = setInterval(() => {
        const randomId = sampleEdgeIds[Math.floor(Math.random() * sampleEdgeIds.length)];
        const randomSpeed = Math.floor(Math.random() * 40);

        socket.emit('traffic_update', { edge_id: randomId, avg_speed: randomSpeed });
    }, 1000);

    socket.on('disconnect', () => {
        clearInterval(simulation);
    });
});

server.listen(4000, () => console.log(`SERVER RUNNING ON PORT 4000`));
