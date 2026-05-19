# 🚚 Logistics Real-Time Dashboard

Hệ thống giám sát và tối ưu hóa logistics theo thời gian thực.  
**Stack**: Kafka · Spark · Redis · MongoDB · React · Socket.IO · Kubernetes

---

## 📁 Cấu trúc

```
Logictics-real-time/
├── data_ingestion/         # Bot mô phỏng 10k xe → Kafka
├── stream_processing/      # Spark Streaming: map-matching → Redis
├── route_optimization/     # GA tối ưu tuyến đường → MongoDB
├── dashboard/
│   ├── bridge-server/      # Backend (Socket.IO + Kafka + Redis + Mongo)
│   └── frontend/           # React + Leaflet (Vite dev server)
├── infrastructure/         # docker-compose.yml
├── k8s/                    # Kubernetes YAMLs
├── data/                   # edges_schema.json, hanoi_map.osm
└── Dockerfile.*
```

---

## ☸️ Chạy với Kubernetes

### 1. Build images

```bash
docker build --no-cache -t logictics-bot:latest -f Dockerfile.data_ingestion .
docker build --no-cache -t logictics-backend:v6 -f Dockerfile.backend .
docker build --no-cache -t logictics-frontend:v3 -f Dockerfile.frontend .
docker build --no-cache -t logictics-opt:v5 -f Dockerfile.route_optimization .
docker build --no-cache -t logictics-stream:v4 -f Dockerfile.stream_processing .
```

### 2. Deploy infrastructure

```bash
kubectl apply -f k8s/01-infrastructure.yaml
kubectl get pods -w   # Đợi tất cả Running
```

### 3. Khởi tạo MongoDB Replica Set

```bash
# Lấy tên pod
kubectl get pods | findstr mongodb

# Khởi tạo với host = service name (BẮT BUỘC)
kubectl exec -it <mongo-pod> -- mongosh --eval "rs.initiate({_id:'rs0', members:[{_id:0, host:'mongodb:27017'}]})"
```

**Nếu lỗi "already initialized":**

```bash
kubectl exec -it <mongo-pod> -- mongosh --eval "var cfg = rs.conf(); cfg.members[0].host = 'mongodb:27017'; rs.reconfig(cfg, {force: true});"
```

### 4. Deploy microservices

```bash
kubectl apply -f k8s/02-microservices.yaml
kubectl get pods -w   # Đợi tất cả Running
```

### 5. Seed routes + Kích hoạt GA tối ưu lộ trình

```bash
# Lấy tên pod route-optimization
kubectl get pods | findstr route

# Seed 100 trucks vào MongoDB (GA sẽ tự chạy tối ưu sau vài giây)
kubectl exec -it <route-optimization-pod> -- python route_optimization/seed_assigned_routes.py --count 100
```

> 💡 Sau khi seed, đợi ~10s rồi F5 Dashboard để thấy lộ trình tối ưu (polyline màu) hiện trên bản đồ.

### 6. Truy cập Dashboard

```bash
kubectl port-forward svc/dashboard-frontend 3000:5173
```

Mở: **http://localhost:3000/**

---

## 🐳 Chạy với Docker Compose

```bash
cd infrastructure
docker-compose up -d --build
```

Truy cập: **http://localhost:5173/**

Tắt: `docker-compose down`

---

## 🛑 Quản lý K8s

```bash
kubectl scale deployment --all --replicas=0   # Tạm tắt
kubectl scale deployment --all --replicas=1   # Bật lại
kubectl rollout restart deployment/<tên>      # Restart 1 service
kubectl delete -f k8s/02-microservices.yaml   # Xoá services
kubectl delete -f k8s/01-infrastructure.yaml  # Xoá infra
```

---

## 🔧 Troubleshooting

| Vấn đề | Giải pháp |
|--------|-----------|
| MongoDB `ENOTFOUND mongodb-xxx` | Chạy `rs.reconfig` với host `mongodb:27017` (xem bước 3) |
| Spark lỗi Kafka timeout | Kiểm tra env `KAFKA_BROKER` = `kafka.default.svc.cluster.local:9092` |
| Tuyến đường chỉ hiện xanh dương | Stream-processing chưa chạy → check logs: `kubectl logs deploy/stream-processing` |
| Port bị chiếm | `netstat -ano \| findstr :<port>` → `taskkill /F /PID <pid>` |
| WebSocket failed | Bình thường lúc đầu, Socket.IO tự retry |

---

## 📌 Ports

| Service | Port |
|---------|------|
| Frontend | 3000 (K8s) / 5173 (Compose) |
| Backend API | 4000 |
| Kafka | 9092 |
| MongoDB | 27017 |
| Redis | 6379 |
