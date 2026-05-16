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
docker build -t logictics-bot:latest -f Dockerfile.data_ingestion .
docker build --no-cache -t logictics-backend:v5 -f Dockerfile.backend .
docker build -t logictics-frontend:v2 -f Dockerfile.frontend .
docker build -t logictics-opt:v4 -f Dockerfile.route_optimization .
docker build -t logictics-stream:v4 -f Dockerfile.stream_processing .
```

### 2. Deploy infrastructure

```bash
kubectl apply -f k8s/01-infrastructure.yaml
kubectl get pods -w   # Đợi tất cả Running
```

### 3. Khởi tạo MongoDB Replica Set

```bash
kubectl exec -it <mongo-pod> -- mongosh --eval "rs.initiate({_id:'rs0', members:[{_id:0, host:'mongodb:27017'}]})"
```

> ⚠️ **Bắt buộc** phải chỉ định `host:'mongodb:27017'` (tên Service). Nếu không, backend sẽ lỗi `ENOTFOUND` do MongoDB đăng ký pod name.

**Nếu đã initiate rồi** (lỗi "already initialized"):

```bash
kubectl exec -it <mongo-pod> -- mongosh --eval "var cfg = rs.conf(); cfg.members[0].host = 'mongodb:27017'; rs.reconfig(cfg, {force: true});"
```

### 4. Deploy microservices

```bash
kubectl apply -f k8s/02-microservices.yaml
```

### 5. Truy cập Dashboard

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
kubectl delete -f k8s/02-microservices.yaml   # Xoá services
kubectl delete -f k8s/01-infrastructure.yaml  # Xoá infra
```

---

## 📌 Ports

| Service | Port |
|---------|------|
| Frontend | 3000 (K8s) / 5173 (Compose) |
| Backend API | 4000 |
| Kafka | 9092 |
| MongoDB | 27017 |
| Redis | 6379 |
