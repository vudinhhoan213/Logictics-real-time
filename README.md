# Logictics-real-time

### Ở trong folder LOGICTICS-REAL-TIME, tạo venv (môi trường ảo) để cài thư viện

# Tạo thư viện

python -m venv venv

# Kích hoạt môi trường ảo

.\venv\Scripts\activate

# Tải các thư viện

pip install confluent-kafka

### Tạo các Container của Docker

# Vào đúng thư mục chứa file docker-compose.yml

cd .\infrastructure\

# Tạo và chạy các Container

docker-compose up -d

# tắt các Container

docker-compose down

### Chạy thử sinh 10.000 bot

# Chạy file bot_simulation.py

python data_ingestion/bot_simulation.py

# Mở Terminal mới và chạy để đưa ra 10 mess từ Kafka

cd .\infrastructure\
docker-compose exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic gps_stream --max-messages 10

### Lệnh chạy tất cả qua Docker

# Vào đúng thư mục chứa file docker-compose.yml

cd .\infrastructure\cd

# Tạo và chạy các Container

docker-compose up -d --build

#### Cách chạy với Kuberneties

# Sau khi cài Kuberneties vào Docker Desktop. Tạo image
docker build -t logictics-bot:latest -f Dockerfile.data_ingestion .
docker build --no-cache -t logictics-backend:v3 -f Dockerfile.backend .
docker build -t logictics-frontend:latest -f Dockerfile.frontend .
docker build -t logictics-opt:latest -f Dockerfile.route_optimization .
docker build -t logictics-stream:latest -f Dockerfile.stream_processing .

# Khởi động K8s
kubectl apply -f k8s/01-infrastructure.yaml

# Lệnh kiểm tra K8s, nếu cả zookeeper, kafka, redis, mongodb đang running thì sang bước tiếp theo
kubectl get pods -w

# Kích hoạt MongoDB qua pod - lấy từ mục name khi chạy lệnh trên
kubectl exec -it mongodb-85d4b65c9d-xg6jj -- mongosh --eval "rs.initiate()"

# Làm tương tự với file thứ yaml thứ 2
kubectl apply -f k8s/02-microservices.yaml
