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
cd .\infrastructure\
# Tạo và chạy các Container
docker-compose up -d --build
