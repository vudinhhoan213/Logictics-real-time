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
# tắt các Container nhưng không xóa
docker-compose down