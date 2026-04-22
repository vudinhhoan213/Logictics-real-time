# Optimization Design

## 1. Mục tiêu module

Module **Route Optimization** chịu trách nhiệm tái tối ưu hóa lộ trình cho xe tải khi hệ thống phát hiện tuyến đường hiện tại không còn hiệu quả do ùn tắc hoặc thời gian di chuyển tăng cao. Module này nhận dữ liệu trạng thái hiện tại của xe, danh sách khách hàng chưa giao và thông tin giao thông thời gian thực từ các thành phần khác của hệ thống, sau đó sử dụng **Genetic Algorithm (GA)** để tìm ra lộ trình mới có tổng thời gian di chuyển nhỏ nhất và tránh các đoạn đường đang tắc nghiêm trọng.

Nói ngắn gọn, đây là thành phần ra quyết định cho bài toán điều phối: khi route cũ không còn tốt, module này sẽ đề xuất route mới tốt hơn.

---

## 2. Nhiệm vụ module

Module Route Optimization có 3 nhiệm vụ chính:

### 2.1. Nhận dữ liệu đầu vào từ hệ thống
Module nhận các thông tin đã được xử lý từ các phần khác, bao gồm:
- **Trạng thái hiện tại của xe tải**: `vehicle_id`, `current_edge_id`, `distance_on_edge`
- **Danh sách khách hàng chưa giao**: `remaining_customers`
- **Thông tin trạng thái đường từ Redis**: `avg_speed`, `estimated_travel_time`, `vehicle_count`, `distance`
- **Danh sách edge cần tránh**: các edge có `avg_speed` rất thấp hoặc bị xem là tắc nghiêm trọng

### 2.2. Tối ưu lại lộ trình
Từ dữ liệu đầu vào, module sẽ:
- Sinh ra nhiều phương án thứ tự giao hàng khác nhau
- Đánh giá từng phương án bằng hàm fitness
- Loại bỏ hoặc phạt nặng những route đi qua edge đang bị tắc nghiêm trọng
- Chọn ra route có chi phí di chuyển thấp nhất

### 2.3. Trả về kết quả cho hệ thống
Kết quả đầu ra của module là:
- **Lộ trình mới** cho xe tải
- Có thể biểu diễn dưới dạng:
  - danh sách khách hàng theo thứ tự giao mới, hoặc
  - danh sách các edge trong route mới (`assigned_route` mới)

Lộ trình mới này sẽ được ghi đè vào MongoDB để frontend và dashboard hiển thị theo thời gian thực.

---

## 3. Phạm vi phần việc

Để tránh chồng chéo với các thành viên khác, cần xác định rõ phạm vi của module Route Optimization.

### 3.1. Những gì module này phụ trách
- Thiết kế bài toán tối ưu route
- Xây dựng mô hình Genetic Algorithm
- Thiết kế chromosome biểu diễn thứ tự giao hàng
- Thiết kế hàm fitness để đánh giá route
- Sinh route mới tối ưu hơn từ dữ liệu đầu vào
- Trả kết quả route mới cho hệ thống

### 3.2. Những gì module này không phụ trách
- Không thu thập dữ liệu GPS từ bot hoặc xe tải
- Không xử lý Kafka
- Không làm map-matching từ GPS sang `edge_id`
- Không cập nhật Redis
- Không xây dựng dashboard hiển thị bản đồ
- Không thiết kế giao diện frontend

Nói cách khác, module này **không tạo ra dữ liệu giao thông**, mà chỉ **sử dụng dữ liệu giao thông đã được xử lý** để ra quyết định tối ưu route.

---

## 4. Vai trò của module trong toàn hệ thống

Trong toàn bộ pipeline của hệ thống, Route Optimization nằm sau bước phân tích giao thông và trước bước cập nhật dữ liệu hiển thị.

Luồng tổng quát:

1. Bot và xe tải gửi GPS vào Kafka  
2. Spark Streaming xử lý dữ liệu và map-matching  
3. Redis lưu trạng thái giao thông của từng edge  
4. Khi route hiện tại của xe đi qua edge có tốc độ quá thấp, hệ thống kích hoạt Route Optimization  
5. Route Optimization tính route mới  
6. Route mới được cập nhật vào MongoDB  
7. Dashboard hiển thị route mới theo thời gian thực

Vì vậy, phần Optimization là **bộ não ra quyết định** của hệ thống điều phối.

---

## 5. Đồng bộ với phần Data Ingestion và hạ tầng hiện tại

Sau khi kiểm tra các file mà thành viên A đã triển khai, có thể chốt một số điểm quan trọng để đảm bảo phần Route Optimization đồng bộ với toàn hệ thống:

### 5.1. Kafka chỉ mang dữ liệu GPS thô
Dữ liệu mà Kafka producer hiện tại gửi lên topic chỉ gồm các trường như `entity_id`, `entity_type`, `latitude`, `longitude`, `speed`, `timestamp`. Điều đó có nghĩa là module Route Optimization **không làm việc trực tiếp với dữ liệu GPS thô**, mà chỉ nhận dữ liệu sau khi đã qua bước map-matching và xử lý giao thông.

### 5.2. Edge là đơn vị cơ bản của route
Phần xử lý bản đồ hiện đang tách dữ liệu OSM thành các edge nhỏ, mỗi edge có các thuộc tính như:
- `edge_id`
- `start_node`
- `end_node`
- `length_meters`
- `max_speed_kmh`
- `road_name`

Vì vậy, để đồng bộ với phần đã làm, Route Optimization sẽ lấy **edge** làm đơn vị chính khi biểu diễn route. Điều này dẫn đến các quyết định thiết kế sau:
- `current_edge_id` là vị trí hiện tại của xe
- `blocked_edges` là danh sách các `edge_id` cần tránh
- `assigned_route` là danh sách `edge_id` theo thứ tự cần đi qua

### 5.3. Dữ liệu giao thông động sẽ đến từ Redis
Redis được dùng làm nơi lưu trạng thái giao thông của từng edge. Khi hoàn thiện đầy đủ pipeline, chi phí di chuyển trên mỗi edge sẽ được lấy từ các trường như `avg_speed` hoặc `estimated_travel_time`.

Trong giai đoạn hiện tại, nếu chưa có đầy đủ dữ liệu từ Redis, module Optimization có thể dùng chi phí tĩnh tạm thời theo công thức:

`travel_cost = length_meters / max_speed_kmh`

Khi hệ thống hoàn chỉnh, chi phí tĩnh này sẽ được thay bằng chi phí động lấy từ Redis.

### 5.4. Optimizer không kết nối trực tiếp Kafka
Do Kafka chỉ mang GPS raw message, module Optimization nên được thiết kế theo hướng:
- nhận input logic từ tầng stream processing hoặc conflict checker
- sử dụng graph đã xử lý sẵn
- đọc trạng thái edge từ Redis
- trả route mới để MongoDB cập nhật

Thiết kế này giúp phần của thành viên C độc lập, rõ ràng và không chồng chéo với phần ingestion hay stream processing.

---

## 6. Input và Output của module

### 6.1. Input chính
Module Route Optimization nhận dữ liệu đầu vào ở mức logic, không phải dữ liệu GPS thô. Một input chuẩn có thể gồm:

```json
{
  "vehicle_id": "Truck_001",
  "current_edge_id": "E_123_456",
  "distance_on_edge": 150.5,
  "remaining_customers": [
    {
      "cust_id": "Cust_001",
      "latitude": 21.012345,
      "longitude": 105.812345
    },
    {
      "cust_id": "Cust_002",
      "latitude": 21.013210,
      "longitude": 105.809876
    }
  ],
  "blocked_edges": [
    "E_222_333",
    "E_444_555"
  ]
}
```

Ý nghĩa các trường:
- `vehicle_id`: định danh xe cần tái tối ưu
- `current_edge_id`: edge hiện tại của xe sau map-matching
- `distance_on_edge`: vị trí tương đối của xe trên edge hiện tại
- `remaining_customers`: danh sách khách còn chưa giao
- `blocked_edges`: các edge cần tránh hoặc bị phạt nặng trong fitness

### 6.2. Input phụ trợ
Ngoài input chính, module còn cần hai nguồn dữ liệu phụ trợ:

#### a. Graph dữ liệu đường đi
Graph được xây từ OSM và edge schema, dùng để:
- xác định kết nối giữa các node
- tìm đường ngắn nhất giữa các điểm
- xây route chi tiết từ thứ tự giao khách

#### b. Trạng thái edge từ Redis
Mỗi edge có thể có thêm dữ liệu giao thông động như:
- `avg_speed`
- `estimated_travel_time`
- `vehicle_count`
- `distance`

Các thông tin này được dùng để tính chi phí di chuyển thực tế trong fitness function.

### 6.3. Output
Kết quả đầu ra của module là route tối ưu mới cho xe tải. Một output chuẩn có thể gồm:

```json
{
  "vehicle_id": "Truck_001",
  "optimized_customer_order": [
    "Cust_002",
    "Cust_001"
  ],
  "new_assigned_route": [
    "E_123_456",
    "E_456_789",
    "E_789_999"
  ],
  "estimated_total_travel_time": 1320.5
}
```

Ý nghĩa các trường:
- `optimized_customer_order`: thứ tự giao khách mới sau tối ưu
- `new_assigned_route`: danh sách `edge_id` của route mới
- `estimated_total_travel_time`: tổng thời gian di chuyển ước lượng

Output này phù hợp để:
- ghi đè vào MongoDB
- hiển thị trên dashboard
- so sánh route cũ và route mới
- debug thuật toán trong quá trình phát triển

---

## 7. Kết luận

Module Route Optimization là thành phần chịu trách nhiệm tái lập kế hoạch giao hàng khi điều kiện giao thông thay đổi. Trong phạm vi đồ án, module này sẽ được xây dựng theo hướng sử dụng Genetic Algorithm để tìm route mới tối ưu hơn dựa trên trạng thái xe hiện tại, danh sách khách hàng chưa giao và dữ liệu giao thông thời gian thực.

Sau khi đối chiếu với phần code ingestion và mô phỏng hiện tại, có thể xác định rõ rằng module này sẽ làm việc ở mức **edge-based routing**, sử dụng dữ liệu đã qua xử lý thay vì GPS raw, và trả về route mới dưới dạng danh sách `edge_id`. Đây là cách thiết kế phù hợp nhất để đảm bảo tính thống nhất với kiến trúc chung của hệ thống.

