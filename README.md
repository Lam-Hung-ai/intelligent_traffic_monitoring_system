# Intelligent Traffic Monitoring System

Hệ thống giám sát giao thông thông minh sử dụng Big Data (Spark, Kafka) và Deep Learning (YOLO).

## 1. Thành viên nhóm
- Nguyễn Văn Lâm Hùng
- Phan Anh
- Lê Sỹ Long Nhật
- Vũ Trí Trường

## 2. Yêu cầu hệ thống
- **Hệ điều hành:** Ubuntu 24.04+ (hoặc các bản phân phối Linux tương đương)
- **Java:** JDK 21
- **Spark:** 4.1.0
- **Kafka:** 4.1.1 (KRaft mode)
- **Python Manager:** uv

---

## 3. Cài đặt môi trường

### 3.1. Cài đặt Java, Spark và Kafka
Chạy các lệnh sau để cài đặt các thành phần nền tảng vào thư mục `/opt`.

```bash
# Cập nhật hệ thống
sudo apt update && sudo apt upgrade -y
sudo apt install wget curl -y

# 1. Cài đặt Java 21 (LTS)
sudo apt install openjdk-21-jdk -y

# 2. Cài đặt Apache Spark 4.1.0
cd /opt
sudo wget https://dlcdn.apache.org/spark/spark-4.1.0/spark-4.1.0-bin-hadoop3.tgz
sudo tar xvf spark-4.1.0-bin-hadoop3.tgz
sudo mv spark-4.1.0-bin-hadoop3 spark
sudo rm spark-4.1.0-bin-hadoop3.tgz

# 3. Cài đặt Apache Kafka 4.1.1 (KRaft Mode)
sudo wget https://dlcdn.apache.org/kafka/4.1.1/kafka_2.13-4.1.1.tgz
sudo tar xfv kafka_2.13-4.1.1.tgz
sudo mv kafka_2.13-4.1.1 kafka
sudo rm kafka_2.13-4.1.1.tgz

# Cấp quyền sở hữu thư mục cho người dùng hiện tại
sudo chown -R $USER:$USER /opt/spark /opt/kafka
```

### 3.2. Cấu hình Biến môi trường
Thêm cấu hình vào file `~/.bashrc` để có thể sử dụng các lệnh ở bất cứ đâu:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-21-openjdk-amd64' >> ~/.bashrc
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc
echo 'export KAFKA_HOME=/opt/kafka' >> ~/.bashrc
echo 'export PATH=$PATH:$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$KAFKA_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

### 3.3. Cài đặt Python và thư viện (sử dụng `uv`)
Dự án sử dụng `uv` để quản lý thư viện nhanh hơn.

```bash
# Cài đặt uv
curl -LsSf https://astral.sh/uv/install.sh | sh
source $HOME/.cargo/env
source ~/.bashrc

# Khởi tạo môi trường và cài đặt thư viện
uv venv
uv sync
```

---

## 4. Cấu hình và Khởi chạy Kafka

Kafka 4.x sử dụng KRaft thay thế cho Zookeeper.

### 4.1. Cấu hình ban đầu
```bash
# 1. Tạo Cluster ID và định dạng ổ đĩa
KAFKA_CLUSTER_ID="$(kafka-storage.sh random-uuid)"
kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c /opt/kafka/config/server.properties

# 2. Cấu hình giới hạn dung lượng message (> 10 MB cho dữ liệu hình ảnh YOLO)
echo "message.max.bytes=10485760" >> /opt/kafka/config/server.properties
echo "replica.fetch.max.bytes=10485760" >> /opt/kafka/config/server.properties
```

### 4.2. Khởi động Server và Tạo Topic
```bash
# Chạy Kafka Server (Chế độ chạy ngầm - daemon)
kafka-server-start.sh -daemon /opt/kafka/config/server.properties

# Tạo topic cho dữ liệu YOLO
kafka-topics.sh --create --topic yolo-data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

---

## 5. Hướng dẫn chạy hệ thống

Hệ thống hoạt động theo luồng: **Gửi ảnh (Producer) -> Kafka -> Xử lý (Spark Consumer)**.
Bạn cần mở 2 terminal để chạy song song. Chú ý nhớ tải yolov11s.pt về máy tính đặt tại thư mục 

### Terminal 1: Producer (Gửi ảnh)
Gửi dữ liệu hình ảnh từ thư mục `images/` vào Kafka topic `yolo-data`.
```bash
uv run producer_images.py
```

### Terminal 2: Consumer (Spark Streaming & YOLO)
Đọc dữ liệu từ Kafka và thực hiện nhận diện phương tiện.
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 spark_vehicle_counting.py
```

---

## 6. Quy trình làm việc với Git (Dành cho Dev)

### Cài đặt khuyến nghị
- Cài đặt extension **Ruff** trên VSCode để tự động format code chuẩn.

### Các lệnh Git cơ bản
1. **Clone dự án:**
   ```bash
   git clone https://github.com/Lam-Hung-ai/intelligent_traffic_monitoring_system.git
   ```

2. **Tạo nhánh mới để làm việc:**
   ```bash
   git branch ten_cua_ban      # Tạo nhánh mới
   git checkout ten_cua_ban    # Chuyển sang nhánh đó
   ```

3. **Cập nhật code lên Github:**
   ```bash
   git add .
   git commit -m "mô tả thay đổi"
   git push -u origin ten_cua_ban
   ```

4. **Đồng bộ code mới nhất từ main:**
   ```bash
   git pull --no-rebase
   ```
