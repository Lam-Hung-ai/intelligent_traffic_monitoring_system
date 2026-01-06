import os
import sys
import threading
import json
import yaml
import torch
import numpy as np
import cv2
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
from ultralytics.models import YOLO

# ==============================================================================
# 1. CẤU HÌNH HỆ THỐNG & KHỞI TẠO THAM SỐ
# ==============================================================================
CONFIG_PATH = "./config/cameras.yaml"

def load_config():
    """Tải cấu hình từ file YAML, hỗ trợ giá trị mặc định nếu file không tồn tại."""
    if not os.path.exists(CONFIG_PATH):
        return {}
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

# Khởi tạo đối tượng cấu hình toàn cục
cfg = load_config()

# Cấu hình kết nối Kafka
KAFKA_SERVERS = cfg.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN = cfg.get("TOPIC_NAME", "traffic-volume")   # Topic nguồn chứa dữ liệu hình ảnh
TOPIC_OUT = "traffic-raw"                            # Topic đích chứa kết quả xử lý JSON
NUM_WORKERS = cfg.get("NUM_WORKERS", 1)              # Số lượng đơn vị xử lý song song

# Cấu hình mô hình AI nhận diện
MODEL_NAME = "yolo11s.pt"
# Danh mục các loại phương tiện cần phân loại (ID dựa trên tập dữ liệu COCO)
TARGET_CLASSES = {1: "bicycle", 2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}

# ==============================================================================
# 2. QUẢN LÝ MÔ HIỂN AI (Singleton Pattern cho Worker)
# ==============================================================================
def get_model_instance():
    """
    Đảm bảo mô hình YOLO chỉ được nạp một lần duy nhất trên mỗi tiến trình (Worker).
    Sử dụng cơ chế Thread-safe để tránh xung đột khi khởi tạo.
    """
    # Khởi tạo khóa bảo vệ luồng nếu chưa tồn tại
    if not hasattr(sys, "_yolo_lock"):
        sys._yolo_lock = threading.Lock()
    
    # Kiểm tra sự tồn tại của mô hình theo cơ chế Double-checked locking
    if not hasattr(sys, "_yolo_model"):
        with sys._yolo_lock:
            if not hasattr(sys, "_yolo_model"):
                # Ưu tiên sử dụng GPU (CUDA) để tăng tốc độ xử lý
                device = 'cuda' if torch.cuda.is_available() else 'cpu'
                print(f">>> [PID {os.getpid()}] Initializing YOLOv11 on {device}...")
                
                model = YOLO(MODEL_NAME)
                model.to(device)
                
                # Thực hiện 'Warmup' để tối ưu hóa bộ nhớ và tốc độ cho các lượt dự đoán đầu tiên
                model.predict(np.zeros((640, 640, 3), dtype=np.uint8), verbose=False)
                
                # Lưu trữ instance vào hệ thống để tái sử dụng
                sys._yolo_model = model
                sys._yolo_device = device
                
    return sys._yolo_model, sys._yolo_device

# ==============================================================================
# 3. LOGIC XỬ LÝ DỮ LIỆU TẬP TRUNG (AI Inference to JSON)
# ==============================================================================
# Định nghĩa cấu trúc dữ liệu đầu ra tương thích với định dạng Kafka (Key-Value)
SCHEMA_KAFKA_OUT = StructType([
    StructField("key", StringType(), True),   # Định danh camera (Dùng để phân vùng Kafka)
    StructField("value", StringType(), True)  # Chuỗi JSON chứa thông tin chi tiết
])

def process_images_to_json(iterator):
    """
    Xử lý dữ liệu ảnh theo lô (Batch) bằng Pandas UDF để tối ưu hiệu năng trên Spark.
    """
    model, device = get_model_instance()
    SUB_BATCH_SIZE = 20  # Kích thước lô nhỏ để cân bằng giữa tốc độ và bộ nhớ VRAM
    
    for pdf in iterator:
        images = []
        valid_indices = []
        
        # Bước 1: Giải mã dữ liệu nhị phân (Binary) sang định dạng ảnh OpenCV
        for idx, row in pdf.iterrows():
            img_bytes = row['value']
            # Chuyển đổi buffer thành mảng numpy và decode sang ảnh màu
            img = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
            
            if img is not None:
                images.append(img)
                valid_indices.append(idx)
        
        # Trả về DataFrame rỗng nếu lô dữ liệu không chứa ảnh hợp lệ
        if not images:
            yield pd.DataFrame(columns=SCHEMA_KAFKA_OUT.names)
            continue

        # Bước 2: Thực thi nhận diện đối tượng (Inference) theo Batch
        all_results = []
        for i in range(0, len(images), SUB_BATCH_SIZE):
            img_chunk = images[i : i + SUB_BATCH_SIZE]
            res_chunk = model.predict(
                img_chunk, 
                verbose=False, 
                device=device, 
                classes=list(TARGET_CLASSES.keys()),
                conf=0.25
            )
            all_results.extend(res_chunk)
            
            # Giải phóng bộ nhớ đệm GPU sau mỗi chu kỳ xử lý
            if device == 'cuda': torch.cuda.empty_cache()
        
        # Bước 3: Tổng hợp kết quả và đóng gói thành định dạng JSON
        output_rows = []
        for i, res in enumerate(all_results):
            # Truy xuất thông tin metadata gốc của ảnh
            orig = pdf.iloc[valid_indices[i]]
            
            # Thống kê số lượng từng loại phương tiện
            counts = {name: 0 for name in TARGET_CLASSES.values()}
            if res.boxes:
                classes = res.boxes.cls.cpu().numpy().astype(int)
                for cls_id in classes:
                    if cls_id in TARGET_CLASSES:
                        counts[TARGET_CLASSES[cls_id]] += 1
            
            # Chỉ giữ lại các loại phương tiện có xuất hiện trong khung hình
            counts = {k: v for k, v in counts.items() if v > 0}
            
            # Xây dựng cấu trúc bản ghi dữ liệu cuối cùng
            record = {
                "cam_id": str(orig['cam_id']),
                "location": str(orig['location']),
                "timestamp": str(orig['timestamp']),
                "traffic_volume": int(sum(counts.values())),
                "detail": counts 
            }
            
            # Chuẩn bị bản ghi cho Kafka: Key định danh và Value là nội dung JSON
            output_rows.append({
                "key": str(orig['cam_id']),
                "value": json.dumps(record)
            })
            
        yield pd.DataFrame(output_rows)

# ==============================================================================
# 4. LUỒNG XỬ LÝ CHÍNH (MAIN PIPELINE)
# ==============================================================================
def main():
    # Khởi tạo Spark Session tích hợp Kafka
    spark = SparkSession.builder \
        .appName("Job1_Traffic_Vision_YOLO") \
        .config("spark.sql.shuffle.partitions", str(NUM_WORKERS)) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # --- GIAI ĐOẠN 1: ĐỌC DỮ LIỆU STREAMING TỪ KAFKA ---
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", TOPIC_IN) \
        .option("maxOffsetsPerTrigger", 50) \
        .option("includeHeaders", "true") \
        .option("failOnDataLoss", "false") \
        .load()

    # Phân tách Headers để trích xuất metadata (cam_id, location, timestamp)
    input_df = raw_stream.select(
        F.expr("map_from_entries(transform(headers, x -> struct(x.key, decode(x.value, 'UTF-8'))))").alias("h"),
        "value"
    ).select(
        F.col("h").getItem("cam_id").alias("cam_id"),
        F.col("h").getItem("location").alias("location"),
        F.col("h").getItem("timestamp").alias("timestamp"),
        "value" # Dữ liệu ảnh nhị phân
    ).filter("cam_id IS NOT NULL")

    # --- GIAI ĐOẠN 2: THỰC THI AI VÀ BIẾN ĐỔI DỮ LIỆU ---
    # Phân phối dữ liệu tới các Worker và áp dụng mô hình YOLO
    json_stream = input_df.repartition(NUM_WORKERS) \
        .mapInPandas(process_images_to_json, schema=SCHEMA_KAFKA_OUT)

    # --- GIAI ĐOẠN 3: XUẤT KẾT QUẢ RA KAFKA TOPIC ---
    # Tự động ghi cột "key" và "value" vào Kafka Topic đích
    query = json_stream.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("topic", TOPIC_OUT) \
        .option("checkpointLocation", "/tmp/checkpoint_traffic_monitor") \
        .start()

    print(f">>> Pipeline started: Monitoring '{TOPIC_IN}' -> AI Detection -> Outputting to '{TOPIC_OUT}'")
    query.awaitTermination()

if __name__ == "__main__":
    main()