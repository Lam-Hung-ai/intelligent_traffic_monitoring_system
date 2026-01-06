import os
import sys
import threading
import json  # Thư viện để đóng gói JSON
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
# 1. CẤU HÌNH & THAM SỐ
# ==============================================================================
CONFIG_PATH = "./config/cameras.yaml"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        # Fallback nếu không có file config để test
        return {}
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

cfg = load_config()

# Kafka Config
KAFKA_SERVERS = cfg.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_IN = cfg.get("TOPIC_NAME", "traffic-volume")   # Đầu vào: Ảnh
TOPIC_OUT = "traffic-raw"                            # Đầu ra: JSON số liệu
NUM_WORKERS = cfg.get("NUM_WORKERS", 1)

# AI Config
MODEL_NAME = "yolo11s.pt"
TARGET_CLASSES = {1: "bicycle", 2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}

# ==============================================================================
# 2. QUẢN LÝ MODEL (Worker Singleton)
# ==============================================================================
def get_model_instance():
    """Load model 1 lần duy nhất trên mỗi Worker"""
    if not hasattr(sys, "_yolo_lock"):
        sys._yolo_lock = threading.Lock()
    
    if not hasattr(sys, "_yolo_model"):
        with sys._yolo_lock:
            if not hasattr(sys, "_yolo_model"):
                device = 'cuda' if torch.cuda.is_available() else 'cpu'
                print(f">>> [PID {os.getpid()}] Init YOLOv11 on {device}...")
                model = YOLO(MODEL_NAME)
                model.to(device)
                # Warmup
                model.predict(np.zeros((640, 640, 3), dtype=np.uint8), verbose=False)
                sys._yolo_model = model
                sys._yolo_device = device
    return sys._yolo_model, sys._yolo_device

# ==============================================================================
# 3. LOGIC XỬ LÝ AI -> JSON (Output schema cho Kafka)
# ==============================================================================
# Kafka yêu cầu Output DataFrame phải có cột "key" và "value" (dạng String/Binary)
SCHEMA_KAFKA_OUT = StructType([
    StructField("key", StringType(), True),   # Dùng cam_id làm key
    StructField("value", StringType(), True)  # JSON String
])

def process_images_to_json(iterator):
    model, device = get_model_instance()
    SUB_BATCH_SIZE = 20
    
    for pdf in iterator:
        images = []
        valid_indices = []
        
        # 1. Decode ảnh từ binary
        for idx, row in pdf.iterrows():
            img_bytes = row['value']
            # Decode ảnh
            img = cv2.imdecode(np.frombuffer(img_bytes, np.uint8), cv2.IMREAD_COLOR)
            
            if img is not None:
                images.append(img)
                valid_indices.append(idx)
        
        if not images:
            yield pd.DataFrame(columns=SCHEMA_KAFKA_OUT.names)
            continue

        # 2. Chạy Inference (Batch Processing)
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
            if device == 'cuda': torch.cuda.empty_cache()
        
        # 3. Đóng gói kết quả thành JSON
        output_rows = []
        for i, res in enumerate(all_results):
            # Lấy thông tin gốc
            orig = pdf.iloc[valid_indices[i]]
            
            # Đếm số lượng
            counts = {name: 0 for name in TARGET_CLASSES.values()}
            if res.boxes:
                classes = res.boxes.cls.cpu().numpy().astype(int)
                for cls_id in classes:
                    if cls_id in TARGET_CLASSES:
                        counts[TARGET_CLASSES[cls_id]] += 1
            counts = {k: v for k, v in counts.items() if v > 0}
            # Tạo dictionary dữ liệu
            record = {
                "cam_id": str(orig['cam_id']),
                "location": str(orig['location']),
                "timestamp": str(orig['timestamp']),
                "traffic_volume": int(sum(counts.values())),
                "detail": counts  # VD: {"car": 5, "bus": 1...}
            }
            
            # Kafka Record: Key = cam_id, Value = JSON String
            output_rows.append({
                "key": str(orig['cam_id']),
                "value": json.dumps(record) # Serialize thành chuỗi JSON
            })
            
        yield pd.DataFrame(output_rows)

# ==============================================================================
# 4. MAIN PIPELINE
# ==============================================================================
def main():
    spark = SparkSession.builder \
        .appName("Job1_Traffic_Vision_YOLO") \
        .config("spark.sql.shuffle.partitions", str(NUM_WORKERS)) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    # --- BƯỚC 1: ĐỌC ẢNH TỪ KAFKA ---
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", TOPIC_IN) \
        .option("maxOffsetsPerTrigger", 50) \
        .option("includeHeaders", "true") \
        .option("failOnDataLoss", "false") \
        .load()

    # Giải mã Headers để lấy metadata
    input_df = raw_stream.select(
        F.expr("map_from_entries(transform(headers, x -> struct(x.key, decode(x.value, 'UTF-8'))))").alias("h"),
        "value"
    ).select(
        F.col("h").getItem("cam_id").alias("cam_id"),
        F.col("h").getItem("location").alias("location"),
        F.col("h").getItem("timestamp").alias("timestamp"),
        "value" # Binary Image
    ).filter("cam_id IS NOT NULL")

    # --- BƯỚC 2: XỬ LÝ AI & CHUYỂN THÀNH JSON ---
    # mapInPandas trả về DataFrame có cột [key, value]
    json_stream = input_df.repartition(NUM_WORKERS) \
        .mapInPandas(process_images_to_json, schema=SCHEMA_KAFKA_OUT)

    # --- BƯỚC 3: GHI VÀO KAFKA (OUTPUT) ---
    # Spark Kafka Writer tự động nhận diện cột "key" và "value"
    query = json_stream.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("topic", TOPIC_OUT) \
        .option("checkpointLocation", "/tmp/checkpoint_job1_vision") \
        .start()

    print(f">>> Job 1 đang chạy: Nhận ảnh từ '{TOPIC_IN}' -> Detect -> Gửi JSON vào '{TOPIC_OUT}'")
    query.awaitTermination()

if __name__ == "__main__":
    main()