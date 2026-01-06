import os
import sys
import threading
import yaml
import torch
import numpy as np
import cv2
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, MapType
from ultralytics.models import YOLO

# ==============================================================================
# 1. TẢI CẤU HÌNH TỪ YAML
# ==============================================================================
CONFIG_PATH = "/home/lamhung/code/intelligent_traffic_monitoring_system/config/cameras.yaml"

def load_config():
    if not os.path.exists(CONFIG_PATH):
        print(f"LỖI: Không tìm thấy file cấu hình tại {CONFIG_PATH}")
        sys.exit(1)
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

cfg = load_config()

# Tham số Kafka & MongoDB
KAFKA_SERVERS = cfg.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_NAME = cfg.get("TOPIC_NAME", "traffic-volume")
MONGO_URI = cfg.get("MONGODB_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = cfg.get("MONGODB_DB", "traffic_monitoring")
NUM_WORKERS = cfg.get("NUM_WORKERS", 1)

# Tham số AI
MODEL_NAME = "yolo11s.pt"
# 1: bicycle, 2: car, 3: motorcycle, 5: bus, 7: truck
TARGET_CLASSES = {1: "bicycle", 2: "car", 3: "motorcycle", 5: "bus", 7: "truck"}

# ==============================================================================
# 2. QUẢN LÝ MODEL (Worker Singleton)
# ==============================================================================
def get_model_instance():
    """Đảm bảo model chỉ load 1 lần duy nhất trên mỗi Worker Process"""
    if not hasattr(sys, "_yolo_lock"):
        sys._yolo_lock = threading.Lock()
    
    if not hasattr(sys, "_yolo_model"):
        with sys._yolo_lock:
            if not hasattr(sys, "_yolo_model"):
                device = 'cuda' if torch.cuda.is_available() else 'cpu'
                print(f">>> [PID {os.getpid()}] Đang khởi tạo YOLOv11 trên {device}...")
                model = YOLO(MODEL_NAME)
                model.to(device)
                # Warmup
                model.predict(np.zeros((640, 640, 3), dtype=np.uint8), verbose=False)
                sys._yolo_model = model
                sys._yolo_device = device
    return sys._yolo_model, sys._yolo_device

# ==============================================================================
# 3. LOGIC XỬ LÝ AI (Batch Inference)
# ==============================================================================
def process_batch_iterator(iterator):
    model, device = get_model_instance()
    
    for pdf in iterator:
        decoded_images = []
        valid_indices = []
        
        for idx, row in pdf.iterrows():
            img_bytes = row['value']
            if img_bytes:
                nparr = np.frombuffer(img_bytes, np.uint8)
                img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
                if img is not None:
                    decoded_images.append(img)
                    valid_indices.append(idx)
        
        if not decoded_images:
            continue

        # Chạy dự đoán cho cả batch ảnh
        results = model.predict(
            decoded_images, 
            device=device, 
            classes=list(TARGET_CLASSES.keys()), 
            verbose=False,
            conf=0.25
        )
        
        batch_results = []
        for i, res in enumerate(results):
            counts = {name: 0 for name in TARGET_CLASSES.values()}
            if res.boxes:
                cls_ids = res.boxes.cls.cpu().numpy().astype(int)
                for cid in cls_ids:
                    if cid in TARGET_CLASSES:
                        counts[TARGET_CLASSES[cid]] += 1
            
            # Lọc các class có số lượng > 0
            detail = {k: v for k, v in counts.items() if v > 0}
            orig_row = pdf.iloc[valid_indices[i]]
            
            batch_results.append({
                "cam_id": str(orig_row['cam_id']),
                "location": str(orig_row['location']),
                "timestamp": str(orig_row['timestamp']),
                "traffic_volume": int(sum(detail.values())),
                "detail": detail
            })
            
        yield pd.DataFrame(batch_results)

# ==============================================================================
# 4. GHI DỮ LIỆU VÀO MONGODB (Location-based Collection)
# ==============================================================================
def save_to_mongodb(df, batch_id):
    if df.count() == 0:
        return

    # Chuyển về Driver để xử lý phân loại collection
    rows = df.collect()
    
    # Nhóm dữ liệu theo location (Sạch hóa tên collection)
    data_by_loc = {}
    for r in rows:
        d = r.asDict()
        # Chuẩn hóa tên collection: "Bai do xe" -> "bai_do_xe"
        loc_key = d['location'].strip().lower().replace(" ", "_")
        
        # Chuyển String timestamp -> Datetime object cho MongoDB
        try:
            d['timestamp'] = datetime.strptime(d['timestamp'], "%Y-%m-%d %H:%M:%S")
        except:
            d['timestamp'] = datetime.now()
            
        if loc_key not in data_by_loc:
            data_by_loc[loc_key] = []
        data_by_loc[loc_key].append(d)

    # Kết nối MongoDB
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    
    try:
        for loc, docs in data_by_loc.items():
            collection = db[loc]
            # Đảm bảo Index (MongoDB sẽ tự bỏ qua nếu đã tồn tại)
            collection.create_index([("timestamp", -1)])
            collection.create_index("timestamp", expireAfterSeconds=2592000) # 30 ngày
            
            collection.insert_many(docs)
            print(f">>> [Batch {batch_id}] Đã ghi {len(docs)} records vào collection: {loc}")
    except Exception as e:
        print(f"LỖI ghi MongoDB: {e}")
    finally:
        client.close()

# ==============================================================================
# 5. CẤU TRÚC PIPELINE SPARK
# ==============================================================================
SCHEMA_OUT = StructType([
    StructField("cam_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("traffic_volume", IntegerType(), True),
    StructField("detail", MapType(StringType(), IntegerType()), True)
])

def main():
    spark = SparkSession.builder \
        .appName("Traffic_AI_MongoDB_Streaming") \
        .config("spark.sql.shuffle.partitions", str(NUM_WORKERS)) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Đọc luồng từ Kafka
    raw_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
        .option("subscribe", TOPIC_NAME) \
        .option("includeHeaders", "true")   \
        .option("maxOffsetsPerTrigger", 200) \
        .load()

    # 2. Giải mã Metadata từ Headers
    # Headers thường ở dạng Binary, cần decode sang UTF-8
    parsed_stream = raw_stream.select(
        F.expr("map_from_entries(transform(headers, x -> struct(x.key, decode(x.value, 'UTF-8'))))").alias("h"),
        F.col("value")
    ).select(
        F.col("h").getItem("cam_id").alias("cam_id"),
        F.col("h").getItem("location").alias("location"),
        F.col("h").getItem("timestamp").alias("timestamp"),
        "value"
    ).filter("cam_id IS NOT NULL")


    # 3. Phân phối và Chạy YOLO
    # Repartition đảm bảo tận dụng đúng số lượng Worker/GPU đã cấu hình
    results_stream = parsed_stream.repartition(NUM_WORKERS) \
        .mapInPandas(process_batch_iterator, schema=SCHEMA_OUT)

    # 4. Ghi kết quả vào MongoDB theo từng Location
    query = results_stream.writeStream \
        .foreachBatch(save_to_mongodb) \
        .option("checkpointLocation", f"/tmp/checkpoint_traffic_{MONGO_DB_NAME}") \
        .start()

    print(f"Hệ thống đang chạy... Đang lắng nghe topic: {TOPIC_NAME}")
    query.awaitTermination()

if __name__ == "__main__":
    main()