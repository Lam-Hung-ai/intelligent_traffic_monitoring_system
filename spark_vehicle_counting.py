from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType, IntegerType, MapType # Thêm MapType
import pandas as pd
from ultralytics.models import YOLO
import io
from PIL import Image
import numpy as np
from collections import Counter

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Distributed-Vehicle-Counter") \
    .getOrCreate()

# Danh sách các lớp đối tượng mục tiêu
VEHICLE_CLASSES = ['car', 'motorcycle', 'bus', 'truck', 'bicycle']

# 2. Định nghĩa hàm xử lý phân tán
# Thay đổi return type thành MapType để trả về dictionary
@pandas_udf(MapType(StringType(), IntegerType()))
def count_vehicles_udf(binary_series: pd.Series) -> pd.Series:
    # Tải mô hình YOLOv11
    model = YOLO("yolo11s.pt") 
    
    results_list = []
    for img_bytes in binary_series:
        try:
            # Chuyển Byte thành ảnh
            img = Image.open(io.BytesIO(img_bytes))
            img_array = np.array(img)
            
            # Chạy nhận diện
            results = model.predict(img_array, conf=0.4, verbose=False)
            
            # Thu thập tên các loại phương tiện xuất hiện trong ảnh
            detected_vehicles = []
            for box in results[0].boxes:
                class_id = int(box.cls[0])
                class_name = results[0].names[class_id]
                
                if class_name in VEHICLE_CLASSES:
                    detected_vehicles.append(class_name)
            
            # Đếm số lượng từng loại: {'car': 5, 'bicycle': 3}
            counts = dict(Counter(detected_vehicles))
            results_list.append(counts)
            
        except Exception as e:
            # Nếu lỗi, trả về dict rỗng
            results_list.append({})
            
    return pd.Series(results_list)

# 3. Kết nối với luồng dữ liệu Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "yolo-data") \
    .option("startingOffsets", "latest") \
    .load()

# 4. Trích xuất dữ liệu và thực hiện đếm
# Kết quả cột "counts" bây giờ sẽ là một Map (Dictionary)
df_result = df_kafka.select(
    count_vehicles_udf(col("value")).alias("counts")
)

# 5. Xuất kết quả ra Console
query = df_result.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime='2 seconds') \
    .start()

print(">>>> Hệ thống đang sẵn sàng. Đang chờ ảnh từ Kafka...")
query.awaitTermination()