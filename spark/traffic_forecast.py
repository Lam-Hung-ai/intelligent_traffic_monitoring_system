import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
from pyspark.ml import PipelineModel

# Khởi tạo phiên làm việc Spark với cấu hình múi giờ địa phương và tối ưu hóa số lượng phân vùng xử lý
spark = SparkSession.builder \
    .appName("Job2_TrafficPredictor_CorrectTime") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Tải mô hình học máy GBT đã được huấn luyện từ trước; dừng chương trình nếu có lỗi để tránh dự báo sai lệch
model_path = "forecast_traffic_volume/traffic_gbt_model"
try:
    traffic_model = PipelineModel.load(model_path)
    print(">>> Load Model thành công!")
except Exception as e:
    import sys; sys.exit(f"!!! Lỗi load model: {e}")

# Định nghĩa cấu trúc dữ liệu cho luồng đầu vào, khớp với định dạng JSON từ topic 'traffic-aggregated'
input_schema = StructType([
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("cam_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("avg_traffic_volume", DoubleType(), True)
])

# Thiết lập kết nối streaming tới Kafka để tiêu thụ dữ liệu tổng hợp theo thời gian thực
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-aggregated") \
    .option("startingOffsets", "latest") \
    .load()

# Giải mã dữ liệu nhị phân từ Kafka và phân tách các trường thông tin dựa trên schema đã định nghĩa
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), input_schema).alias("data")) \
    .select("data.*")

# Sử dụng cơ chế cửa sổ trượt (Sliding Window) để thu thập chuỗi dữ liệu lịch sử trong 5 phút gần nhất mỗi phút một lần.
# Watermark được áp dụng để quản lý bộ nhớ đệm và xử lý các bản ghi đến chậm trong ngưỡng 1 phút.
windowed_df = parsed_df \
    .withWatermark("end_time", "1 minute") \
    .groupBy(
        F.window(F.col("end_time"), "5 minutes", "1 minute"),
        F.col("cam_id")
    ) \
    .agg(
        F.collect_list(F.struct(
            F.col("end_time"), 
            F.col("avg_traffic_volume")
        )).alias("history")
    )

# Logic xử lý đặc trưng (Feature Engineering): Sắp xếp chuỗi thời gian và bù đắp dữ liệu thiếu (Padding).
# Nếu dữ liệu trong cửa sổ nhỏ hơn 5 mẫu, hệ thống sẽ điền giá trị mặc định để đảm bảo kích thước đầu vào cho mô hình.
@F.udf(returnType=ArrayType(DoubleType()))
def pad_and_extract_volume(history):
    sorted_data = sorted(history, key=lambda x: x['end_time'])
    vols = [x['avg_traffic_volume'] for x in sorted_data]
    
    count = len(vols)
    if count < 5:
        missing = 5 - count
        vols = [20.0] * missing + vols
    
    return vols[-5:]

# Trích xuất các biến trễ (Lag features) từ t-4 đến t để tạo không gian đặc trưng cho mô hình dự báo
features_df = windowed_df \
    .withColumn("padded_vols", pad_and_extract_volume(F.col("history"))) \
    .select(
        F.col("window.end").alias("current_timestamp_t"), 
        F.col("cam_id"),
        F.col("padded_vols")[4].alias("t"),
        F.col("padded_vols")[3].alias("t1"),
        F.col("padded_vols")[2].alias("t2"),
        F.col("padded_vols")[1].alias("t3"),
        F.col("padded_vols")[0].alias("t4")
    )

# Làm giàu dữ liệu bằng cách nhúng các đặc trưng thời gian (Giờ, Thứ) và chuyển đổi lượng giác 
# để mô hình nhận diện được tính chu kỳ của lưu lượng giao thông theo thời gian trong ngày.
final_input_df = features_df \
    .withColumn("local_time_t", F.from_utc_timestamp(F.col("current_timestamp_t"), "Asia/Ho_Chi_Minh")) \
    .withColumn("hour", F.hour("local_time_t")) \
    .withColumn("dayofweek", F.dayofweek("local_time_t")) \
    .withColumn("hour_sin", F.sin(F.col("hour") * (2 * math.pi / 24))) \
    .withColumn("hour_cos", F.cos(F.col("hour") * (2 * math.pi / 24))) \
    .drop("local_time_t")

# Thực hiện dự báo lưu lượng giao thông dựa trên các đặc trưng đã chuẩn bị
predictions = traffic_model.transform(final_input_df)

# Đóng gói kết quả dự báo thành định dạng JSON để gửi về Kafka.
# Thời điểm dự báo được tính tiến lên 5 phút so với thời điểm hiện tại (t+5).
kafka_output_df = predictions.select(
    F.col("cam_id").alias("key"),
    F.to_json(F.struct(
        (F.col("current_timestamp_t") + F.expr("INTERVAL 5 MINUTES")).alias("prediction_timestamp"),
        F.col("current_timestamp_t").alias("base_time_t"),
        F.col("cam_id"),
        F.col("t").alias("current_volume"),
        F.col("prediction").alias("predicted_volume"),
        F.when(F.col("t4") == 20.0, True).otherwise(False).alias("is_padded")
    )).alias("value")
)

# Kích hoạt luồng ghi dữ liệu liên tục sang Kafka topic 'traffic-forecast' với độ trễ tối thiểu (0s)
query = kafka_output_df.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "traffic-forecast") \
    .option("checkpointLocation", "/tmp/checkpoint_traffic_forecast") \
    .trigger(processingTime='0 seconds') \
    .start()

print(">>> Đang dự đoán lưu lượng 5 phút tiếp theo: Gửi kết quả vào 'traffic-forecast'...")
query.awaitTermination()