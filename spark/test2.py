from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("TrafficAverageCalculator") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# 2. Định nghĩa Schema cho dữ liệu JSON
# Chúng ta chỉ cần lấy các trường cần thiết, bỏ qua 'detail' nếu không dùng
json_schema = StructType([
    StructField("cam_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True), # Đọc là String trước, convert sau
    StructField("traffic_volume", IntegerType(), True)
])

# 3. Đọc dữ liệu từ Kafka (Stream)
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-raw") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Chuyển đổi dữ liệu: Binary -> String -> Columns
parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*")

# 5. Chuẩn hóa thời gian và Xử lý nghiệp vụ
# Chuyển string timestamp sang kiểu Timestamp thực sự
traffic_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp")))

# --- PHẦN QUAN TRỌNG NHẤT: WINDOWING & AGGREGATION ---
windowed_counts = traffic_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("cam_id"),
        col("location")
    ) \
    .agg(avg("traffic_volume").alias("avg_traffic_volume"))

# Tối ưu hóa Trigger xuống 0 giây (chạy nhanh nhất có thể)
query = windowed_counts.writeStream \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query.awaitTermination()