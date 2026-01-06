from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Thiết lập môi trường Spark và cấu hình phân vùng dữ liệu để tối ưu hóa việc xáo trộn (shuffle) dữ liệu trong cụm
spark = SparkSession.builder \
    .appName("Job1_TrafficAggregator") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Khai báo cấu trúc dữ liệu mong đợi từ Kafka để Spark có thể ánh xạ chính xác các trường dữ liệu từ JSON
raw_schema = StructType([
    StructField("cam_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("traffic_volume", IntegerType(), True)
])

# Thiết lập kết nối streaming tới Kafka, thu thập dữ liệu thô từ topic đầu vào từ thời điểm sớm nhất có thể
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-raw") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển đổi dữ liệu nhị phân từ Kafka sang chuỗi, sau đó giải mã JSON dựa trên Schema và chuẩn hóa kiểu dữ liệu thời gian
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), raw_schema).alias("data")) \
    .select(
        col("data.cam_id"),
        col("data.location"),
        to_timestamp(col("data.timestamp")).alias("timestamp"),
        col("data.traffic_volume")
    )

# Áp dụng cơ chế cửa sổ thời gian (Windowing) để gom nhóm dữ liệu theo từng phút. 
# Watermark được thiết lập để xử lý dữ liệu đến trễ và cho phép hệ thống giải phóng trạng thái cũ sau ngưỡng 10 giây.
agg_df = parsed_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("cam_id"),
        col("location")
    ) \
    .agg(avg("traffic_volume").alias("avg_traffic_volume"))

# Tái cấu trúc dữ liệu sau tổng hợp thành định dạng Key-Value chuẩn của Kafka.
# Toàn bộ thông tin tính toán được đóng gói vào một chuỗi JSON duy nhất trong cột 'value'.
kafka_output_df = agg_df.select(
    col("cam_id").alias("key"),
    to_json(struct(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("cam_id"),
        col("location"),
        col("avg_traffic_volume")
    )).alias("value")
)

# Thực thi tiến trình Streaming, đẩy dữ liệu tổng hợp trở lại Kafka định kỳ mỗi 10 giây.
# Sử dụng cơ chế checkpoint để đảm bảo khả năng phục hồi và tính toàn vẹn của luồng dữ liệu khi có sự cố.
query = kafka_output_df.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "traffic-aggregated") \
    .option("checkpointLocation", "/tmp/checkpoint_traffic_aggregated") \
    .trigger(processingTime='10 seconds') \
    .start()

print(">>> Đang tổng hợp dữ liệu: Gửi dữ liệu tổng hợp vào 'traffic-aggregated'...")
query.awaitTermination()