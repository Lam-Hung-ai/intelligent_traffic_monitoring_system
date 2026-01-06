# ==========================================
# FILE: job1_aggregator.py
# ==========================================
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_timestamp, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# 1. Init Spark
spark = SparkSession.builder \
    .appName("Job1_TrafficAggregator") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Schema Input (Raw Data)
raw_schema = StructType([
    StructField("cam_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("traffic_volume", IntegerType(), True)
])

# 3. Read Raw Kafka
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-raw") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse & Clean
parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), raw_schema).alias("data")) \
    .select(
        col("data.cam_id"),
        col("data.location"),
        to_timestamp(col("data.timestamp")).alias("timestamp"),
        col("data.traffic_volume")
    )

# 5. Window Aggregation (Tính trung bình 1 phút)
# Watermark 10s để chốt sổ nhanh
agg_df = parsed_df \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("cam_id"),
        col("location")
    ) \
    .agg(avg("traffic_volume").alias("avg_traffic_volume"))

# 6. Chuẩn bị JSON Payload để gửi sang Job 2
# Job 2 cần các trường: start_time, end_time, cam_id, location, avg_traffic_volume
kafka_output_df = agg_df.select(
    # Key là cam_id để Kafka phân phối partition đều
    col("cam_id").alias("key"),
    to_json(struct(
        col("window.start").alias("start_time"),
        col("window.end").alias("end_time"),
        col("cam_id"),
        col("location"),
        col("avg_traffic_volume")
    )).alias("value")
)

# 7. Write to Kafka (Topic: traffic-aggregated)
# Mode "update" để gửi ngay khi có kết quả mới nhất (hoặc "append" nếu muốn chốt cứng)
query = kafka_output_df.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "traffic-aggregated") \
    .option("checkpointLocation", "/tmp/checkpoints/job1_agdfsd") \
    .trigger(processingTime='10 seconds') \
    .start()

print(">>> Job 1 đang chạy: Gửi dữ liệu tổng hợp vào 'traffic-aggregated'...")
query.awaitTermination()