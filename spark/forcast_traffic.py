import math
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
from pyspark.ml import PipelineModel

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("Job2_TrafficPredictor_CorrectTime") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Load Model
model_path = "forecast_traffic_volume/traffic_gbt_model"
try:
    traffic_model = PipelineModel.load(model_path)
    print(">>> Load Model thành công!")
except Exception as e:
    import sys; sys.exit(f"!!! Lỗi load model: {e}")

# 3. Schema Input
input_schema = StructType([
    StructField("start_time", TimestampType(), True),
    StructField("end_time", TimestampType(), True),
    StructField("cam_id", StringType(), True),
    StructField("location", StringType(), True),
    StructField("avg_traffic_volume", DoubleType(), True)
])

# 4. Read Kafka
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic-aggregated") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = kafka_df.selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), input_schema).alias("data")) \
    .select("data.*")

# 5. Sliding Window Logic
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

# 6. Feature Engineering & Padding
@F.udf(returnType=ArrayType(DoubleType()))
def pad_and_extract_volume(history):
    # Sắp xếp tăng dần theo thời gian (t-4 -> t)
    sorted_data = sorted(history, key=lambda x: x['end_time'])
    vols = [x['avg_traffic_volume'] for x in sorted_data]
    
    # Padding nếu thiếu dữ liệu (điền 20 vào quá khứ)
    count = len(vols)
    if count < 5:
        missing = 5 - count
        vols = [20.0] * missing + vols
    
    return vols[-5:]

# Tách features
features_df = windowed_df \
    .withColumn("padded_vols", pad_and_extract_volume(F.col("history"))) \
    .select(
        # ĐÂY LÀ THỜI ĐIỂM T (Hiện tại)
        F.col("window.end").alias("current_timestamp_t"), 
        F.col("cam_id"),
        F.col("padded_vols")[4].alias("t"),  # Volume tại t
        F.col("padded_vols")[3].alias("t1"), # Volume tại t-1
        F.col("padded_vols")[2].alias("t2"),
        F.col("padded_vols")[1].alias("t3"),
        F.col("padded_vols")[0].alias("t4")
    )

# Feature Engineering: Tính Hour, DayOfWeek TỪ THỜI ĐIỂM T
# (Vì Model dùng ngữ cảnh hiện tại để đoán tương lai)
final_input_df = features_df \
    .withColumn("local_time_t", F.from_utc_timestamp(F.col("current_timestamp_t"), "Asia/Ho_Chi_Minh")) \
    .withColumn("hour", F.hour("local_time_t")) \
    .withColumn("dayofweek", F.dayofweek("local_time_t")) \
    .withColumn("hour_sin", F.sin(F.col("hour") * (2 * math.pi / 24))) \
    .withColumn("hour_cos", F.cos(F.col("hour") * (2 * math.pi / 24))) \
    .drop("local_time_t")

# 7. Predict
predictions = traffic_model.transform(final_input_df)

# 8. Output Kafka
# LOGIC MỚI: prediction_timestamp = t + 5 phút
kafka_output_df = predictions.select(
    F.col("cam_id").alias("key"),
    F.to_json(F.struct(
        # Timestamp hiển thị trong kết quả là (t + 5 phút)
        (F.col("current_timestamp_t") + F.expr("INTERVAL 5 MINUTES")).alias("prediction_timestamp"),
        
        # Thời điểm thực tế dùng để tính toán (Optional - để debug)
        F.col("current_timestamp_t").alias("base_time_t"),
        
        F.col("cam_id"),
        F.col("t").alias("current_volume"),
        F.col("prediction").alias("predicted_volume"),
        F.when(F.col("t4") == 20.0, True).otherwise(False).alias("is_padded")
    )).alias("value")
)

# Trigger 0s để chạy ngay lập tức khi có dữ liệu
query = kafka_output_df.writeStream \
    .format("kafka") \
    .outputMode("append") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "traffic-forecast") \
    .option("checkpointLocation", "/tmp/checkpoints/job2_correct_time_v2") \
    .trigger(processingTime='0 seconds') \
    .start()

print(">>> Job 2 (Correct Time Logic) đang chạy...")
query.awaitTermination()