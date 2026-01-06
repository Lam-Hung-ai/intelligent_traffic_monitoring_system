from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline

# 1. Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("TrafficPrediction_GBT") \
    .getOrCreate()

# 2. Đọc dữ liệu từ file CSV
# Giả sử file traffic_data.csv nằm cùng thư mục
df = spark.read.csv("dataset/traffic_data.csv", header=True, inferSchema=True)

# 3. Chuẩn bị Feature Vector
# Các cột đầu vào theo yêu cầu của bạn
feature_cols = ['t', 't1', 't2', 't3', 't4', 'hour_sin', 'hour_cos', 'dayofweek']
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# 4. Chia dữ liệu Train (80%) và Test (20%)
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# 5. Khởi tạo GBTRegressor
# maxIter: số lượng cây, stepSize: learning rate
gbt = GBTRegressor(featuresCol="features", labelCol="target", maxIter=100, stepSize=0.1)

# 6. Tạo Pipeline và huấn luyện
pipeline = Pipeline(stages=[assembler, gbt])
model = pipeline.fit(train_data)

# 7. Dự báo trên tập Test
predictions = model.transform(test_data)

# 8. Đánh giá mô hình
evaluator = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
r2 = RegressionEvaluator(labelCol="target", predictionCol="prediction", metricName="r2").evaluate(predictions)
with open("model_evaluation.txt", "w") as f:
    f.write(f"Root Mean Squared Error (RMSE): {rmse:.4f}\n")
    f.write(f"R-squared (R2): {r2:.4f}\n")

# 9. Xuất (Lưu) mô hình để chạy phân tán sau này
# Lưu toàn bộ Pipeline để khi load lại không cần xử lý feature thủ công
model.write().overwrite().save("traffic_gbt_model")
print("Mô hình đã được lưu tại thư mục: traffic_gbt_model")