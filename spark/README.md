```bash
spark-submit \
    # Chạy Spark trên YARN (resource manager của Hadoop)
    --master yarn \
    
    # Driver chạy trong cluster (phù hợp production, job dài, streaming)
    --deploy-mode cluster \
    
    # Số executor (thường tương ứng với số node nếu đủ tài nguyên)
    # Ở đây: 4 executor → phù hợp với cluster 4 máy
    --num-executors 1 \
    
    # Số CPU core cho mỗi executor
    # Mỗi executor chạy tối đa 4 task song song
    --executor-cores 4 \
    
    # Bộ nhớ heap JVM cho mỗi executor
    # Chưa bao gồm memory overhead của YARN
    --executor-memory 4G \
    
    # Cấp phát GPU cho mỗi executor
    # Mỗi executor được độc quyền 1 GPU
    --conf spark.executor.resource.gpu.amount=1 \
    
    # Lượng GPU mà mỗi task sử dụng
    # 0.25 GPU/task → 1 GPU chạy được 4 task song song
    # Cân bằng tốt với executor-cores=4
    --conf spark.task.resource.gpu.amount=0.25 \
    
    # Cấu hình Apache Arrow cho PySpark ↔ Pandas
    # Giới hạn số record mỗi batch để tránh spike bộ nhớ
    # Phù hợp với dữ liệu nặng (image, tensor, NLP, CV)
    --conf spark.sql.execution.arrow.maxRecordsPerBatch=20 \
    
    # File Python chính của Spark application (driver code)
    traffic_app.py

```