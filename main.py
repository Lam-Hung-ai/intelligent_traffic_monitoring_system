from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Khởi tạo SparkSession
# 'local[*]' nghĩa là sử dụng tất cả các nhân CPU có sẵn trên máy bạn để chạy song song
spark = SparkSession.builder \
    .appName("BaiTapPhanTanDonGian") \
    .master("local[*]") \
    .getOrCreate()

# 2. Tạo tập dữ liệu mẫu (Giả lập dữ liệu lớn)
data = [
    ("Hoang", "IT", 8500),
    ("An", "IT", 9200),
    ("Binh", "HR", 5000),
    ("Chi", "Marketing", 7000),
    ("Dung", "IT", 8800),
    ("En", "HR", 5500),
    ("Giang", "Marketing", 7200)
]
columns = ["Ten", "PhongBan", "Luong"]

# 3. Chuyển dữ liệu vào DataFrame (Dữ liệu lúc này đã được phân tán)
df = spark.createDataFrame(data, schema=columns)

print("Dữ liệu ban đầu:")
df.show()

# 4. Xử lý phân tán: Tính lương trung bình và tổng lương mỗi phòng ban
# Spark sẽ tự động chia các phòng ban cho các nhân CPU khác nhau xử lý cùng lúc
ket_qua = df.groupBy("PhongBan").agg(
    F.avg("Luong").alias("Luong_Trung_Binh"),
    F.count("Ten").alias("So_Nhan_Vien")
)

# 5. Hiển thị kết quả cuối cùng
print("Kết quả xử lý phân tán:")
ket_qua.show()

# 6. Đóng session
spark.stop()