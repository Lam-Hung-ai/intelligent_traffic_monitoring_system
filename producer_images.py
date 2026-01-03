import os
from kafka import KafkaProducer

# 1. Cấu hình các thông số tối ưu cho việc gửi ảnh lớn
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    # Cho phép gửi gói tin lên tới 10MB (phải khớp với config trên Broker)
    max_request_size=10485760, 
    # Nén dữ liệu để truyền tải nhanh hơn qua mạng
    compression_type='lz4',
    # Đảm bảo tin nhắn được gửi đi theo đúng thứ tự
    retries=2
)

topic_name = 'yolo-data'

# 2. Danh sách 5 đường dẫn ảnh của bạn (Hãy thay đổi đường dẫn thực tế trên máy bạn)
image_paths = [
    "/home/lamhung/code/intelligent_traffic_monitoring_system/images/giao-thong-ha-noi-autonews.jpg",
    # "/home/lamhung/code/intelligent_traffic_monitoring_system/images/gt2.jpg",
    # "/home/lamhung/images/photo2.jpg",
    # "/home/lamhung/images/photo3.jpg",
    # "/home/lamhung/images/photo4.jpg",
    # "/home/lamhung/images/photo5.jpg"
]

def send_images_to_kafka(paths):
    count = 0
    for path in paths:
        # Kiểm tra xem file có tồn tại không trước khi đọc
        if os.path.exists(path):
            try:
                with open(path, "rb") as image_file:
                    # Đọc toàn bộ nội dung ảnh dưới dạng Byte
                    image_bytes = image_file.read()

                    print("Dung lượng ảnh (bytes):", os.path.getsize(path)/1024/1024, "MB")
                    # Gửi vào Kafka (Value là mảng Byte)
                    # Bạn có thể dùng Key để định danh đây là ảnh từ máy nào
                    future = producer.send(topic_name, value=image_bytes, key=bytes(path, 'utf-8'))
                    
                    # Đợi xác nhận gửi thành công
                    record_metadata = future.get(timeout=10)
                    
                    count += 1
                    print(f"[{count}] Đã gửi thành công: {path}")
                    print(f"    - Partition: {record_metadata.partition}")
                    print(f"    - Offset: {record_metadata.offset}")
                    
            except Exception as e:
                print(f"Lỗi khi gửi ảnh {path}: {e}")
        else:
            print(f"Cảnh báo: Không tìm thấy file tại {path}")

    # Đẩy toàn bộ dữ liệu còn lại trong bộ đệm đi
    producer.flush()
    print(f"\nHoàn thành! Đã gửi tổng cộng {count} ảnh.")

if __name__ == "__main__":
    send_images_to_kafka(image_paths)
    # Đóng kết nối
    producer.close()