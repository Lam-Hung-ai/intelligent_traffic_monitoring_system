import cv2
import yaml
import socket
import os
from datetime import datetime, timezone
from confluent_kafka import Producer

# ============================================================================
# Image Preprocessing (Giữ nguyên từ bản gốc)
# ============================================================================
def letterbox(img, new_shape=(640, 640), color=(114, 114, 114)):
    shape = img.shape[:2]
    r = min(new_shape[0] / shape[0], new_shape[1] / shape[1])
    new_unpad = (int(round(shape[1] * r)), int(round(shape[0] * r)))
    dw, dh = (new_shape[1] - new_unpad[0]) / 2, (new_shape[0] - new_unpad[1]) / 2
    
    if shape[::-1] != new_unpad:
        img = cv2.resize(img, new_unpad, interpolation=cv2.INTER_LINEAR)
    
    top, bottom = int(round(dh - 0.1)), int(round(dh + 0.1))
    left, right = int(round(dw - 0.1)), int(round(dw + 0.1))
    return cv2.copyMakeBorder(img, top, bottom, left, right, 
                              cv2.BORDER_CONSTANT, value=color)

def delivery_report(err, msg):
    if err is not None:
        print(f"[KAFKA ERROR] Delivery failed: {err}")

# ============================================================================
# Main Processing Function
# ============================================================================
def send_image_list_to_kafka(image_paths, config_path):
    # 1. Load Configuration
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Lấy thông tin camera mặc định hoặc đầu tiên để làm metadata
    cam_cfg = config['cameras'][0] 
    
    # 2. Kafka Producer Config
    kafka_conf = {
        'bootstrap.servers': config['KAFKA_BOOTSTRAP_SERVERS'],
        'client.id': f"{socket.gethostname()}_static_images",
        'message.max.bytes': 10485760,
        'compression.type': 'lz4',
    }
    producer = Producer(kafka_conf)

    print(f"[START] Sending {len(image_paths)} images to topic: {config['TOPIC_NAME']}")

    # 3. Duyệt qua danh sách ảnh
    for img_path in image_paths:
        if not os.path.exists(img_path):
            print(f"[SKIP] File not found: {img_path}")
            continue

        # Đọc ảnh
        frame = cv2.imread(img_path)
        if frame is None:
            print(f"[ERROR] Could not decode image: {img_path}")
            continue

        # --- TIỀN XỬ LÝ (Giữ đúng chuẩn của code mẫu) ---
        frame_fixed = letterbox(frame, new_shape=(640, 640))
        _, buffer = cv2.imencode('.jpg', frame_fixed, [int(cv2.IMWRITE_JPEG_QUALITY), 80])
        
        # --- KHỞI TẠO HEADERS (Giống hệt code mẫu) ---
        headers = [
            ("cam_id", cam_cfg["camera_id"].encode('utf-8')),
            ("location", cam_cfg["location"].encode('utf-8')),
            ("timestamp", datetime.now(timezone.utc).isoformat().encode('utf-8'))
        ]

        # --- GỬI LÊN KAFKA ---
        try:
            producer.produce(
                topic=config['TOPIC_NAME'],
                key=cam_cfg["camera_id"].encode('utf-8'),
                value=buffer.tobytes(),
                headers=headers,
                callback=delivery_report
            )
            producer.poll(0) # Phục vụ callback
            print(f"[SENT] {img_path}")
            
        except BufferError:
            producer.poll(1)
            # Thử gửi lại nếu buffer đầy
            producer.produce(topic=config['TOPIC_NAME'], value=buffer.tobytes(), headers=headers)

    # Đợi tất cả tin nhắn được gửi đi trước khi đóng chương trình
    print("[FLUSHING] Waiting for all messages to be delivered...")
    producer.flush()
    print("[DONE] Process finished.")

# ============================================================================
# Ví dụ sử dụng
# ============================================================================
if __name__ == "__main__":
    # Danh sách các file ảnh của bạn
    my_images = [
        # "images/giao-thong-ha-noi-autonews.jpg",
        "images/gt2.jpg",
    ]
    
    # Đường dẫn file config của bạn
    CONFIG_FILE = "config/cameras.yaml"
    
    send_image_list_to_kafka(my_images, CONFIG_FILE)