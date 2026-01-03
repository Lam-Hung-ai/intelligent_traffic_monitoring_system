import torch
from ultralytics.models import YOLO
from collections import Counter # Thêm thư viện này để đếm nhanh hơn

# Device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# Load model
model = YOLO(model='yolo11s.pt', task="detect")
model.to(device)

# Target classes
target_cls = [1, 2, 3, 5, 7]

# Predict
results = model.predict(
    source='images/gt2.jpg',
    conf=0.25,
    save=False,
    device=device
)

# Biến để lưu trữ kết quả cuối cùng
final_counts = {}

for result in results:
    detected_names = []
    
    if result.boxes is not None:
        for box in result.boxes:
            cls_id = int(box.cls[0])
            if cls_id in target_cls:
                # Lấy tên class tương ứng (vd: 'car', 'bicycle')
                class_name = model.names[cls_id]
                detected_names.append(class_name)
    
    # Sử dụng Counter để đếm số lần xuất hiện của mỗi tên lớp
    final_counts = dict(Counter(detected_names))

# In kết quả ra màn hình
print(final_counts)