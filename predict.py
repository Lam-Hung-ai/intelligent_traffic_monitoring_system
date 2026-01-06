import torch
import cv2
from ultralytics.models import YOLO

# Device
device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')

# Load model
model = YOLO(model='yolo11s.pt', task="detect", verbose=True)
model.to(device)

# Target classes
target_cls = [1, 2, 3, 5, 7]

# Color map (BGR)
CLASS_COLORS = {
    1: (255, 0, 0),    # bicycle
    2: (0, 255, 0),    # car
    3: (0, 0, 255),    # motorcycle
    5: (255, 255, 0),  # bus
    7: (255, 0, 255),  # truck
}

# Predict
results = model.predict(
    source='images/gt2.jpg',
    conf=0.25,
    save=False,
    save_txt=False,
    device=device
)


for result in results:
    img = result.orig_img.copy()
    if result.boxes is None or len(result.boxes) == 0:
        print("No target objects detected.")
        continue
    for box in result.boxes:
        cls_id = int(box.cls[0])
        if cls_id not in target_cls:
            continue

        conf = float(box.conf[0])
        class_name = model.names[cls_id]
        x1, y1, x2, y2 = box.xyxy[0].cpu().numpy().astype(int)
        color = CLASS_COLORS.get(cls_id, (0, 255, 0))

        # ---- vẽ box ----
        cv2.rectangle(
            img,
            (x1, y1),
            (x2, y2),
            color,
            thickness=2,
            lineType=cv2.LINE_AA
        )

        # ---- vẽ label ----
        label = f"{class_name} {conf:.2f}"

        cv2.putText(
            img,
            label,
            (x1, max(12, y1 - 4)),
            cv2.FONT_HERSHEY_SIMPLEX,
            fontScale=0.45,
            color=color,
            thickness=2,
            lineType=cv2.LINE_AA
        )

    cv2.imshow("Detection (thin & small)", img)
    cv2.waitKey(0)
    cv2.destroyAllWindows()
