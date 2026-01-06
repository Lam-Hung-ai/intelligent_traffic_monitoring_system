# Hướng dẫn Thiết lập Dữ liệu Dự báo Giao thông PEMSD4

Tài liệu này hướng dẫn cách tải, giải nén và tiền xử lý bộ dữ liệu PEMSD4 về định dạng CSV để huấn luyện model.

## 1. Yêu cầu hệ thống

* Python 3.x
* Thư viện cần thiết:
```bash
pip install pandas numpy
```



## 2. Tải và Giải nén Dữ liệu

1. **Tải xuống:** Truy cập [LibCity Google Drive](https://drive.google.com/drive/folders/1g5v2Gq1tkOq8XO0HDCZ9nOTtRpB6-gPe?usp=sharing) và tải file `PEMSD4.zip`.
2. **Giải nén:**
```bash
unzip PEMSD4.zip -d ./PEMSD4
cd PEMSD4
```


*Đảm bảo trong thư mục có các file: `PEMSD4.dyna`, `PEMSD4.geo`, `PEMSD4.rel`, `config.json`.*

## 3. Tiền xử lý dữ liệu

Tạo file `preprocess.py` trong thư mục `PEMSD4` với nội dung code đã cung cấp. File này sẽ thực hiện:

* Trích xuất đặc trưng thời gian (Sin/Cos cho khung giờ).
* Tạo cửa sổ trượt: **t, t-1, t-2, t-3, t-4** (dữ liệu 25 phút trước).
* Gắn nhãn **target** (lưu lượng 5 phút tiếp theo).
* Lọc ngẫu nhiên 2000 dòng (ưu tiên lưu lượng < 150).

**Chạy lệnh sau:**

```bash
python3 preprocess.py
```

## 4. Kết quả tiền xử lý

Sau khi chạy xong, file dữ liệu dùng để train sẽ xuất hiện tại:

* **Đường dẫn:** `traffic_train_data.csv`
* **Định dạng:** 9 cột (`t`, `t1`, `t2`, `t3`, `t4`, `hour_sin`, `hour_cos`, `dayofweek`, `target`).

---

**Lưu ý cho đồng đội:** * Nếu file `.dyna` có tên cột khác `traffic_flow`, hãy cập nhật dòng số 29 trong file `preprocess.py`.

* Cột `dayofweek` nhận giá trị từ 0 (Thứ 2) đến 6 (Chủ nhật).

---

## 5. Train model

```
cd forecast_traffic_volumn
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.0 train.py
```