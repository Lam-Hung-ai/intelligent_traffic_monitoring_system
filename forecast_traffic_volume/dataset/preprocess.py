import pandas as pd
import numpy as np

# 1. Đọc dữ liệu từ file .dyna
# File dyna của LibCity thường có header: dyna_id, type, time, entity_id, traffic_flow
df = pd.read_csv('PEMSD4/PEMSD4.dyna')

# Chuyển đổi cột thời gian sang định dạng datetime
df['time'] = pd.to_datetime(df['time'])

# 2. Tạo các cột đặc trưng thời gian (Time Features)
def encode_time(df):
    # Lấy giờ dưới dạng số thực (ví dụ 14:30 -> 14.5)
    hour_float = df['time'].dt.hour + df['time'].dt.minute / 60.0
    
    # Tính Sin/Cos cho khung giờ (chu kỳ 24h)
    df['hour_sin'] = np.sin(2 * np.pi * hour_float / 24)
    df['hour_cos'] = np.cos(2 * np.pi * hour_float / 24)
    
    # Ngày trong tuần (0: Thứ 2, 6: Chủ nhật)
    df['dayofweek'] = df['time'].dt.dayofweek
    return df

df = encode_time(df)

# 3. Tạo các cột Lag (t, t-1, t-2, t-3, t-4) và Target (t+1)
# Chúng ta cần thực hiện việc này theo từng cảm biến (entity_id) để tránh nhầm lẫn dữ liệu giữa các trạm
processed_data = []

sensors = df['entity_id'].unique()

for sensor in sensors:
    sensor_df = df[df['entity_id'] == sensor].sort_values('time').copy()
    
    # Giả sử cột lưu lượng tên là 'traffic_flow' (kiểm tra lại tên cột trong file của bạn)
    # t4 là xa nhất, t là hiện tại, target là 5 phút tới
    sensor_df['t4'] = sensor_df['traffic_flow'].shift(4)
    sensor_df['t3'] = sensor_df['traffic_flow'].shift(3)
    sensor_df['t2'] = sensor_df['traffic_flow'].shift(2)
    sensor_df['t1'] = sensor_df['traffic_flow'].shift(1)
    sensor_df['t']  = sensor_df['traffic_flow']
    sensor_df['target'] = sensor_df['traffic_flow'].shift(-1)
    
    # Xóa các dòng có giá trị NaN do quá trình shift tạo ra
    processed_data.append(sensor_df.dropna())

# Gộp lại thành một DataFrame lớn
final_df = pd.concat(processed_data)

# 4. Lọc dữ liệu theo điều kiện lưu lượng < 150
# Ưu tiên lấy các dòng có lưu lượng mục tiêu < 150
filtered_df = final_df[final_df['target'] < 150]

# Nếu sau khi lọc < 150 mà không đủ 2000 dòng, ta sẽ lấy thêm từ phần còn lại
if len(filtered_df) < 2000:
    extra_needed = 2000 - len(filtered_df)
    others = final_df[final_df['target'] >= 150]
    # Lấy ngẫu nhiên phần còn lại
    sampled_others = others.sample(n=min(extra_needed, len(others)), random_event=42)
    result_df = pd.concat([filtered_df, sampled_others])
else:
    # Nếu nhiều hơn 2000 dòng, lấy ngẫu nhiên 2000 dòng từ tập đã lọc
    result_df = filtered_df.sample(n=2000, random_state=42)

# 5. Chọn lọc các cột cuối cùng và lưu kết quả
columns_to_keep = ['t', 't1', 't2', 't3', 't4', 'hour_sin', 'hour_cos', 'dayofweek', 'target']
result_df = result_df[columns_to_keep]

# Lưu ra file CSV để bạn train model
result_df.to_csv('traffic_data.csv', index=False)

print("Đã tạo xong dữ liệu! Xem file: traffic_data.csv")
print(result_df.head())