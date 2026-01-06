import asyncio
import json
import yaml
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaConsumer
from redis import asyncio as aioredis
from pydantic import BaseModel

# --- TẢI CẤU HÌNH ---
def load_config(path: str = "config/cameras.yaml") -> dict:
    """Đọc và chuyển đổi tệp cấu hình YAML sang dictionary Python."""
    with open(path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(f"Error loading config: {exc}")
            raise exc

# --- MÔ HÌNH DỮ LIỆU (SCHEMA) ---
class TrafficAggregated(BaseModel):
    start_time: str 
    end_time: str
    cam_id: str
    location: str
    avg_traffic_volume: float

class TrafficForecast(BaseModel):
    prediction_timestamp: str
    base_time_t: str
    cam_id: str
    current_volume: float
    predicted_volume: float
    is_padded: bool

class CameraDataResponse(BaseModel):
    cam_id: str
    location: str
    last_updated: str
    history: List[TrafficAggregated]
    forecast: List[TrafficForecast] = []

# --- LOGIC QUẢN LÝ DANH SÁCH DỮ LIỆU ---

def manage_history_list(current_list: list, new_record: dict) -> list:
    """
    Cập nhật danh sách lịch sử: Loại bỏ bản ghi trùng thời gian, 
    sắp xếp tăng dần và duy trì kích thước tối đa 30 phần tử.
    """
    current_list = [x for x in current_list if x['start_time'] != new_record['start_time']]
    current_list.append(new_record)
    current_list.sort(key=lambda x: x["start_time"])
    if len(current_list) > 30:
        current_list = current_list[-30:]
    return current_list

def manage_forecast_list(current_list: list, new_record: dict) -> list:
    """
    Cập nhật danh sách dự báo: Ghi đè nếu trùng mốc thời gian dự báo,
    sắp xếp theo trình tự thời gian và duy trì bộ đệm tối đa 288 bản ghi (tương đương 24h).
    """
    filtered_list = [x for x in current_list if x['prediction_timestamp'] != new_record['prediction_timestamp']]
    filtered_list.append(new_record)
    filtered_list.sort(key=lambda x: x["prediction_timestamp"])
    
    if len(filtered_list) > 288: 
        filtered_list = filtered_list[-288:]
        
    return filtered_list

# --- TIẾN TRÌNH XỬ LÝ DỮ LIỆU KAFKA ---

async def process_message(redis_client: aioredis.Redis, msg, config: dict):
    """
    Giải mã thông điệp từ Kafka và cập nhật vào kho lưu trữ Redis.
    Sử dụng Distributed Lock để đảm bảo tính toàn vẹn dữ liệu khi có nhiều worker xử lý cùng một Camera ID.
    """
    try:
        value_str = msg.value.decode("utf-8")
        data = json.loads(value_str)
        topic = msg.topic
        cam_id = data.get("cam_id")

        if not cam_id:
            return

        redis_key = f"cam_data:{cam_id}"
        lock_key = f"lock:cam:{cam_id}"

        # Thiết lập cơ chế khóa độc quyền trong 5 giây để tránh xung đột ghi (Race Condition)
        async with redis_client.lock(lock_key, timeout=5):
            
            # Truy xuất trạng thái hiện tại từ Redis hoặc khởi tạo mới nếu chưa tồn tại
            current_data_json = await redis_client.get(redis_key)
            
            if current_data_json:
                cam_data = json.loads(current_data_json)
            else:
                cam_config = next((c for c in config['cameras'] if c["camera_id"] == cam_id), None)
                loc = cam_config['location'] if cam_config else data.get("location", "Unknown")
                cam_data = {
                    "cam_id": cam_id,
                    "location": loc,
                    "history": [],
                    "forecast": [],
                    "last_updated": ""
                }

            if not isinstance(cam_data.get("history"), list): cam_data["history"] = []
            if not isinstance(cam_data.get("forecast"), list): cam_data["forecast"] = []

            # Phân loại logic xử lý dựa trên Topic nguồn (Dữ liệu thực tế vs Dữ liệu dự báo)
            if topic == config['TOPIC_AGGREGATED']:
                if "location" in data: cam_data["location"] = data["location"]
                cam_data["history"] = manage_history_list(cam_data["history"], data)

            elif topic == config['TOPIC_FORECAST']:
                cam_data["forecast"] = manage_forecast_list(cam_data["forecast"], data)

            from datetime import datetime
            cam_data["last_updated"] = datetime.now().isoformat()

            # Đồng bộ hóa dữ liệu đã xử lý ngược lại bộ nhớ đệm Redis
            await redis_client.set(redis_key, json.dumps(cam_data))

    except Exception as e:
        print(f"Error processing message: {e}")

async def consume_kafka(redis_client: aioredis.Redis, config: dict):
    """Khởi tạo Consumer lắng nghe đồng thời nhiều Topic và duy trì luồng đọc dữ liệu liên tục."""
    topics = [config['TOPIC_AGGREGATED'], config['TOPIC_FORECAST']]
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=config['KAFKA_BOOTSTRAP_SERVERS'],
        group_id="traffic_backend_v4_locked",
        auto_offset_reset="latest"
    )
    await consumer.start()
    try:
        async for msg in consumer:
            await process_message(redis_client, msg, config)
    except Exception as e:
        print(f"Consumer Error: {e}")
    finally:
        await consumer.stop()

# --- QUẢN LÝ VÒNG ĐỜI ỨNG DỤNG ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Định nghĩa các tác vụ khởi tạo và dọn dẹp hệ thống: 
    Tải cấu hình, kết nối Redis, khởi tạo dữ liệu mẫu và kích hoạt worker chạy ngầm.
    """
    config = load_config()
    app.state.config = config
    
    redis = aioredis.from_url(f"redis://{config['REDIS_HOST']}:{config['REDIS_PORT']}", decode_responses=True)
    app.state.redis = redis
    
    # Đăng ký danh sách camera hoạt động và tạo cấu trúc dữ liệu mặc định để tránh lỗi truy xuất 404
    for cam in config['cameras']:
        cid = cam['camera_id']
        await redis.sadd("active_cameras", cid)
        if not await redis.exists(f"cam_data:{cid}"):
            init = {
                "cam_id": cid, 
                "location": cam['location'], 
                "history": [], 
                "forecast": [], 
                "last_updated": "Waiting for data..."
            }
            await redis.set(f"cam_data:{cid}", json.dumps(init))

    # Kích hoạt tiến trình đọc Kafka chạy song song với API server
    task = asyncio.create_task(consume_kafka(redis, config))
    
    yield
    
    task.cancel()
    await redis.close()

app = FastAPI(title="Traffic Monitor", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- CÁC ĐIỂM CUỐI API (ENDPOINTS) ---

@app.get("/traffic/{cam_id}", response_model=CameraDataResponse)
async def get_traffic_data(cam_id: str):
    """
    Truy xuất dữ liệu giao thông của một camera. 
    Logic quan trọng: Chỉ trả về các bản ghi dự báo có thời gian xảy ra sau bản ghi lịch sử cuối cùng (dữ liệu tương lai).
    """
    redis = app.state.redis
    data_json = await redis.get(f"cam_data:{cam_id}")
    
    if not data_json:
        raise HTTPException(status_code=404, detail="Camera not found")
    
    data = json.loads(data_json)
    history = data.get("history", [])
    forecasts = data.get("forecast", [])
    
    valid_forecasts = []
    
    # Lọc bỏ các dự đoán đã trở thành quá khứ dựa trên mốc thời gian thực tế mới nhất
    if history:
        last_history_end = history[-1].get("end_time")
        if last_history_end:
            valid_forecasts = [
                f for f in forecasts 
                if f.get("prediction_timestamp") > last_history_end
            ]
        else:
            valid_forecasts = forecasts
    else:
        valid_forecasts = forecasts

    data["forecast"] = valid_forecasts
    return data

@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """Phục vụ tệp giao diện người dùng (HTML) tại trang chủ."""
    try:
        with open("web/index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Lỗi: Không tìm thấy file index.html</h1>"

@app.get("/cameras")
async def get_cameras():
    """Trả về danh sách định danh và vị trí vật lý của tất cả camera từ tệp cấu hình hệ thống."""
    config = app.state.config
    details = []
    
    if 'cameras' in config:
        for cam in config['cameras']:
            details.append({
                "id": cam['camera_id'],
                "location": cam['location']
            })
    
    return {
        "cameras": [d['id'] for d in details],
        "details": details
    }