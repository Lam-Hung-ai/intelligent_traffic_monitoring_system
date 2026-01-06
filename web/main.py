import asyncio
import json
import yaml
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from aiokafka import AIOKafkaConsumer
from redis import asyncio as aioredis
from redis.asyncio.lock import Lock # Import Lock
from pydantic import BaseModel

# --- LOAD CONFIG ---
def load_config(path: str = "config/cameras.yaml") -> dict:
    with open(path, 'r') as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(f"Error loading config: {exc}")
            raise exc

# --- MODELS ---
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

# --- LOGIC QUẢN LÝ LIST ---

def manage_history_list(current_list: list, new_record: dict) -> list:
    """Giữ history luôn sorted và tối đa 30 bản ghi"""
    # Xóa trùng lặp nếu có (dựa vào start_time)
    current_list = [x for x in current_list if x['start_time'] != new_record['start_time']]
    current_list.append(new_record)
    current_list.sort(key=lambda x: x["start_time"])
    # Cắt lấy 30 cái mới nhất
    if len(current_list) > 30:
        current_list = current_list[-30:]
    return current_list

def manage_forecast_list(current_list: list, new_record: dict) -> list:
    """
    Quản lý forecast: Update nếu trùng time, thêm mới, sort, và giữ lại nhiều nhất có thể.
    """
    # 1. Update or Append
    # Lọc bỏ bản ghi cũ có cùng prediction_timestamp (để update giá trị mới nhất)
    filtered_list = [x for x in current_list if x['prediction_timestamp'] != new_record['prediction_timestamp']]
    filtered_list.append(new_record)
    
    # 2. Sort tăng dần theo thời gian dự đoán
    filtered_list.sort(key=lambda x: x["prediction_timestamp"])
    
    # 3. Buffer Size: Tăng lên 288 (tương đương 24h nếu 5 phút/lần) để đảm bảo không bị thiếu
    if len(filtered_list) > 288: 
        # Cắt bớt những dự đoán quá khứ xa xưa nếu list quá dài, giữ lại tương lai
        filtered_list = filtered_list[-288:]
        
    return filtered_list

# --- WORKER XỬ LÝ KAFKA ---

async def process_message(redis_client: aioredis.Redis, msg, config: dict):
    try:
        value_str = msg.value.decode("utf-8")
        data = json.loads(value_str)
        topic = msg.topic
        cam_id = data.get("cam_id")

        if not cam_id:
            return

        redis_key = f"cam_data:{cam_id}"
        lock_key = f"lock:cam:{cam_id}"

        # --- QUAN TRỌNG: SỬ DỤNG REDIS LOCK ---
        # Đảm bảo chỉ 1 worker được quyền sửa data của cam_id này tại 1 thời điểm
        # timeout=5s: Nếu lock lỗi, tự động nhả sau 5s để không treo hệ thống
        async with redis_client.lock(lock_key, timeout=5):
            
            # 1. Đọc dữ liệu mới nhất trong Redis (bên trong Lock)
            current_data_json = await redis_client.get(redis_key)
            
            if current_data_json:
                cam_data = json.loads(current_data_json)
            else:
                # Init data
                cam_config = next((c for c in config['cameras'] if c["camera_id"] == cam_id), None)
                loc = cam_config['location'] if cam_config else data.get("location", "Unknown")
                cam_data = {
                    "cam_id": cam_id,
                    "location": loc,
                    "history": [],
                    "forecast": [],
                    "last_updated": ""
                }

            # Đảm bảo type safe
            if not isinstance(cam_data.get("history"), list): cam_data["history"] = []
            if not isinstance(cam_data.get("forecast"), list): cam_data["forecast"] = []

            # 2. Xử lý Logic Update
            if topic == config['TOPIC_AGGREGATED']:
                if "location" in data: cam_data["location"] = data["location"]
                cam_data["history"] = manage_history_list(cam_data["history"], data)

            elif topic == config['TOPIC_FORECAST']:
                cam_data["forecast"] = manage_forecast_list(cam_data["forecast"], data)

            # Update timestamp hệ thống
            from datetime import datetime
            cam_data["last_updated"] = datetime.now().isoformat()

            # 3. Ghi ngược lại Redis (vẫn nằm trong Lock)
            await redis_client.set(redis_key, json.dumps(cam_data))
            # print(f"Updated {cam_id} | History: {len(cam_data['history'])} | Forecast: {len(cam_data['forecast'])}")

    except Exception as e:
        print(f"Error processing message: {e}")

async def consume_kafka(redis_client: aioredis.Redis, config: dict):
    topics = [config['TOPIC_AGGREGATED'], config['TOPIC_FORECAST']]
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=config['KAFKA_BOOTSTRAP_SERVERS'],
        group_id="traffic_backend_v4_locked", # Đổi group ID để re-read nếu cần
        auto_offset_reset="latest"
    )
    await consumer.start()
    print(f"Started Kafka Consumer for topics: {topics}")
    try:
        async for msg in consumer:
            await process_message(redis_client, msg, config)
    except Exception as e:
        print(f"Consumer Error: {e}")
    finally:
        await consumer.stop()

# --- LIFECYCLE ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    config = load_config()
    app.state.config = config
    
    # Kết nối Redis
    redis = aioredis.from_url(f"redis://{config['REDIS_HOST']}:{config['REDIS_PORT']}", decode_responses=True)
    app.state.redis = redis
    
    # Pre-load metadata (để tránh 404 khi mới chạy)
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

    # Chạy Consumer
    task = asyncio.create_task(consume_kafka(redis, config))
    
    yield
    
    task.cancel()
    await redis.close()

app = FastAPI(title="Traffic Monitor", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Cho phép mọi nguồn truy cập (để test cho dễ)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- API ENDPOINTS ---

@app.get("/traffic/{cam_id}", response_model=CameraDataResponse)
async def get_traffic_data(cam_id: str):
    redis = app.state.redis
    data_json = await redis.get(f"cam_data:{cam_id}")
    
    if not data_json:
        raise HTTPException(status_code=404, detail="Camera not found")
    
    data = json.loads(data_json)
    history = data.get("history", [])
    forecasts = data.get("forecast", [])
    
    # --- LOGIC LỌC DỮ LIỆU ---
    # Chỉ trả về forecast có thời gian > end_time của history mới nhất
    valid_forecasts = []
    
    if history:
        # Lấy phần tử cuối cùng (mới nhất)
        last_history_end = history[-1].get("end_time")
        
        if last_history_end:
            # Lọc: Chỉ lấy dự đoán trong tương lai so với dữ liệu thực
            valid_forecasts = [
                f for f in forecasts 
                if f.get("prediction_timestamp") > last_history_end
            ]
        else:
            # Nếu bản ghi history lỗi không có end_time, lấy hết forecast
            valid_forecasts = forecasts
    else:
        # Nếu chưa có history nào, trả về toàn bộ forecast đang có
        valid_forecasts = forecasts

    data["forecast"] = valid_forecasts
    return data
@app.get("/", response_class=HTMLResponse)
async def serve_frontend():
    """
    Đọc file index.html và trả về trình duyệt
    khi người dùng truy cập http://localhost:8000/
    """
    try:
        # Đảm bảo file index.html nằm cùng thư mục với main.py
        with open("web/index.html", "r", encoding="utf-8") as f:
            return f.read()
    except FileNotFoundError:
        return "<h1>Lỗi: Không tìm thấy file index.html</h1>"

@app.get("/cameras")
async def get_cameras():
    """
    Trả về danh sách camera dựa trên file cấu hình (cameras.yaml).
    Điều này đảm bảo Location luôn hiển thị đúng, không bị 'Unknown'.
    """
    config = app.state.config
    details = []
    
    # Duyệt qua từng camera trong file config
    if 'cameras' in config:
        for cam in config['cameras']:
            details.append({
                "id": cam['camera_id'],
                "location": cam['location']
            })
    
    # Trả về cấu trúc mà Frontend mong đợi
    return {
        "cameras": [d['id'] for d in details], # List các ID
        "details": details                     # List chi tiết {id, location}
    }