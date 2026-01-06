"""
Kafka Video Stream Producer
Streams video frames from multiple camera sources to Kafka topics with optimized batching and compression.
Supports up to 100 concurrent streams with letterbox preprocessing.
"""

import cv2
import time
import yaml
import socket
from argparse import ArgumentParser
from confluent_kafka import Producer
from datetime import datetime, timezone


# ============================================================================
# Image Preprocessing
# ============================================================================

def letterbox(img, new_shape=(640, 640), color=(114, 114, 114)):
    """
    Resize image with aspect ratio preservation using letterbox padding.
    Implementation follows Ultralytics YOLOv5/v8 preprocessing standards.
    
    Args:
        img (np.ndarray): Input image in BGR format
        new_shape (tuple): Target dimensions (height, width)
        color (tuple): RGB padding color values
        
    Returns:
        np.ndarray: Letterboxed image with preserved aspect ratio
    """
    shape = img.shape[:2]  # current [height, width]
    
    # Calculate scaling ratio (maintain aspect ratio)
    r = min(new_shape[0] / shape[0], new_shape[1] / shape[1])
    
    # Compute new unpadded dimensions
    new_unpad = (int(round(shape[1] * r)), int(round(shape[0] * r)))
    dw, dh = (new_shape[1] - new_unpad[0]) / 2, (new_shape[0] - new_unpad[1]) / 2
    
    # Resize if dimensions changed
    if shape[::-1] != new_unpad:
        img = cv2.resize(img, new_unpad, interpolation=cv2.INTER_LINEAR)
    
    # Apply symmetric padding
    top, bottom = int(round(dh - 0.1)), int(round(dh + 0.1))
    left, right = int(round(dw - 0.1)), int(round(dw + 0.1))
    
    return cv2.copyMakeBorder(img, top, bottom, left, right, 
                              cv2.BORDER_CONSTANT, value=color)


# ============================================================================
# Kafka Utilities
# ============================================================================

def delivery_report(err, msg):
    """
    Kafka producer delivery callback for async error logging.
    
    Args:
        err: Error object if delivery failed, None otherwise
        msg: Message metadata object
    """
    if err is not None:
        print(f"[KAFKA ERROR] Message delivery failed: {err}")


def start_producer(args):
    """
    Initialize and run Kafka producer for streaming camera frames.
    Handles connection retries, frame preprocessing, and batched delivery.
    
    Args:
        args: Command-line arguments containing camera index and config path
    """
    
    # -------------------------------------------------------------------------
    # Configuration Loading
    # -------------------------------------------------------------------------
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)
    
    gen_cfg = config
    cam_cfg = config['cameras'][args.idx]
    
    # -------------------------------------------------------------------------
    # Kafka Producer Configuration (Optimized for High-Throughput)
    # -------------------------------------------------------------------------
    # Configuration tuned for 100+ concurrent streams with large payloads
    kafka_conf = {
        'bootstrap.servers': gen_cfg['KAFKA_BOOTSTRAP_SERVERS'],
        'client.id': f"{socket.gethostname()}_{cam_cfg['camera_id']}",
        
        # Payload & Compression
        'message.max.bytes': 10485760,      # Max message size: 10MB
        'compression.type': 'lz4',           # Fast compression for video frames
    }
    
    producer = Producer(kafka_conf)
    print(f"[START] Camera ID: {cam_cfg['camera_id']} | Source: {cam_cfg['source']}")
    
    # -------------------------------------------------------------------------
    # Main Streaming Loop with Auto-Reconnect
    # -------------------------------------------------------------------------
    while True:
        cap = cv2.VideoCapture(cam_cfg["source"])
        
        # Connection retry logic
        if not cap.isOpened():
            print(f"[RETRY] Failed to connect to {cam_cfg['camera_id']}. Retrying in 5s...")
            time.sleep(5)
            continue
        
        try:
            while cap.isOpened():
                start_t = time.time()
                ret, frame = cap.read()
                
                if not ret:
                    break  # Stream ended or read error
                
                # ----------------------------------------------------------
                # Frame Preprocessing
                # ----------------------------------------------------------
                # Apply letterbox to maintain aspect ratio at 640x640
                frame_fixed = letterbox(frame, new_shape=(640, 640))
                
                # Encode to JPEG with 80% quality (balance size/quality)
                _, buffer = cv2.imencode('.jpg', frame_fixed, 
                                        [int(cv2.IMWRITE_JPEG_QUALITY), 80])
                
                # ----------------------------------------------------------
                # Kafka Message Construction
                # ----------------------------------------------------------
                headers = [
                    ("cam_id", cam_cfg["camera_id"].encode('utf-8')),
                    ("location", cam_cfg["location"].encode('utf-8')),
                    ("timestamp", datetime.now(timezone.utc).isoformat().encode('utf-8'))
                ]
                
                # Send to Kafka with error handling
                try:
                    producer.produce(
                        topic=gen_cfg['TOPIC_NAME'],
                        key=cam_cfg["camera_id"].encode('utf-8'),
                        value=buffer.tobytes(),
                        headers=headers,
                        callback=delivery_report
                    )
                    producer.poll(0)  # Non-blocking poll for callbacks
                    
                except BufferError:
                    # Buffer full - block until space available
                    producer.poll(1)
                
                # ----------------------------------------------------------
                # FPS Throttling
                # ----------------------------------------------------------
                elapsed = time.time() - start_t
                time.sleep(max(0.001, (1.0 / gen_cfg['FPS_LIMIT']) - elapsed))
                
        except Exception as e:
            print(f"[ERROR] Camera {cam_cfg['camera_id']}: {e}")
            
        finally:
            cap.release()
            time.sleep(2)  # Cooldown before reconnection attempt


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    parser = ArgumentParser(description="Kafka video stream producer for multi-camera setup")
    parser.add_argument("--idx", type=int, required=True, 
                       help="Camera index in config file")
    parser.add_argument("--config", type=str, default="config/cameras.yaml",
                       help="Path to YAML configuration file")
    
    args = parser.parse_args()
    start_producer(args)