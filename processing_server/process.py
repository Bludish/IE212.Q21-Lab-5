"""
processing_server / process.py
----------------------------------
Server 2: Thực thi nhận diện đối tượng (người) trong khung hình.

Luồng xử lý:
  1. Consume message JSON (chứa ảnh base64) từ Kafka topic "raw-frames".
  2. Decode ảnh, chạy mô hình YOLOv8n (Ultralytics) để phát hiện đối
     tượng, chỉ giữ lại các đối tượng thuộc lớp "person" (class 0 trong
     bộ dữ liệu COCO).
  3. Đóng gói kết quả (số lượng người + toạ độ bounding box) thành JSON
     và publish vào Kafka topic "detections" để server lưu trữ xử lý tiếp.

Đây là "worker" xử lý dữ liệu lớn theo mô hình stream processing:
đọc liên tục, xử lý độc lập theo từng message, không giữ trạng thái
giữa các frame (stateless), cho phép mở rộng (scale-out) bằng cách
chạy nhiều bản sao của service này trong cùng consumer group.
"""

import base64
import json
import logging
import os
import time

import cv2
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from ultralytics import YOLO

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [processing-server] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------- Cấu hình từ biến môi trường -------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW_FRAMES = os.getenv("KAFKA_TOPIC_RAW_FRAMES", "raw-frames")
KAFKA_TOPIC_DETECTIONS = os.getenv("KAFKA_TOPIC_DETECTIONS", "detections")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "processing-group")
MODEL_NAME = os.getenv("MODEL_NAME", "yolov8n.pt")
CONF_THRESHOLD = float(os.getenv("CONF_THRESHOLD", "0.4"))

PERSON_CLASS_ID = 0  # Trong bộ nhãn COCO, class 0 = "person"


def connect_kafka(max_retries: int = 15, delay_seconds: int = 5):
    """Kết nối tới Kafka consumer + producer, thử lại nếu broker chưa sẵn sàng."""
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_RAW_FRAMES,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_CONSUMER_GROUP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
                max_partition_fetch_bytes=5 * 1024 * 1024,
                fetch_max_bytes=5 * 1024 * 1024,
            )
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_request_size=5 * 1024 * 1024,
            )
            logger.info("Kết nối Kafka (consumer + producer) thành công.")
            return consumer, producer
        except NoBrokersAvailable:
            logger.warning(
                "Chưa kết nối được Kafka (lần %d/%d). Thử lại sau %ds...",
                attempt, max_retries, delay_seconds,
            )
            time.sleep(delay_seconds)
    raise RuntimeError("Không thể kết nối tới Kafka sau nhiều lần thử.")


def decode_base64_image(encoded_image: str) -> np.ndarray:
    """Giải mã chuỗi base64 -> ảnh OpenCV (BGR numpy array)."""
    image_bytes = base64.b64decode(encoded_image)
    np_array = np.frombuffer(image_bytes, dtype=np.uint8)
    frame = cv2.imdecode(np_array, cv2.IMREAD_COLOR)
    return frame


def detect_people(model: YOLO, frame: np.ndarray, conf_threshold: float):
    """
    Chạy YOLOv8n trên khung hình, trả về danh sách bounding box của người.

    Mỗi bounding box gồm: [x1, y1, x2, y2, confidence]
    """
    results = model.predict(frame, conf=conf_threshold, verbose=False)
    bboxes = []

    for result in results:
        for box in result.boxes:
            class_id = int(box.cls[0])
            if class_id != PERSON_CLASS_ID:
                continue
            x1, y1, x2, y2 = box.xyxy[0].tolist()
            confidence = float(box.conf[0])
            bboxes.append({
                "x1": round(x1, 2),
                "y1": round(y1, 2),
                "x2": round(x2, 2),
                "y2": round(y2, 2),
                "confidence": round(confidence, 4),
            })

    return bboxes


def main():
    logger.info("Đang tải mô hình YOLOv8n ('%s')...", MODEL_NAME)
    model = YOLO(MODEL_NAME)
    logger.info("Tải mô hình thành công.")

    consumer, producer = connect_kafka()

    logger.info("Bắt đầu lắng nghe topic '%s'...", KAFKA_TOPIC_RAW_FRAMES)
    for message in consumer:
        try:
            data = message.value
            camera_id = data["camera_id"]
            frame_id = data["frame_id"]
            timestamp = data["timestamp"]

            frame = decode_base64_image(data["image"])
            if frame is None:
                logger.warning("Không decode được ảnh, bỏ qua frame_id=%s.", frame_id)
                continue

            bboxes = detect_people(model, frame, CONF_THRESHOLD)
            person_count = len(bboxes)

            result_message = {
                "camera_id": camera_id,
                "frame_id": frame_id,
                "timestamp": timestamp,
                "width": data.get("width"),
                "height": data.get("height"),
                "person_count": person_count,
                "bboxes": bboxes,
            }

            producer.send(KAFKA_TOPIC_DETECTIONS, value=result_message)
            logger.info(
                "Đã xử lý frame_id=%s (camera=%s): phát hiện %d người.",
                frame_id, camera_id, person_count,
            )

        except Exception:
            logger.exception("Lỗi khi xử lý message, bỏ qua và tiếp tục.")


if __name__ == "__main__":
    main()
