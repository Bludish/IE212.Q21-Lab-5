"""
storage_server / storage.py
----------------------------------
Server 3: Lưu trữ kết quả nhận diện.

Luồng xử lý:
  1. Consume message JSON (person_count + bboxes) từ Kafka topic "detections".
  2. Ghi mỗi message thành một document trong MongoDB, collection "detections".

MongoDB được chọn vì kết quả đầu ra có cấu trúc bán cấu trúc (danh sách
bounding box lồng nhau, số lượng phần tử thay đổi theo từng frame),
rất phù hợp với mô hình document-oriented thay vì bảng quan hệ cố định.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [storage-server] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------- Cấu hình từ biến môi trường -------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_DETECTIONS = os.getenv("KAFKA_TOPIC_DETECTIONS", "detections")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "storage-group")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "people_counting")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "detections")


def connect_kafka_consumer(max_retries: int = 15, delay_seconds: int = 5) -> KafkaConsumer:
    """Kết nối tới Kafka consumer, thử lại nếu broker chưa sẵn sàng."""
    for attempt in range(1, max_retries + 1):
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_DETECTIONS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_CONSUMER_GROUP,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                auto_offset_reset="latest",
            )
            logger.info("Kết nối Kafka consumer thành công.")
            return consumer
        except NoBrokersAvailable:
            logger.warning(
                "Chưa kết nối được Kafka (lần %d/%d). Thử lại sau %ds...",
                attempt, max_retries, delay_seconds,
            )
            time.sleep(delay_seconds)
    raise RuntimeError("Không thể kết nối tới Kafka sau nhiều lần thử.")


def connect_mongo(max_retries: int = 15, delay_seconds: int = 5):
    """Kết nối tới MongoDB, thử lại nếu chưa sẵn sàng."""
    for attempt in range(1, max_retries + 1):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            client.admin.command("ping")  # Kiểm tra kết nối thực sự
            logger.info("Kết nối MongoDB thành công.")
            db = client[MONGO_DB_NAME]
            collection = db[MONGO_COLLECTION]
            # Tạo index để tăng tốc truy vấn theo camera_id và thời gian
            collection.create_index([("camera_id", 1), ("timestamp", -1)])
            return collection
        except ConnectionFailure:
            logger.warning(
                "Chưa kết nối được MongoDB (lần %d/%d). Thử lại sau %ds...",
                attempt, max_retries, delay_seconds,
            )
            time.sleep(delay_seconds)
    raise RuntimeError("Không thể kết nối tới MongoDB sau nhiều lần thử.")


def main():
    consumer = connect_kafka_consumer()
    collection = connect_mongo()

    logger.info("Bắt đầu lắng nghe topic '%s' và ghi vào MongoDB...", KAFKA_TOPIC_DETECTIONS)
    for message in consumer:
        try:
            data = message.value
            document = {
                "camera_id": data["camera_id"],
                "frame_id": data["frame_id"],
                "timestamp": data["timestamp"],
                "width": data.get("width"),
                "height": data.get("height"),
                "person_count": data["person_count"],
                "bboxes": data["bboxes"],
                "stored_at": datetime.now(timezone.utc).isoformat(),
            }

            collection.insert_one(document)
            logger.info(
                "Đã lưu frame_id=%s (camera=%s, person_count=%d) vào MongoDB.",
                data["frame_id"], data["camera_id"], data["person_count"],
            )

        except Exception:
            logger.exception("Lỗi khi lưu message vào MongoDB, bỏ qua và tiếp tục.")


if __name__ == "__main__":
    main()
