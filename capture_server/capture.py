"""
capture_server / capture.py
----------------------------------
Server 1: Nhận (giả lập) khung hình từ camera.

Trong bài tập này, "camera" được giả lập bằng một file video .mp4 đọc
tuần hoàn (loop) bằng OpenCV. Mỗi khung hình đọc được sẽ:
  1. Được resize để giảm băng thông truyền qua Kafka.
  2. Được encode sang JPEG rồi base64 để đóng gói vào JSON.
  3. Được publish vào Kafka topic "raw-frames" (đóng vai trò hàng đợi
     dữ liệu lớn giữa server capture và server xử lý).

Việc đọc frame được giới hạn theo TARGET_FPS để giả lập tốc độ khung
hình thực tế của một camera giám sát, tránh làm ngập Kafka.
"""

import base64
import json
import logging
import os
import time
from datetime import datetime, timezone

import cv2
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [capture-server] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ------------------------- Cấu hình từ biến môi trường -------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RAW_FRAMES = os.getenv("KAFKA_TOPIC_RAW_FRAMES", "raw-frames")
VIDEO_PATH = os.getenv("VIDEO_PATH", "/app/videos/sample.mp4")
CAMERA_ID = os.getenv("CAMERA_ID", "cam01")
TARGET_FPS = float(os.getenv("TARGET_FPS", "5"))
FRAME_WIDTH = int(os.getenv("FRAME_WIDTH", "640"))
JPEG_QUALITY = int(os.getenv("JPEG_QUALITY", "80"))


def connect_kafka_producer(max_retries: int = 15, delay_seconds: int = 5) -> KafkaProducer:
    """Kết nối tới Kafka, thử lại nhiều lần vì Kafka có thể khởi động chậm hơn."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                max_request_size=5 * 1024 * 1024,  # 5MB, khớp với cấu hình broker
                linger_ms=10,
            )
            logger.info("Kết nối Kafka producer thành công.")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                "Chưa kết nối được Kafka (lần %d/%d). Thử lại sau %ds...",
                attempt, max_retries, delay_seconds,
            )
            time.sleep(delay_seconds)
    raise RuntimeError("Không thể kết nối tới Kafka sau nhiều lần thử.")


def resize_frame(frame, target_width: int):
    """Resize khung hình giữ tỉ lệ, giảm băng thông khi truyền qua Kafka."""
    h, w = frame.shape[:2]
    if w <= target_width:
        return frame
    ratio = target_width / float(w)
    new_size = (target_width, int(h * ratio))
    return cv2.resize(frame, new_size, interpolation=cv2.INTER_AREA)


def encode_frame_to_base64(frame) -> str:
    """Encode khung hình sang JPEG rồi base64 để nhúng vào JSON payload."""
    success, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, JPEG_QUALITY])
    if not success:
        raise ValueError("Không thể encode khung hình sang JPEG.")
    return base64.b64encode(buffer).decode("utf-8")


def main():
    if not os.path.exists(VIDEO_PATH):
        raise FileNotFoundError(
            f"Không tìm thấy video nguồn tại {VIDEO_PATH}. "
            "Hãy đặt file .mp4 vào thư mục sample_videos/ với tên tương ứng."
        )

    producer = connect_kafka_producer()
    frame_interval = 1.0 / TARGET_FPS if TARGET_FPS > 0 else 0
    frame_id = 0

    logger.info(
        "Bắt đầu giả lập camera '%s' từ video '%s' với tốc độ %s FPS.",
        CAMERA_ID, VIDEO_PATH, TARGET_FPS,
    )

    while True:
        cap = cv2.VideoCapture(VIDEO_PATH)
        if not cap.isOpened():
            raise RuntimeError(f"Không thể mở video: {VIDEO_PATH}")

        while True:
            start_time = time.time()
            ret, frame = cap.read()

            if not ret:
                # Hết video -> quay lại từ đầu để giả lập luồng camera liên tục
                logger.info("Video kết thúc, phát lại từ đầu (loop).")
                break

            frame = resize_frame(frame, FRAME_WIDTH)
            encoded_image = encode_frame_to_base64(frame)

            message = {
                "camera_id": CAMERA_ID,
                "frame_id": frame_id,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "width": frame.shape[1],
                "height": frame.shape[0],
                "image": encoded_image,
            }

            producer.send(KAFKA_TOPIC_RAW_FRAMES, value=message)
            logger.info("Đã gửi frame_id=%d (camera=%s) tới topic '%s'.",
                        frame_id, CAMERA_ID, KAFKA_TOPIC_RAW_FRAMES)

            frame_id += 1

            # Giữ nhịp gửi frame đúng theo TARGET_FPS
            elapsed = time.time() - start_time
            sleep_time = max(0.0, frame_interval - elapsed)
            time.sleep(sleep_time)

        cap.release()

    # (Không bao giờ đến đây vì vòng lặp ngoài chạy vô hạn để giả lập camera 24/7)


if __name__ == "__main__":
    main()
