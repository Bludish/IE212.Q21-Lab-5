FROM python:3.11-slim

# Thư viện hệ thống cần cho OpenCV / Torch
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Tải sẵn trọng số YOLOv8n ngay khi build image để tránh phải tải lại
# mỗi lần container khởi động (tăng tốc độ start, tránh lỗi mạng lúc runtime).
RUN python -c "from ultralytics import YOLO; YOLO('yolov8n.pt')"

COPY process.py .

CMD ["python", "process.py"]
