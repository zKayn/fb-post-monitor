# Dựng bằng Dockerfile chuẩn (không qua Railpack) để tránh lỗi "frontend grpc server closed unexpectedly".
FROM python:3.13-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY fb_post_monitor.py .
CMD ["python", "fb_post_monitor.py"]
