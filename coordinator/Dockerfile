FROM python:3.12-slim
WORKDIR /app
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 boto3 kazoo
COPY coordinator.py /app/coordinator.py
