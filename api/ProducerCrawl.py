import json
import logging
import os
import sys
import signal
import websocket
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Cấu hình Kafka từ biến môi trường
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9094")
TOPIC = os.getenv("KAFKA_TOPIC", "binance_trades")

# Khởi tạo Kafka Producer
def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,  # Thử lại tối đa 5 lần nếu gửi thất bại
            request_timeout_ms=30000  # Timeout sau 30 giây
        )
        logger.info(f"Kafka producer connected to broker: {KAFKA_BROKER}")
        return producer
    except KafkaError as e:
        logger.error(f"Failed to create Kafka producer: {e}")
        sys.exit(1)

# Xử lý tin nhắn từ WebSocket
def on_message(ws, message):
    try:
        data = json.loads(message)
        trade = {
            "trade_id": data["t"],
            "symbol": data["s"],
            "price": float(data["p"]),
            "quantity": float(data["q"]),
            "time": data["T"],
            "is_buyer_maker": data["m"]
        }
        # Gửi dữ liệu đến Kafka
        future = producer.send(TOPIC, trade)
        future.add_callback(lambda _: logger.info(f"Sent trade: {trade}"))
        future.add_errback(lambda e: logger.error(f"Failed to send trade: {e}"))
    except Exception as e:
        logger.error(f"Error processing message: {e}")

# Xử lý lỗi WebSocket
def on_error(ws, error):
    logger.error(f"WebSocket Error: {error}")

# Xử lý khi WebSocket đóng
def on_close(ws, close_status_code, close_msg):
    logger.info("WebSocket closed")
    producer.close()  # Đóng Kafka producer khi WebSocket đóng

# Xử lý khi WebSocket mở
def on_open(ws):
    logger.info("WebSocket connected")

# Xử lý tín hiệu dừng chương trình (Ctrl+C)
def signal_handler(sig, frame):
    logger.info("Shutting down gracefully...")
    ws.close()  # Đóng WebSocket
    producer.close()  # Đóng Kafka producer
    sys.exit(0)

# Khởi tạo Kafka producer
producer = create_kafka_producer()

# Kết nối WebSocket Binance
ws = websocket.WebSocketApp(
    "wss://stream.binance.com:9443/ws/btcusdt@trade",
    on_message=on_message,
    on_error=on_error,
    on_close=on_close,
    on_open=on_open
)

# Đăng ký signal handler để dừng chương trình đúng cách
signal.signal(signal.SIGINT, signal_handler)

# Chạy WebSocket
logger.info("Starting WebSocket connection...")
ws.run_forever(ping_interval=10)