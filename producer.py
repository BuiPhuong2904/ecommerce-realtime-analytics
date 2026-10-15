import pandas as pd
import time
import json
from kafka import KafkaProducer

# CẤU HÌNH KAFKA
# Thay 'IP_AZURE_CUA_BAN' bằng địa chỉ IP Public của máy ảo VM Đông Nam Á
KAFKA_BROKER = 'IP_AZURE_CUA_BAN:9092' 
TOPIC_NAME = 'ecommerce_events'

print("Đang kết nối tới Kafka...")
try:
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kết nối Kafka thành công!")
except Exception as e:
    print(f"Chưa kết nối được Kafka. Lỗi: {e}")

chunk_size = 10000
file_path = "../archive/2019-Oct.csv"

print(f"Bắt đầu đọc và đẩy stream từ {file_path}...")
try:
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Lặp qua từng dòng trong chunk
        for index, row in chunk.iterrows():
            # Chuyển dòng dữ liệu thành dictionary
            data = row.to_dict()
            # Gửi dữ liệu lên Kafka
            producer.send(TOPIC_NAME, value=data)
        
        # Đẩy dữ liệu đi ngay lập tức
        producer.flush()
        print(f"Đã bắn thành công {len(chunk)} dòng sự kiện lên topic '{TOPIC_NAME}'.")
        
        # Tạm dừng 1 giây để mô phỏng real-time
        time.sleep(1)
        
        # Tạm thời vẫn giữ lệnh break để test chunk đầu tiên
        break 
        
except FileNotFoundError:
    print("Lỗi: Không tìm thấy file dữ liệu.")