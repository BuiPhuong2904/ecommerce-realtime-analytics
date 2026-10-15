import json
import time
import pandas as pd
from kafka import KafkaProducer

# 1. Cấu hình kết nối tới Kafka trên Azure
producer = KafkaProducer(
    bootstrap_servers=['74.176.216.15:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

csv_file_path = r'D:\Do_An\IS402_DTDM\Seminar\Data\2019-Oct.csv' 

print("Đang nạp dữ liệu, quá trình này có thể mất chút thời gian tùy thuộc vào RAM máy tính...")

chunksize = 1000 
for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
    for index, row in chunk.iterrows():
        event_data = row.dropna().to_dict()
        producer.send('ecommerce-events', value=event_data)
        print(f"Đã gửi sự kiện: {event_data.get('event_type', 'N/A')} - Sản phẩm: {event_data.get('product_id', 'N/A')}")
        time.sleep(0.05) 

print("Đã hoàn tất quá trình stream toàn bộ dữ liệu!")