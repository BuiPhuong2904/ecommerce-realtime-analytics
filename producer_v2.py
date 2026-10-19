import argparse
import csv
import json
import os
import time
from itertools import islice

import pandas as pd
from kafka import KafkaProducer

DEFAULT_CSV = r'D:\Do_An\IS402_DTDM\Seminar\Data\2019-Oct.csv'

parser = argparse.ArgumentParser()
parser.add_argument('--csv', default=DEFAULT_CSV, help='đường dẫn file CSV')
parser.add_argument('--server', default='74.176.216.15:9092', help='địa chỉ Kafka broker')
parser.add_argument('--topic', default='ecommerce-events', help='tên topic cần gửi')
parser.add_argument('--rate', type=float, default=100,
                    help='số tin/giây; 0 = gửi nhanh nhất có thể')
parser.add_argument('--limit', type=int, default=0,
                    help='số dòng tối đa cần gửi; 0 = gửi hết file')
parser.add_argument('--linger-ms', type=int, default=20,
                    help='thời gian chờ gom batch (ms)')
parser.add_argument('--batch-size', type=int, default=16384,
                    help='kích thước batch (byte)')
parser.add_argument('--compression', default=None, choices=['gzip'],
                    help='nén dữ liệu khi gửi (mặc định: không nén)')
parser.add_argument('--results-csv', default='producer_results.csv',
                    help='file ghi kết quả mỗi lần chạy (thêm dòng mới vào cuối); để rỗng nếu không muốn ghi')
args = parser.parse_args()


def to_json(obj):
    return json.dumps(
        obj, default=lambda o: o.item() if hasattr(o, 'item') else str(o)
    ).encode('utf-8')


def to_key(k):
    return str(k).encode('utf-8') if k is not None else None


producer = KafkaProducer(
    bootstrap_servers=[args.server],
    key_serializer=to_key,
    value_serializer=to_json,
    linger_ms=args.linger_ms,
    batch_size=args.batch_size,
    compression_type=args.compression,
)

errors = 0
acked = 0


def on_ack(_metadata):
    global acked
    acked += 1


def on_error(exc):
    global errors
    errors += 1


def events():
    for chunk in pd.read_csv(args.csv, chunksize=10000):
        for rec in chunk.to_dict('records'):
            yield {k: v for k, v in rec.items() if pd.notna(v)}


log_every = 1000 if args.rate > 0 else 50000
sent = 0
start = time.time()
print(f'Bắt đầu gửi lên {args.server}, topic {args.topic} '
      f'(rate={args.rate}, limit={args.limit or "toàn bộ file"}, '
      f'linger_ms={args.linger_ms}, batch_size={args.batch_size}, '
      f'compression={args.compression})')

try:
    for event in islice(events(), args.limit or None):
        producer.send(args.topic, key=event.get('user_session'), value=event) \
            .add_callback(on_ack).add_errback(on_error)
        sent += 1

        if args.rate > 0:
            delay = start + sent / args.rate - time.time()
            if delay > 0:
                time.sleep(delay)

        if sent % log_every == 0:
            elapsed = time.time() - start
            print(f'Đã đưa vào hàng đợi {sent:,} tin | broker đã xác nhận {acked:,} tin '
                  f'({acked / elapsed:,.0f} tin/giây) | lỗi: {errors}')
except KeyboardInterrupt:
    print('Đã dừng bằng tay (Ctrl+C).')
finally:
    producer.flush()
    elapsed = max(time.time() - start, 1e-9)
    print(f'XONG: {sent:,} tin ({acked:,} đã được xác nhận) trong {elapsed:.1f} giây '
          f'({sent / elapsed:,.0f} tin/giây), lỗi: {errors}')
    try:
        m = producer.metrics().get('producer-metrics', {})

        def g(key):
            v = m.get(key)
            return f'{v:,.2f}' if isinstance(v, (int, float)) else 'n/a'

        print(f'Chỉ số producer: batch TB {g("batch-size-avg")} byte | '
              f'{g("records-per-request-avg")} tin/request | '
              f'tỉ lệ nén {g("compression-rate-avg")} (sau nén / trước nén) | '
              f'độ trễ request TB {g("request-latency-avg")} ms')
    except Exception:
        pass
    producer.close()

    if args.results_csv:
        new_file = not os.path.exists(args.results_csv)
        with open(args.results_csv, 'a', newline='', encoding='utf-8') as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(['thoi_diem', 'server', 'topic', 'so_tin', 'da_xac_nhan', 'loi', 'linger_ms',
                            'batch_size', 'nen', 'rate', 'giay', 'tin_moi_giay'])
            w.writerow([time.strftime('%Y-%m-%d %H:%M:%S'), args.server, args.topic, sent, acked, errors,
                        args.linger_ms, args.batch_size, args.compression or 'none', args.rate,
                        round(elapsed, 2), round(sent / elapsed, 1)])
        print(f'Đã ghi kết quả vào {args.results_csv}')