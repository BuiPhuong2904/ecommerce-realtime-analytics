import argparse
import csv
import json
import statistics
import time

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument('--server', required=True, help='địa chỉ Kafka broker, ví dụ 1.2.3.4:9092')
parser.add_argument('--label', required=True, help='nhãn để đặt tên file kết quả, ví dụ japan')
parser.add_argument('--topic', default='latency-test', help='topic dùng để đo (tự tạo nếu chưa có)')
parser.add_argument('--n', type=int, default=300, help='số tin đo mỗi lượt')
parser.add_argument('--runs', type=int, default=3, help='số lượt đo')
parser.add_argument('--warmup', type=int, default=20, help='số tin gửi trước, không tính vào kết quả')
parser.add_argument('--size', type=int, default=300, help='kích thước mỗi tin (byte)')
args = parser.parse_args()


def make_payload(size):
    event = {'event_type': 'view', 'product_id': 1004237, 'category_id': 2053013555631882655,
             'category_code': 'electronics.smartphone', 'brand': 'apple', 'price': 1081.98,
             'user_id': 535871217, 'user_session': 'latency-test'}
    data = json.dumps(event).encode('utf-8')
    return data + b' ' * max(0, size - len(data))


def percentile(sorted_vals, p):
    return sorted_vals[min(len(sorted_vals) - 1, round(p / 100 * (len(sorted_vals) - 1)))]


def describe(vals):
    s = sorted(vals)
    return {
        'min': s[0], 'mean': statistics.mean(s), 'median': statistics.median(s),
        'p95': percentile(s, 95), 'p99': percentile(s, 99), 'max': s[-1],
    }


def fmt(d):
    return (f"min {d['min']:.1f} | trung bình {d['mean']:.1f} | trung vị {d['median']:.1f} | "
            f"p95 {d['p95']:.1f} | p99 {d['p99']:.1f} | max {d['max']:.1f} (ms)")


payload = make_payload(args.size)

t0 = time.perf_counter()
producer = KafkaProducer(
    bootstrap_servers=[args.server],
    linger_ms=0,   # gửi ngay, không chờ gom batch
    acks=1,        # chờ broker leader xác nhận
    request_timeout_ms=30000,
    max_block_ms=30000,
)
print(f'[{args.label}] Kết nối ban đầu tới {args.server}: {(time.perf_counter() - t0) * 1000:.0f} ms')

for _ in range(args.warmup):  # làm nóng: tạo topic, lấy metadata, mở kết nối
    producer.send(args.topic, value=payload).get(timeout=30)

rows = []
all_vals = []
errors = 0
for run in range(1, args.runs + 1):
    vals = []
    for seq in range(1, args.n + 1):
        t = time.perf_counter()
        try:
            producer.send(args.topic, value=payload).get(timeout=30)
        except Exception:
            errors += 1
            continue
        ms = (time.perf_counter() - t) * 1000
        vals.append(ms)
        rows.append((run, seq, round(ms, 3)))
    if vals:
        all_vals.extend(vals)
        print(f'[{args.label}] Lượt {run}: {len(vals)} tin | {fmt(describe(vals))}')

producer.close()

if all_vals:
    print(f'[{args.label}] TỔNG ({len(all_vals)} tin, lỗi {errors}): {fmt(describe(all_vals))}')
    out = f'latency_{args.label}.csv'
    with open(out, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['run', 'seq', 'latency_ms'])
        w.writerows(rows)
    print(f'Đã lưu {out}')
else:
    print(f'[{args.label}] Không đo được tin nào (lỗi {errors}). Kiểm tra lại địa chỉ và firewall.')