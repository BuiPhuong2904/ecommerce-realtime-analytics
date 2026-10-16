import argparse
import os
import shutil
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

KAFKA_PACKAGE = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1'  # phải khớp phiên bản pyspark
VALID_EVENTS = ['view', 'cart', 'remove_from_cart', 'purchase']

# Các cột bị null đã bị producer bỏ khỏi JSON, nên cột nào cũng có thể thiếu
SCHEMA = StructType([
    StructField('event_time', StringType()),
    StructField('event_type', StringType()),
    StructField('product_id', LongType()),
    StructField('category_id', LongType()),
    StructField('category_code', StringType()),
    StructField('brand', StringType()),
    StructField('price', DoubleType()),
    StructField('user_id', LongType()),
    StructField('user_session', StringType()),
])

parser = argparse.ArgumentParser()
parser.add_argument('--server', default='74.176.216.15:9092', help='địa chỉ Kafka broker')
parser.add_argument('--topic', default='ecommerce-events')
parser.add_argument('--out', default='./output', help='thư mục gốc chứa parquet/, csv/, _checkpoint/')
parser.add_argument('--once', action='store_true', help='xử lý hết dữ liệu đang có rồi dừng')
parser.add_argument('--interval', type=int, default=10, help='giây giữa các micro-batch (chế độ liên tục)')
parser.add_argument('--max-per-batch', type=int, default=5000, help='số tin tối đa mỗi micro-batch')
parser.add_argument('--reset', action='store_true', help='xóa output và checkpoint cũ trước khi chạy')
args = parser.parse_args()

parquet_path = f'{args.out}/parquet'
csv_path = f'{args.out}/csv'
checkpoint_path = f'{args.out}/_checkpoint'

if args.reset:
    for p in (parquet_path, csv_path, checkpoint_path):
        shutil.rmtree(p, ignore_errors=True)

spark = (
    SparkSession.builder.appName('ecommerce-etl')
    .config('spark.jars.packages', KAFKA_PACKAGE)
    .config('spark.sql.session.timeZone', 'UTC')
    .config('spark.sql.shuffle.partitions', '3')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')


def clean(df):
    """Làm sạch một micro-batch đã parse JSON."""
    return (
        df.withColumn('event_ts', F.to_timestamp('event_time', "yyyy-MM-dd HH:mm:ss 'UTC'"))
        .filter(F.col('event_ts').isNotNull())
        .filter(F.col('event_type').isin(VALID_EVENTS))
        .filter(F.col('product_id').isNotNull() & F.col('user_id').isNotNull())
        .filter(F.col('price').isNotNull() & (F.col('price') >= 0))
        .withColumn('brand', F.coalesce('brand', F.lit('unknown')))
        .withColumn('category_code', F.coalesce('category_code', F.lit('unknown')))
        .withColumn('category_main', F.split('category_code', r'\.').getItem(0))
        .withColumn('event_date', F.to_date('event_ts'))
        .drop('event_time')
    )


def process_batch(batch_df, batch_id):
    t0 = time.time()
    batch_df.persist()
    total = batch_df.count()
    if total == 0:
        batch_df.unpersist()
        return

    cleaned = clean(batch_df).persist()
    kept = cleaned.count()  # tính xong trước khi đo thời gian ghi để so sánh công bằng
    t1 = time.time()

    if batch_id == 0:
        print('--- Mẫu dữ liệu TRƯỚC khi làm sạch ---')
        batch_df.show(5, truncate=False)
        print('--- Mẫu dữ liệu SAU khi làm sạch ---')
        cleaned.show(5, truncate=False)

    (cleaned.write.mode('append').partitionBy('event_date')
        .option('compression', 'snappy').parquet(parquet_path))
    t2 = time.time()
    (cleaned.write.mode('append').partitionBy('event_date')
        .option('header', True).csv(csv_path))
    t3 = time.time()

    cleaned.unpersist()
    batch_df.unpersist()
    print(f'[Batch {batch_id}] đọc {total:,} dòng | giữ {kept:,} | loại {total - kept:,} | '
          f'ghi Parquet {t2 - t1:.2f}s | ghi CSV {t3 - t2:.2f}s | tổng {t3 - t0:.2f}s', flush=True)


def dir_size(path):
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs)


def summarize():
    """Đọc lại hai thư mục output, in số dòng, dung lượng, thời gian đọc."""
    print('=== TỔNG KẾT ===')
    for name, path in (('Parquet', parquet_path), ('CSV', csv_path)):
        if not os.path.isdir(path):
            print(f'{name}: chưa có dữ liệu ở {path}')
            continue
        t = time.time()
        reader = spark.read.parquet if name == 'Parquet' else spark.read.option('header', True).csv
        n = reader(path).count()
        dt = time.time() - t
        print(f'{name}: {n:,} dòng | dung lượng {dir_size(path) / 1e6:.1f} MB | đọc + đếm {dt:.2f}s')
    print('--- Schema và 5 dòng mẫu (Parquet) ---')
    pdf = spark.read.parquet(parquet_path)
    pdf.printSchema()
    pdf.show(5, truncate=False)


parsed = (
    spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', args.server)
    .option('subscribe', args.topic)
    .option('startingOffsets', 'earliest')
    .option('maxOffsetsPerTrigger', args.max_per_batch)
    .option('failOnDataLoss', 'false')
    .load()
    .select(F.from_json(F.col('value').cast('string'), SCHEMA).alias('e'))
    .select('e.*')
)

writer = parsed.writeStream.foreachBatch(process_batch).option('checkpointLocation', checkpoint_path)
writer = (writer.trigger(availableNow=True) if args.once
          else writer.trigger(processingTime=f'{args.interval} seconds'))

print(f'ETL bắt đầu: Kafka {args.server}, topic {args.topic} -> {args.out}', flush=True)
query = writer.start()
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print('Đã dừng bằng tay (Ctrl+C).')
    query.stop()

if args.once:
    summarize()