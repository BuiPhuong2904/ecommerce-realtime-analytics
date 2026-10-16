import argparse
import os
import shutil
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

KAFKA_PACKAGE = 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1'
AZURE_PACKAGE = 'org.apache.hadoop:hadoop-azure:3.3.4'
VALID_EVENTS = ['view', 'cart', 'remove_from_cart', 'purchase']

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
parser.add_argument('--out', default='./output',
                    help='thư mục gốc khi ghi local (chứa parquet/, csv/, _checkpoint/)')
parser.add_argument('--azure-account', default=None,
                    help='tên Storage account; nếu có thì ghi lên Azure thay vì local')
parser.add_argument('--azure-container', default='ecommerce-data', help='tên container')
parser.add_argument('--azure-folder', default='etl', help='thư mục con trong container')
parser.add_argument('--once', action='store_true', help='xử lý hết dữ liệu đang có rồi dừng')
parser.add_argument('--interval', type=int, default=10, help='giây giữa các micro-batch (chế độ liên tục)')
parser.add_argument('--max-per-batch', type=int, default=5000, help='số tin tối đa mỗi micro-batch')
parser.add_argument('--reset', action='store_true', help='xóa output và checkpoint cũ trước khi chạy')
args = parser.parse_args()

use_azure = bool(args.azure_account)
azure_key = None
if use_azure:
    azure_key = os.environ.get('AZURE_STORAGE_KEY')
    if not azure_key:
        sys.exit("Thiếu biến môi trường AZURE_STORAGE_KEY. Chạy trước: export AZURE_STORAGE_KEY='<access key>'")
    base = (f'abfss://{args.azure_container}@{args.azure_account}.dfs.core.windows.net/'
            f'{args.azure_folder}')
    destination = f'Azure Storage {args.azure_account}/{args.azure_container}/{args.azure_folder}'
else:
    base = args.out
    destination = args.out

parquet_path = f'{base}/parquet'
csv_path = f'{base}/csv'
checkpoint_path = f'{base}/_checkpoint'

packages = [KAFKA_PACKAGE] + ([AZURE_PACKAGE] if use_azure else [])
builder = (
    SparkSession.builder.appName('ecommerce-etl')
    .config('spark.jars.packages', ','.join(packages))
    .config('spark.sql.session.timeZone', 'UTC')
    .config('spark.sql.shuffle.partitions', '3')
)
if use_azure:
    builder = builder.config(
        f'spark.hadoop.fs.azure.account.key.{args.azure_account}.dfs.core.windows.net', azure_key)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

def hadoop_path(path):
    jpath = spark._jvm.org.apache.hadoop.fs.Path(path)
    return jpath, jpath.getFileSystem(spark._jsc.hadoopConfiguration())


def path_exists(path):
    if use_azure:
        jpath, fs = hadoop_path(path)
        return fs.exists(jpath)
    return os.path.isdir(path)


def remove_path(path):
    if use_azure:
        jpath, fs = hadoop_path(path)
        if fs.exists(jpath):
            fs.delete(jpath, True)
    else:
        shutil.rmtree(path, ignore_errors=True)


def dir_size(path):
    if use_azure:
        jpath, fs = hadoop_path(path)
        return fs.getContentSummary(jpath).getLength()
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs)


if args.reset:
    for p in (parquet_path, csv_path, checkpoint_path):
        remove_path(p)


def clean(df):
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
    kept = cleaned.count()
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


def summarize():
    print('=== TỔNG KẾT ===')
    for name, path in (('Parquet', parquet_path), ('CSV', csv_path)):
        if not path_exists(path):
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

print(f'ETL bắt đầu: Kafka {args.server}, topic {args.topic} -> {destination}', flush=True)
query = writer.start()
try:
    query.awaitTermination()
except KeyboardInterrupt:
    print('Đã dừng bằng tay (Ctrl+C).')
    query.stop()

if args.once:
    summarize()