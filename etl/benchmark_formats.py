import argparse
import csv
import os
import shutil
import statistics
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

AZURE_PACKAGE = 'org.apache.hadoop:hadoop-azure:3.3.4'

CSV_SCHEMA = StructType([
    StructField('event_type', StringType()),
    StructField('product_id', LongType()),
    StructField('category_id', LongType()),
    StructField('category_code', StringType()),
    StructField('brand', StringType()),
    StructField('price', DoubleType()),
    StructField('user_id', LongType()),
    StructField('user_session', StringType()),
    StructField('event_ts', StringType()),
    StructField('category_main', StringType()),
])

parser = argparse.ArgumentParser()
parser.add_argument('--out', default='./output', help='thư mục gốc local chứa parquet/ và csv/')
parser.add_argument('--azure-account', default=None, help='tên Storage account; nếu có thì đọc/ghi trên Azure')
parser.add_argument('--azure-container', default='ecommerce-data')
parser.add_argument('--azure-folder', default='etl')
parser.add_argument('--repeats', type=int, default=3, help='số lần đo mỗi truy vấn')
parser.add_argument('--skip-write', action='store_true', help='bỏ qua phần đo tốc độ ghi')
parser.add_argument('--driver-memory', default='2g')
parser.add_argument('--results-csv', default='benchmark_results.csv')
args = parser.parse_args()

use_azure = bool(args.azure_account)
azure_key = None
if use_azure:
    azure_key = os.environ.get('AZURE_STORAGE_KEY')
    if not azure_key:
        sys.exit("Thiếu biến môi trường AZURE_STORAGE_KEY. Chạy trước: export AZURE_STORAGE_KEY='<access key>'")
    base = (f'abfss://{args.azure_container}@{args.azure_account}.dfs.core.windows.net/'
            f'{args.azure_folder}')
else:
    base = args.out

parquet_path = f'{base}/parquet'
csv_path = f'{base}/csv'
bench_parquet = f'{base}/bench_write/parquet'
bench_csv = f'{base}/bench_write/csv'

builder = (
    SparkSession.builder.appName('format-benchmark')
    .config('spark.driver.memory', args.driver_memory)
    .config('spark.sql.session.timeZone', 'UTC')
)
if use_azure:
    builder = (builder.config('spark.jars.packages', AZURE_PACKAGE)
               .config(f'spark.hadoop.fs.azure.account.key.{args.azure_account}.dfs.core.windows.net',
                       azure_key))
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel('ERROR')

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


for p in (parquet_path, csv_path):
    if not path_exists(p):
        sys.exit(f'Không tìm thấy {p}. Hãy chạy etl_stream.py trước.')


def read_parquet():
    return spark.read.parquet(parquet_path)


def read_csv():
    return spark.read.schema(CSV_SCHEMA).option('header', True).csv(csv_path)


QUERIES = {
    'Q1 đếm số dòng': lambda df: df.count(),
    'Q2 doanh thu theo nhóm hàng': lambda df: (
        df.filter(F.col('event_type') == 'purchase')
        .groupBy('category_main').agg(F.sum('price').alias('doanh_thu')).collect()),
    'Q3 số sự kiện theo ngày và loại': lambda df: (
        df.groupBy('event_date', 'event_type').count().collect()),
}


def timed(fn):
    t = time.perf_counter()
    fn()
    return time.perf_counter() - t


results = []
rows = read_parquet().count()
print(f'Dữ liệu: {rows:,} dòng | đọc từ {base}')
size_pq, size_csv = dir_size(parquet_path), dir_size(csv_path)
print(f'Dung lượng: Parquet {size_pq / 1e6:.1f} MB | CSV {size_csv / 1e6:.1f} MB '
      f'(CSV lớn gấp {size_csv / size_pq:.1f} lần)')

print('\n=== TỐC ĐỘ ĐỌC (giây) ===')
for qname, q in QUERIES.items():
    line = {}
    for fmt, reader in (('Parquet', read_parquet), ('CSV', read_csv)):
        times = []
        for run in range(1, args.repeats + 1):
            sec = timed(lambda: q(reader()))
            times.append(sec)
            results.append((qname, fmt, run, round(sec, 3)))
        line[fmt] = times
    mp, mc = statistics.mean(line['Parquet']), statistics.mean(line['CSV'])
    print(f'{qname}: Parquet TB {mp:.2f}s (nhỏ nhất {min(line["Parquet"]):.2f}) | '
          f'CSV TB {mc:.2f}s (nhỏ nhất {min(line["CSV"]):.2f}) | CSV chậm gấp {mc / mp:.1f} lần')

if not args.skip_write:
    print('\n=== TỐC ĐỘ GHI (giây) ===')
    src = read_parquet().cache()
    src.count()
    for p in (bench_parquet, bench_csv):
        remove_path(p)
    t_pq = timed(lambda: src.write.mode('overwrite').partitionBy('event_date')
                 .option('compression', 'snappy').parquet(bench_parquet))
    t_csv = timed(lambda: src.write.mode('overwrite').partitionBy('event_date')
                  .option('header', True).csv(bench_csv))
    results.append(('ghi toàn bộ dữ liệu', 'Parquet', 1, round(t_pq, 3)))
    results.append(('ghi toàn bộ dữ liệu', 'CSV', 1, round(t_csv, 3)))
    print(f'Ghi {rows:,} dòng: Parquet {t_pq:.2f}s ({dir_size(bench_parquet) / 1e6:.1f} MB) | '
          f'CSV {t_csv:.2f}s ({dir_size(bench_csv) / 1e6:.1f} MB) | CSV chậm gấp {t_csv / t_pq:.1f} lần')
    src.unpersist()
    for p in (bench_parquet, bench_csv):
        remove_path(p)

with open(args.results_csv, 'w', newline='', encoding='utf-8') as f:
    w = csv.writer(f)
    w.writerow(['phep_do', 'dinh_dang', 'lan', 'giay'])
    w.writerows(results)
    w.writerow(['dung_luong_MB', 'Parquet', 1, round(size_pq / 1e6, 2)])
    w.writerow(['dung_luong_MB', 'CSV', 1, round(size_csv / 1e6, 2)])
    w.writerow(['so_dong', 'ca_hai', 1, rows])
print(f'\nĐã lưu {args.results_csv}')