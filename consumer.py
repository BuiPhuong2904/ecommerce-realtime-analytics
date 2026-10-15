import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# CẤU HÌNH KAFKA BROKER (Lấy IP từ Thành viên B)
KAFKA_BROKER = 'IP_AZURE_CUA_BAN:9092' 
TOPIC_NAME = 'ecommerce_events'

# 1. KHỞI TẠO SPARK SESSION VỚI THƯ VIỆN KAFKA
print("Đang khởi tạo PySpark và tải thư viện Kafka...")
spark = SparkSession.builder \
    .appName("EcommerceRealtimeAnalytics") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Tắt bớt log rác để dễ nhìn console
spark.sparkContext.setLogLevel("ERROR")

# 2. ĐỊNH NGHĨA SCHEMA (CẤU TRÚC DỮ LIỆU) TỪ FILE CSV
schema = StructType([
    StructField("event_time", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_code", StringType(), True),
    StructField("brand", StringType(), True),
    StructField("price", DoubleType(), True),  # Ép giá tiền về số thực để tính toán
    StructField("user_id", StringType(), True),
    StructField("user_session", StringType(), True)
])

print(f"Đang kết nối để lắng nghe Topic: '{TOPIC_NAME}'...")

# 3. EXTRACT (THU THẬP): ĐỌC STREAM TỪ KAFKA
try:
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPIC_NAME) \
        .option("startingOffsets", "latest") \
        .load()

    # 4. TRANSFORM (BIẾN ĐỔI): CHUYỂN JSON THÀNH BẢNG & LỌC DỮ LIỆU
    # Chuyển đổi dữ liệu raw từ Kafka thành định dạng chuỗi JSON
    parsed_df = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    # [Xử lý nghiệp vụ]: Chỉ lọc ra các sự kiện mua hàng (purchase)
    purchase_df = parsed_df.filter(col("event_type") == "purchase")

    # 5. LOAD (TẢI): IN RA MÀN HÌNH ĐỂ TEST (Giai đoạn sau sẽ ghi ra Parquet/CSV)
    print("Bắt đầu xử lý luồng ETL tự động...")
    query = purchase_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

except Exception as e:
    print(f"Lỗi quá trình streaming: {e}")