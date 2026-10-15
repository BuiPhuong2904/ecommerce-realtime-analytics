import findspark
findspark.init() # Lệnh này tự động tìm SPARK_HOME

from pyspark.sql import SparkSession

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("TestLocalSpark") \
    .master("local[*]") \
    .getOrCreate()

print("Spark Version:", spark.version)
spark.stop()