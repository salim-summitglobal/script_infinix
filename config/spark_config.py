from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkFastAPI") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .getOrCreate()
spark.conf.set("spark.sql.debug.maxToStringFields", '2000')
