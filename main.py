import json
import time

from pyspark.sql import SparkSession

from processing import TaxiDataProcessor


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option") \
    .getOrCreate()
sc = spark.sparkContext
data_file = "data.txt"
df = sc.textFile(data_file, minPartitions=100).map(lambda x: eval(x))
td = TaxiDataProcessor(df)

top_drivers = td.top_drivers(100)
with open("top_drivers.json", "w") as f:
    json.dump(top_drivers, f, indent=4, sort_keys=True)
