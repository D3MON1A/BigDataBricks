from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# spark-submit --packages org.apache.spark:spark-avro_2.11:2.4.4 004sparkTesting.py
# command to pass.

spark: SparkSession = SparkSession.builder.master("local[1]")\
.appName("sparkjsonavro")\
.getOrCreate()

sc = SparkContext

df = spark.read.option("multiline",'true').json("file:///home/consultant/Desktop/BigDataBricks/samirs_help/people2.json")
df.printSchema()
df.show()

# Write Avro File

df.write.format("parquet")\
.mode("append") \
.option("mergeSchema", "true") \
.save("file:///home/consultant/Desktop/BigDataBricks/Parquet")


# Read Avro File

# avroDf = spark.read\
# .option("inferSchema", "true")\
# .format("parquet")\
# .load("/sparkTesting/")

avroDf.printSchema()
avroDf.show()