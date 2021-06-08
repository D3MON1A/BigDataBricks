from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import *

mymongo = SparkSession.builder.appName("SpendByCustomerSorted")\
.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/customerorders.orders") \
.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/customerorders.orders") \
.getOrCreate()

sc= SparkContext.getOrCreate()

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:////home/consultant/Desktop/BigDataBricks/final_pipeline/customers_orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

# results = totalByCustomer.collect()
# for result in results:
#     print(result)
schema = StructType([\
StructField("customers_orders", StringType(), True),\
StructField("total_spent", StringType(), True)])



df = mymongo.createDataFrame(schema)
df.write.format("mongo").mode("overwrite").save()
df.show()

# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 total_spent.py