from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import *


mymongo = SparkSession.builder.appName("SpendByCustomer")\
.config("spark.mongodb.output.uri","mongodb://mongo cloud path here") \
.getOrCreate()      


sc= SparkContext.getOrCreate()

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:////home/consultant/Desktop/BigDataBricks/final_pipeline/customers_orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

schema = StructType([\
StructField("customers_orders", StringType(), True),\
StructField("total_spent", StringType(), True)])

df = mymongo.createDataFrame(totalByCustomer, schema)
df.write.format("mongo").mode("overwrite").save()
df.show()

# spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.2 total_spent.py




