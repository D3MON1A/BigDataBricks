
from pyspark.sql import SQLContext, SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *


sc = SparkContext.getOrCreate()
spark = SparkSession.builder.master("local[1]").appName("FetchingTwitter").getOrCreate()


df = sc.textFile("twitter_data/FlumeData.1620691363362").map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b).toDF(['Word', 'Count'])

df.write.format("jdbc")\
	.mode("overwrite")\
	.option("url", "jdbc:mysql://localhost:3306/Twitter") \
	.option("dbtable", "WordCounting") \
	.option("user", "YOUR USERNAME") \
	.option("password", "YOUR PASSWORD")\
    .option("driver", "com.mysql.jdbc.Driver")\
    .save()


















