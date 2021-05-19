from pyspark.context import SparkContext
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

spark = SparkSession.builder.master("local[1]").appName("dataframe.py").getOrCreate()
# Lets read the csv file now using spark.read.csv
df = spark.read.csv('/home/consultant/Desktop/Feuerzeug/Spark/Sample-Spreadsheet-100-rows.csv',header=True)

# Lets check our data type
# type(df)

pyspark.sql.dataframe.DataFrame

df.show()

