import boto3
import json
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import *


mymongo = SparkSession.builder.appName("awsbucket")\
.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/bucket1.example1") \
.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/bucket1.example1") \
.getOrCreate()

s3 = boto3.resource('s3',
                    aws_access_key_id='',\
                    aws_secret_access_key='')

content_object = s3.Object(bucket_name='myfirstumasteritbucket', key='UmasterIT/sample.json')
file_content = content_object.get()['Body'].read().decode('utf-8')
json_content = json.loads(file_content)


sc= SparkContext.getOrCreate()

# Convert list to RDD
rdd = sc.parallelize(json_content).map(lambda x :(x['id'],x['type']))

schema = StructType([\
StructField("id", StringType(), True),\
StructField("type", StringType(), True)])


# Create data frame
df = mymongo.createDataFrame(rdd, schema)
df.write.format("mongo").mode("overwrite").save()
df.show()

 
        
