import boto3
import json

s3 = boto3.resource('s3',
                    aws_access_key_id='AKIAY3CWJHSUTXNREK6Y',\
                    aws_secret_access_key='dI+SpFai6S47INYaGpVi7SOJGVEF2vzos9BNJ0OF')

content_object = s3.Object(bucket_name='myfirstumasteritbucket', key='UmasterIT/sample.json')
file_content = content_object.get()['Body'].read().decode('utf-8')
json_content = json.loads(file_content)
print(json_content)

# mymongo = SparkSession.builder.appName("capstonePlayStore")\
# .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/test.playstore") \
# .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/test.playstore") \
# .getOrCreate()df.write.format("mongo").mode("append").save()