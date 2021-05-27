import boto3
import json

s3 = boto3.resource('s3')
content_object = s3.Object('myfirstumasteritbucket', 's3://myfirstumasteritbucket/UmasterIT/')
file_content = content_object.get()['Body'].read().decode('utf-8')
json_content = json.loads(file_content)
print(json_content['Details'])