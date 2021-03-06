from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import json

spark = SparkSession.builder\
    .appName('NBAGAMES')\
        .config('spark.sql.warehouse.dir','/user/hive/warehouse')\
        .config('hive.metastore.uris','thrift://localhost:9083')\
        .enableHiveSupport()\
        .getOrCreate()
sc = SparkContext.getOrCreate()

ssc = StreamingContext(sc, 10)
ks = KafkaUtils.createDirectStream(ssc, topics=['kafkaNBA'],kafkaParams={"bootstrap.servers": "localhost:9099", "auto.offset.reset": "smallest"})


result1 = ks.map(lambda x: json.loads(x[1])).flatMap(lambda x: x['data']).map(lambda x: x ['visitor_team'])
result1.pprint()



def makeIterable(rdd):
    if not rdd.isEmpty():
        df = spark.createDataFrame(rdd, schema=['visitor_team','city', 'conference', 'division', 'full_name', 'id', 'name' ])
        df.show()
        df.write.saveAsTable(name ='nba.nbagames', format='hive', mode='append')
        
        
       
result1.foreachRDD(makeIterable)

ssc.start()
ssc.awaitTermination()
