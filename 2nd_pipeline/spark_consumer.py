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

# lines = ks.map(lambda x: json.loads(x[1]))
result1 = ks.map(lambda x: json.loads(x[1])).flatMap(lambda x: x['data'])
result1.pprint()

# res = lines.flatMap(lambda x: json.loads(x))
# res.pprint()
# lines.pprint()

def makeIterable(rdd):
    # for x in rdd.collect():
    df = spark.createDataFrame(rdd)
    df.show()
    
        # df.write.mode('overwrite').saveAsTable('kafkaSpark.games')
        # df.write.partitionBy('year', 'month')
result1.foreachRDD(makeIterable)
# lines.pprint()
ssc.start()
ssc.awaitTermination()
