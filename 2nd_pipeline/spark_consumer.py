from kafka import KafkaConsumer
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

spark = SparkSession.builder\
    .appName('NBAGAMES')\
        .config('spark.sql.warehouse.dir','/user/hive/warehouse')\
        .config('hive.metastore.uris','thrift://localhost:9083')\
        .enableHiveSupport()\
        .getOrCreate()
sc = SparkContext.getOrCreate()

ssc = StreamingContext(sc, 2)
ks = KafkaUtils.createDirectStream(ssc, topics=['kafkaNba'],kafkaParams={"bootstrap.servers": "localhost:9099", "auto.offset.reset": "smallest"})

lines = ks.map(lambda x: json.loads(x[1]))

def makeIterable(rdd):
    for x in rdd.collect()
    df = spark.createDataframe(x)
    df.show()
    df.write.mode('overwrite').saveAsTable('kafkaSpark.games')
    df.write.partitionBy('year', 'month')
lines.foreachRDD(makeIterable)
# for message in ks:
#     print(message.value)