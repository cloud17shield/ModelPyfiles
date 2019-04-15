from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from kafka import KafkaProducer, TopicPartition, SimpleProducer, KafkaClient
from kafka.errors import KafkaError, KafkaTimeoutError

conf = SparkConf().setAppName("sendmsg").setMaster("yarn")
sc = SparkContext(conf=conf)
ssc=StreamingContext(sc,10)

topic='fun'
brokers="gpu17:2181,gpu17-x1:2181,gpu17-x2:2181,student49-x1:2181,student49-x2:2181,student50-x1:2181,student50-x2:2181,student51-x1:2181,student51-x2:2181"

kafkaStream = KafkaUtils.createStream(ssc,'gpu17:2181','test-consumer-group', {topic:1})
#kafkaStream = KafkaUtils.createDirectStream(ssc,[topic],kafkaParams={"metadata.broker.list": brokers})
lines = kafkaStream.map(lambda x: x[1])
lines.pprint()
try:
    print('start to build kafka connection')
    producer = KafkaProducer(bootstrap_servers='gpu17:9092')
    print('start to send msg')
    producer.send('fun', b'sendmsgtestproducer')
    producer.flush()
    print('msg sended')
except KafkaTimeoutError as timeout_error:
    print('kafka time out')
except KafkaError:
    print('other kafka exception')
ssc.start()
ssc.awaitTermination()

