from pyspark.ml.classification import LogisticRegression
#from zoo.common.nncontext import *
from pyspark.ml import Pipeline
import sparkdl as dl
from sparkdl import DeepImageFeaturizer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.functions import col, udf, substring
from pyspark.sql.types import DoubleType, StringType

from pyspark.sql import Row
import sys
from kafka import KafkaProducer, KafkaConsumer, TopicPartition
from kafka.errors import KafkaError, KafkaTimeoutError

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml import Pipeline, PipelineModel


conf = SparkConf().setAppName("loadmodeltest").setMaster("yarn")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 10)
input_topic = 'input'
output_topic = 'output'
brokers = "gpu17:2181,gpu17-x1:2181,gpu17-x2:2181,student49-x1:2181,student49-x2:2181,student50-x1:2181," \
        "student50-x2:2181,student51-x1:2181,student51-x2:2181"


sql_sc = SQLContext(sc)
#print('param', str(sys.argv[1]))
#csv_df = sql_sc.read.format("csv").option("header","true").load("hdfs:///project_data/pets/train/train.csv")
kafkaStream = KafkaUtils.createStream(ssc, 'gpu17:2181', 'test-consumer-group', {input_topic:1})
producer = KafkaProducer(bootstrap_servers='gpu17:9092')


def handler(message):
    records = message.collect()
    for record in records:
        # print('record', record, type(record))
        # print('-----------')
        # print('tuple', record[0], record[1], type(record[0]), type(record[1]))
        # producer.send(output_topic, b'message received')
        key = record[0]
        value = record[1]
        if len(key) > 10:
            image_path = value
            image_DF = dl.readImages(image_path)
            lr_test = LogisticRegressionModel.load('hdfs:///lr')
            featurizer_test = dl.DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
            p_lr_test = PipelineModel(stages=[featurizer_test, lr_test])
            tested_lr_test = p_lr_test.transform(image_DF)
            predict_value = tested_lr_test.select('prediction').head()[0]
            producer.send(output_topic, key=key, value=str(predict_value).encode('utf-8'))


kafkaStream.foreachRDD(handler)
# image_path = str(sys.argv[1])
# image_DF = dl.readImages(image_path)
# #lines = kafkaStream.map(lambda x: x[1])
# #lines.pprint()
# #image_DF.printSchema()
# #image_DF.show()
#
# from pyspark.ml.classification import LogisticRegressionModel
# from pyspark.ml import Pipeline, PipelineModel
#
# lr_test = LogisticRegressionModel.load('hdfs:///lr')
# featurizer_test = dl.DeepImageFeaturizer(inputCol = "image", outputCol = "features", modelName = "InceptionV3")
#
# p_lr_test = PipelineModel(stages=[featurizer_test, lr_test])
# tested_lr_test = p_lr_test.transform(image_DF)
# predict_value = tested_lr_test.select('prediction').head()[0]
# print("predict:", predict_value, "type:", type(predict_value))
# try:
#     print('start to build kafka connection')
#     producer = KafkaProducer(bootstrap_servers='gpu17:9092')
#     print('start to send msg')
#     producer.send('fun', b'cbdsiceichiw')
#     print('msg sended')
# except KafkaTimeoutError as timeout_error:
#     print('kafka time out')
# except KafkaError:
#     print('other kafka exception')
ssc.start()
ssc.awaitTermination()
