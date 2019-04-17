from pyspark.ml.classification import LogisticRegression
# from zoo.common.nncontext import *
from pyspark.ml import Pipeline
import sparkdl as dl
from sparkdl import DeepImageFeaturizer
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import *

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
from pyspark.ml.classification import DecisionTreeClassificationModel
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.feature import VectorAssembler

conf = SparkConf().setAppName("loadmodeltest").setMaster("yarn")
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 3)
input_topic = 'input1'
output_topic = 'output'
brokers = "gpu17:2181,gpu17-x1:2181,gpu17-x2:2181,student49-x1:2181,student49-x2:2181,student50-x1:2181," \
          "student50-x2:2181,student51-x1:2181,student51-x2:2181"

sql_sc = SQLContext(sc)
df = sql_sc.read.csv('hdfs:///project_data/pets/train/train.csv', header=True, inferSchema='True').drop('Name').drop(
    'State')
input_cols = [a for a, b in df.dtypes if b == 'int']
indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df) for column in ["AdoptionSpeed"]]
pipeline = Pipeline(stages=indexers)

# print('param', str(sys.argv[1]))
# csv_df = sql_sc.read.format("csv").option("header","true").load("hdfs:///project_data/pets/train/train.csv")
kafkaStream = KafkaUtils.createStream(ssc, 'gpu17:2181', 'test-consumer-group', {input_topic: 1})
producer = KafkaProducer(bootstrap_servers='gpu17:9092')
lr_test = LogisticRegressionModel.load('hdfs:///lr')
featurizer_test = dl.DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
p_lr_test = PipelineModel(stages=[featurizer_test, lr_test])
feature = VectorAssembler(inputCols=input_cols, outputCol="features")


def handler(message):
    records = message.collect()
    for record in records:
        print('record', record, type(record))
        print('-----------')
        print('tuple', record[0], record[1], type(record[0]), type(record[1]))
        # producer.send(output_topic, b'message received')
        key = record[0]
        value = record[1]
        if len(key) > 10:
            image_path = value
            image_DF = dl.readImages(image_path)
            image_DF.show()
            tested_lr_test = p_lr_test.transform(image_DF)
            # tested_lr_test.show()
            predict_value = tested_lr_test.select('prediction').head()[0] - 1
            print('predict', predict_value)
            print('byte predict', str(predict_value).encode('utf-8'))
            print('byte key', str(key).encode('utf-8'))
            producer.send(output_topic, key=str(key).encode('utf-8'), value=str(predict_value).encode('utf-8'))
            producer.flush()
            print('predict over')
        elif len(key) == 10:
            print('entered csv model part')
            modelloaded = DecisionTreeClassificationModel.load("hdfs:///treemodelofcsv")
            NewInput = Row('Type', 'Age', 'Breed1', 'Breed2', 'Gender', 'Color1', 'Color2', 'Color3', 'MaturitySize',
                           'FurLength', 'Vaccinated', 'Dewormed', 'Sterilized', 'Health', 'Quantity', 'Fee', 'VideoAmt',
                           'PhotoAmt')
            value_lst = str(value).split(',')
            print('value_lst', value_lst)
            print('lst_len', len(value_lst))
            new_input = NewInput(int(value_lst[0]), int(value_lst[1]), int(value_lst[2]), int(value_lst[3]),
                                 int(value_lst[4]), int(value_lst[5]), int(value_lst[6]), int(value_lst[7]),
                                 int(value_lst[8]), int(value_lst[9]), int(value_lst[10]), int(value_lst[11]),
                                 int(value_lst[12]), int(value_lst[13]), int(value_lst[14]), int(value_lst[15]),
                                 int(value_lst[16]), value_lst[17])
            df_new_input = sql_sc.createDataFrame([new_input])
            df_new_input.show()
            df_new_input = pipeline.fit(df_new_input).transform(df_new_input)
            df_new_input = feature.transform(df_new_input)
            new_predict = modelloaded.transform(df_new_input)
            new_predict.show()
            predict_value = str(new_predict.select('prediction').head()[0])
            print('predict value', predict_value.encode('utf-8'))
            producer.send(output_topic, key=str(key).encode('utf-8'), value=predict_value.encode('utf-8'))
            producer.flush()

kafkaStream.foreachRDD(handler)
# image_path = str(sys.argv[1])
# image_DF = dl.readImages(image_path)
# lines = kafkaStream.map(lambda x: x[1])
# lines.pprint()
# image_DF.printSchema()
# image_DF.show()

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
