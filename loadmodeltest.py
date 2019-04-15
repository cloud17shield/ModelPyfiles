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

conf = SparkConf().setAppName("loadmodeltest").setMaster("yarn")
sc = SparkContext(conf=conf)

sql_sc = SQLContext(sc)
#print('param', str(sys.argv[1]))
#csv_df = sql_sc.read.format("csv").option("header","true").load("hdfs:///project_data/pets/train/train.csv")
image_path = str(sys.argv[1])
image_DF = dl.readImages(image_path)
#image_DF.printSchema()
#image_DF.show()

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml import Pipeline, PipelineModel

lr_test = LogisticRegressionModel.load('hdfs:///lr')
featurizer_test = dl.DeepImageFeaturizer(inputCol = "image", outputCol = "features", modelName = "InceptionV3")

p_lr_test = PipelineModel(stages=[featurizer_test, lr_test])
tested_lr_test = p_lr_test.transform(image_DF)
predict_value = tested_lr_test.select('prediction').head()[0]
print("predict:", predict_value, "type:", type(predict_value))
try:
    print('start to build kafka connection')
    producer = KafkaProducer(bootstrap_servers='gpu17:9092')
    print('start to send msg')
    producer.send('fun', b'cbdsiceichiw')
    print('msg sended')
except KafkaTimeoutError as timeout_error:
    print('kafka time out')
except KafkaError:
    print('other kafka exception')
