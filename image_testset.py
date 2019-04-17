from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import sparkdl as dl
from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml import Pipeline, PipelineModel
conf = SparkConf().setAppName("image_testset").setMaster("yarn")
sc = SparkContext(conf=conf)
sql_sc = SQLContext(sc)

lr_test = LogisticRegressionModel.load('hdfs:///lr')
featurizer_test = dl.DeepImageFeaturizer(inputCol="image", outputCol="features", modelName="InceptionV3")
p_lr_test = PipelineModel(stages=[featurizer_test, lr_test])
image_path = "hdfs:///project_data/pets/test_images/"
image_DF = dl.readImages(image_path)
image_DF.show(10)
tested_lr_test = p_lr_test.transform(image_DF)
tested_lr_test.sample(False, 0.1).show()
