import pandas as pd
from pyspark.sql import SparkSession, SQLContext
from pyspark import SparkFiles
from pyspark.ml.feature import VectorAssembler
import pyspark
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
import re
import os
import sys
# from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from bigdl.util.common import *
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.ml.classification import DecisionTreeClassificationModel

from bigdl.nn.criterion import CrossEntropyCriterion
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, udf, substring
from pyspark.sql.types import DoubleType, StringType
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

conf = SparkConf().setAppName("pet_adoption").setMaster("yarn")
sc = SparkContext(conf=conf)
# sc = init_nncontext("HowCute_train")
sqlCtx = SQLContext(sc)
df = sqlCtx.read.csv('hdfs:///project_data/pets/train/train.csv', header=True, inferSchema='True').drop('Name').drop(
    'State')
df_test = sqlCtx.read.csv('hdfs:///project_data/pets/train/train.csv', header=True, inferSchema='True').drop(
    'Name').drop('State')
# spark = SparkSession.builder.appName("pet_adoption").getOrCreate()
##pandas frame is easier to read
# df_pd.drop('Name', axis=1, inplace=True)
input_cols = [a for a, b in df.dtypes if b == 'int']
indexers = [StringIndexer(inputCol=column, outputCol=column + "_index").fit(df) for column in ["AdoptionSpeed"]]
pipeline = Pipeline(stages=indexers)
df = pipeline.fit(df).transform(df)
df_test = pipeline.fit(df_test).transform(df_test)

feature = VectorAssembler(inputCols=input_cols, outputCol="features")
feature_vector = feature.transform(df)

feature_vector_test = feature.transform(df_test)
(trainingData, testData) = feature_vector.randomSplit([0.8, 0.2], seed=11)
testData.printSchema()
#testData.show(10)
lr = DecisionTreeClassifier(labelCol="AdoptionSpeed_index", featuresCol="features")
lrModel = lr.fit(trainingData)
lrModel.write().overwrite().save("hdfs:///treemodelofcsv")
modelloaded = DecisionTreeClassificationModel.load("hdfs:///treemodelofcsv")
lr_prediction = modelloaded.transform(testData)
# lr_prediction.select("prediction", "Survived", "features").show()
# evaluator = MulticlassClassificationEvaluator(labelCol="Survived", predictionCol="prediction", metricName="accuracy")
evaluator = MulticlassClassificationEvaluator(labelCol="AdoptionSpeed_index", predictionCol="prediction",
                                              metricName="accuracy")
lr_accuracy = evaluator.evaluate(lr_prediction)
print("Accuracy of DecisionTreeModel is = %g" % (lr_accuracy))
print("Test Error of DecisionTreeModel = %g " % (1.0 - lr_accuracy))
# lr_prediction.show()
lr_prediction = modelloaded.transform(feature_vector_test)
predictions = [int(elem['prediction']) for elem in lr_prediction.select('prediction').collect()]
predictions_ids = [elem['PetID'] for elem in lr_prediction.select('PetID').collect()]
df_new = pd.DataFrame()
df_new['PetID'] = predictions_ids
df_new['AdoptionSpeed'] = predictions

NewInput = Row('Type', 'Age', 'Breed1', 'Breed2', 'Gender', 'Color1', 'Color2', 'Color3', 'MaturitySize', 'FurLength',
               'Vaccinated', 'Dewormed', 'Sterilized', 'Health', 'Quantity', 'Fee', 'VideoAmt', 'PhotoAmt')
new_input = NewInput(1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 100, 1, '2')
df_new_input = sqlCtx.createDataFrame([new_input])
#df_new_input.show()
df_new_input = pipeline.fit(df_new_input).transform(df_new_input)
df_new_input = feature.transform(df_new_input)
new_predict = modelloaded.transform(df_new_input)
#new_predict.show()
predict_value = str(new_predict.select('prediction').head()[0])
print('predict value', predict_value)
