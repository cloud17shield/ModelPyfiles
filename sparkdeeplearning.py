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

#sc = init_nncontext("sparkdl")
conf = SparkConf().setAppName("sparkdl").setMaster("yarn")
sc = SparkContext(conf=conf)

image_path = "hdfs:///project_data/pets/train_images/"
csv_path = "hdfs:///project_data/pets/train/train.csv"
sql_sc = SQLContext(sc)
csv_df = sql_sc.read.format("csv").option("header","true").load(csv_path)
csv_df.printSchema()
image_DF = dl.readImages(image_path).withColumn("id",substring("filePath",50,9))
image_DF.printSchema()
image_DF.show(10)
labelDF = image_DF.join(csv_df, image_DF.id == csv_df.PetID, "left").withColumn("label",col("AdoptionSpeed").cast("double")+1).select("image","label")
#labelDF.count()
labelDF = labelDF.na.drop().limit(3000)
#labelDF.count()

(trainingDF, validationDF) = labelDF.randomSplit([0.7, 0.3])
trainingDF.show(10)
print("show over")
vectorizer = DeepImageFeaturizer(inputCol="image", outputCol="features", modelName='InceptionV3')
logreg = LogisticRegression(maxIter=30,regParam=0.05, elasticNetParam=0.3, labelCol = "label", featuresCol="features")
pipeline = Pipeline(stages=[vectorizer, logreg])

pipeline_model = pipeline.fit(trainingDF)
lrModel = pipeline_model
lrModel.stages[1].write().overwrite().save('hdfs:///lr')

from pyspark.ml.classification import LogisticRegressionModel
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

lr_test = LogisticRegressionModel.load('hdfs:///lr')

# Use a featurizer to use trained features from an existing model
featurizer_test = dl.DeepImageFeaturizer(inputCol = "image", outputCol = "features", modelName = "InceptionV3")

# Pipeline both entities
p_lr_test = PipelineModel(stages=[featurizer_test, lr_test])

# Test and evaluate
tested_lr_test = p_lr_test.transform(validationDF)
evaluator_lr_test = MulticlassClassificationEvaluator(metricName = "accuracy")
print("Logistic Regression Model: Test set accuracy = " + str(evaluator_lr_test.evaluate(tested_lr_test.select("prediction", "label"))))

tested_lr_test.select("label", "probability", "prediction").show(10)

