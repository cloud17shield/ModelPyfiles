import os
import re
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession 
from bigdl.util.common import *
from pyspark.sql import SQLContext

from bigdl.nn.criterion import *
from bigdl.nn.layer import *
from bigdl.optim.optimizer import Adam

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, udf, substring
from pyspark.sql.types import DoubleType, StringType

from zoo.feature.text import TextSet
from zoo.common.nncontext import *
from zoo.feature.image import *
from zoo.pipeline.api.keras.layers import Dense, Input, Flatten
#from zoo.pipeline.api.keras.models import *
#from zoo.pipeline.api.net import *
from zoo.pipeline.nnframes import *
from zoo.models.common.zoo_model import *


from pyspark.sql import Row

sc = init_nncontext("good")


model_path = "hdfs:///user/example/dogscats/bigdl_inception-v1_imagenet_0.4.0.model"
image_path = "hdfs:///project_data/pets/train_images/*"
csv_path = "hdfs:///project_data/pets/train/train.csv"
sql_sc = SQLContext(sc)
csv_df = sql_sc.read.format("csv").option("header","true").load(csv_path)
csv_df.printSchema()
image_DF = NNImageReader.readImages(image_path, sc).withColumn("id",substring("image.origin",50,9))
labelDF = image_DF.join(csv_df, image_DF.id == csv_df.PetID, "left").withColumn("label",col("AdoptionSpeed").cast("double")+1).select("image","label")
#labelDF.count()
labelDF = labelDF.na.drop()
#labelDF.count()

(trainingDF, validationDF) = labelDF.randomSplit([0.64, 0.36])
trainingDF.show(10)

transformer = ChainedPreprocessing(
        [RowToImageFeature(), ImageResize(256, 256), ImageCenterCrop(224, 224),
         ImageChannelNormalize(123.0, 117.0, 104.0), ImageMatToTensor(), ImageFeatureToTensor()])

preTrainedNNModel = NNModel(Model.loadModel(model_path), transformer) \
    .setFeaturesCol("image") \
    .setPredictionCol("embedding")

# create a new model by remove layers after pool5/drop_7x7_s1
#model = preTrainedNNModel.new_graph(["pool5/drop_7x7_s1"])
# freeze layers from input to pool4/3x3_s2 inclusive
#model.freeze_up_to(["pool4/3x3_s2"])

#inputNode = Input(name="input", shape=(3, 224, 224))
#inception = model.to_keras()(inputNode)
#flatten = Flatten()(inception)
#logits = Dense(5)(flatten)

#lrModel = Model(inputNode, logits)

lrModel = Sequential().add(Linear(1000, 5)).add(LogSoftMax())
classifier = NNClassifier(lrModel, ClassNLLCriterion(), SeqToTensor([1000])) \
    .setLearningRate(0.03) \
    .setOptimMethod(Adam()) \
    .setBatchSize(56) \
    .setMaxEpoch(10) \
    .setFeaturesCol("embedding") \
    .setCachingSample(False) \

pipeline = Pipeline(stages=[preTrainedNNModel, classifier])

catdogModel = pipeline.fit(trainingDF)
print("fit over")

#saveModel=lrModel.to_keras()
#saveModel.summary()

#saveModel.save('hdfs:///lr/zootest_lrsave.h5')
catdogModel.summary()
catdogModel.save_model(self, 'hdfs:///lr/zootest_save.h5', weight_path=None, over_write=True)
print("model save success")

predictionDF = catdogModel.transform(validationDF)
predictionDF.sample(False, 0.1).show()

#evaluator = MulticlassClassificationEvaluator(
#    labelCol="label", predictionCol="prediction", metricName="accuracy")
#accuracy = evaluator.evaluate(predictionDF)
# expected error should be less than 10%
#print("Test Error = %g " % (1.0 - accuracy))
correct = predictionDF.filter("label=prediction").count()
overall = predictionDF.count()
accuracy = correct * 1.0 / overall
print("Test Error = %g " % (1.0 - accuracy))




