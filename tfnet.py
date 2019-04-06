from zoo.common.nncontext import *
sc = init_nncontext("Tfnet Example")
import sys
import tensorflow as tf
#sys.path
#slim_path = "hdfs:///slim/models/research/slim" 
# Please set this to the directory where you clone the tensorflow models repository
#sys.path.append(slim_path)

#from tensorflow.contrib.slim.python.slim.nets import inception
from tensorflow.contrib.slim.python.slim.nets.inception_v1 import inception_v1
from tensorflow.contrib.slim.python.slim.nets.inception_v1 import inception_v1_arg_scope
from tensorflow.contrib.slim.python.slim.nets.inception_v1 import inception_v1_base

slim = tf.contrib.slim
tf.reset_default_graph()
images = tf.placeholder(dtype=tf.float32, shape=(None, 224, 224, 3))

with slim.arg_scope(inception_v1_arg_scope()):
    logits, end_points = inception_v1(images, num_classes=1001)

sess = tf.Session()
saver = tf.train.Saver()
saver.restore(sess, "file:///home/hduser/slim/checkpoint/inception_v1.ckpt")
#saver.restore(sess, "hdfs:///slim/checkpoint/inception_v1.ckpt")
# You need to edit this path to the checkpoint you downloaded

from zoo.util.tf import export_tf
avg_pool = end_points['Mixed_3c']
export_tf(sess, "file:///home/hduser/slim/tfnet/", inputs=[images], outputs=[avg_pool])
from zoo.pipeline.api.net import TFNet
amodel = TFNet.from_export_folder("file:///home/hduser/slim/tfnet/")
from bigdl.nn.layer import Sequential,Transpose,Contiguous,Linear,ReLU, SoftMax, Reshape, View, MulConstant, SpatialAveragePooling
full_model = Sequential()
full_model.add(Transpose([(2,4), (2,3)]))
scalar = 1. /255
full_model.add(MulConstant(scalar))
full_model.add(Contiguous())
full_model.add(amodel)
full_model.add(View([1024]))
full_model.add(Linear(1024,5))
import re
from bigdl.nn.criterion import CrossEntropyCriterion
from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType, StringType
from zoo.common.nncontext import *
from zoo.feature.image import *
from zoo.pipeline.api.keras.layers import Dense, Input, Flatten
from zoo.pipeline.api.keras.models import *
from zoo.pipeline.api.net import *
from zoo.pipeline.nnframes import *
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

(trainingDF, validationDF) = labelDF.randomSplit([0.5, 0.5])
trainingDF.show(10)

transformer = ChainedPreprocessing(
        [RowToImageFeature(), ImageResize(224, 224), ImageCenterCrop(224, 224),
         ImageChannelNormalize(123.0, 117.0, 104.0), ImageMatToTensor(), ImageFeatureToTensor()])
         
from bigdl.optim.optimizer import *
from bigdl.nn.criterion import *
classifier = NNClassifier(full_model, CrossEntropyCriterion(), transformer) \
    .setLearningRate(0.03) \
    .setOptimMethod(Adam()) \
    .setBatchSize(56) \
    .setMaxEpoch(10) \
    .setFeaturesCol("image") \
    .setCachingSample(False) \
    
from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[classifier])
trainedModel = pipeline.fit(trainingDF)
print("fit over")
predictionDF = trainedModel.transform(validationDF).cache()
# caculate the correct rate and the test error
correct = predictionDF.filter("label=prediction").count()
overall = predictionDF.count()
accuracy = correct * 1.0 / overall
print(accuracy)
print("Test Error = %g " % (1.0 - accuracy))
