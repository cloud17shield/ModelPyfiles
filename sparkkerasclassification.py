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
from keras.applications import InceptionV3
from keras.models import Model

#sc = init_nncontext("sparkdl")
conf = SparkConf().setAppName("smallv3").setMaster("yarn")
sc = SparkContext(conf=conf)
v3_full_model = InceptionV3(weights="imagenet")
v3_small_model = Model(inputs=v3_full_model.input, outputs=v3_full_model.layers[3].output)
v3_small_model.save("v3_small_model.h5")
from pyspark.ml.image import ImageSchema
image_df = ImageSchema.readImages("hdfs:///project_data/pets/train_images/")
image_df.printSchema()
image_df.show(10)

import PIL.Image
import numpy as np
from keras.applications.imagenet_utils import preprocess_input
from sparkdl.estimators.keras_image_file_estimator import KerasImageFileEstimator
