from zoo.common.nncontext import *
import re
from bigdl.optim.optimizer import *
from bigdl.nn.criterion import *
from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf, substring
from pyspark.sql.types import DoubleType, StringType
from zoo.common.nncontext import *
from zoo.feature.image import *
from zoo.pipeline.api.keras.layers import *
from zoo.pipeline.api.keras.models import *
from zoo.pipeline.api.net import *
from zoo.pipeline.nnframes import *
sc = init_nncontext("keras Example")

model_path = "hdfs:///user/example/dogscats/bigdl_inception-v1_imagenet_0.4.0.model"
full_model = Net.load_bigdl(model_path)
model = full_model.new_graph(["pool5/drop_7x7_s1"])
model.freeze_up_to(["pool4/3x3_s2"])
inputNode = Input(name="input", shape=(3, 224, 224))
inception = model.to_keras()(inputNode)
flatten = Flatten()(inception)
logits = Dense(5)(flatten)
lrModel = Model(inputNode, logits)



lrModel.compile(loss='binary_crossentropy',
              optimizer="sgd",
              metrics=['acc'])
lrModel.summary()
lrModel.save('hdfs:///lr/zoo_keras_test.h5')
print("model save success")


# image_path = "hdfs:///project_data/pets/train_images/*"
# csv_path = "hdfs:///project_data/pets/train/train.csv"
# sql_sc = SQLContext(sc)
# csv_df = sql_sc.read.format("csv").option("header","true").load(csv_path)
# csv_df.printSchema()
# image_DF = NNImageReader.readImages(image_path, sc).withColumn("id",substring("image.origin",50,9))
# labelDF = image_DF.join(csv_df, image_DF.id == csv_df.PetID, "left").withColumn("label",col("AdoptionSpeed").cast("double")+1).select("image","label")
# 
# labelDF = labelDF.na.drop()
# 
# (trainingDF, validationDF) = labelDF.randomSplit([0.5, 0.5])
# trainingDF.show(10)
# 
# transformer = ChainedPreprocessing(
#         [RowToImageFeature(), ImageResize(224, 224), ImageCenterCrop(224, 224),
#          ImageChannelNormalize(123.0, 117.0, 104.0), ImageMatToTensor(), ImageFeatureToTensor()])
#          
# 
# fitmodel = model.fit(transformer(trainingDF), batch_size=14, nb_epoch=10, validation_data=None, distributed=True)


