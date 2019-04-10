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
import tensorflow as tf
sc = init_nncontext("keras Example")

# from keras.applications import VGG16
# conv_base = VGG16(weights='imagenet',include_top=False, input_shape=(224, 224, 3))
# conv_base.summary()
# model = Sequential()
# model.add(conv_base)
# model.add(Flatten())
# model.add(Dense(256, activation='relu'))
# model.add(Dense(5, activation='softmax'))
# 
# conv_base.trainable = False



model = Sequential()
model.add(ZeroPadding2D((1,1),input_shape=(3,224,224)))
model.add(Convolution2D(64, 3, 3, activation='relu'))
model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(64, 3, 3, activation='relu'))
model.add(MaxPooling2D((2,2), strides=(2,2)))

model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(128, 3, 3, activation='relu'))
model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(128, 3, 3, activation='relu'))
model.add(MaxPooling2D((2,2), strides=(2,2)))

model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(256, 3, 3, activation='relu'))
model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(256, 3, 3, activation='relu'))
model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(256, 3, 3, activation='relu'))
model.add(MaxPooling2D((2,2), strides=(2,2)))

model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(512, 3, 3, activation='relu'))
model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(512, 3, 3, activation='relu'))
model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(512, 3, 3, activation='relu'))
model.add(MaxPooling2D((2,2), strides=(2,2)))

model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(512, 3, 3, activation='relu'))
model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(512, 3, 3, activation='relu'))
model.add(ZeroPadding2D((1,1)))
model.add(Convolution2D(512, 3, 3, activation='relu'))
model.add(MaxPooling2D((2,2), strides=(2,2)))

model.add(Flatten())
model.add(Dense(4096, activation='relu'))
model.add(Dropout(0.5))
model.add(Dense(4096, activation='relu'))
model.add(Dropout(0.5))
model.add(Dense(5, activation='softmax'))

model.get_input_shape() 
model.get_output_shape() 

model.compile(loss='binary_crossentropy',
              optimizer="sgd",
              metrics=['acc'])
model.summary()



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

(trainingDF, validationDF) = labelDF.randomSplit([0.6, 0.4])
trainingDF.show(10)

train_rdd = trainingDF.rdd.map()
dataset = TFDataset.from_rdd(train_rdd,names=["image", "label"],shapes=[[224, 224, 1], [1]],types=[tf.float32, tf.int32],batch_size=56)

optimizer = TFOptimizer.from_keras(keras_model=model, dataset=dataset)
optimizer.set_train_summary(TrainSummary("hdfs:///lr/tmp/vgg", "vgg"))
optimizer.optimize(end_trigger=MaxEpoch(5))

# transformer = ChainedPreprocessing(
#         [RowToImageFeature(), ImageResize(224, 224), ImageCenterCrop(224, 224),
#          ImageChannelNormalize(123.0, 117.0, 104.0), ImageMatToTensor(), ImageFeatureToTensor()])
#          

# classifier = NNClassifier(model, CrossEntropyCriterion(), transformer) \
#     .setLearningRate(0.03) \
#     .setOptimMethod(Adam()) \
#     .setBatchSize(14) \
#     .setMaxEpoch(10) \
#     .setFeaturesCol("image") \
#     .setCachingSample(False) \

#fitmodel = model.fit(transformer(trainingDF), batch_size=14, nb_epoch=10, validation_data=None, distributed=True)

# pipeline = Pipeline(stages=[classifier])
# model = pipeline.fit(trainingDF)
print("fit over")
model.save_weights('hdfs:///lr/keraszootestweights.h5')
print("model weights save success")
model.save('hdfs:///lr/keraszootest.h5')
print("model save success")

predictionDF = model.transform(validationDF).cache()
# caculate the correct rate and the test error
correct = predictionDF.filter("label=prediction").count()
overall = predictionDF.count()
accuracy = correct * 1.0 / overall
print(accuracy)
print("Test Error = %g " % (1.0 - accuracy))
