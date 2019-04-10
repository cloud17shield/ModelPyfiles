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

image_path = "hdfs:///test_imagedir/"
image_set = ImageSet.read(image_path,sc, min_partitions=1)
transformer = ChainedPreprocessing(
        [ImageResize(256, 256), ImageCenterCrop(224, 224),
         ImageChannelNormalize(123.0, 117.0, 104.0), ImageMatToTensor(),
         ImageSetToSample()])
image_data = transformer(image_set)
labels = np.array([1,1,1,1,1,1,1,1,1,1,1,1,1])
label_rdd = sc.parallelize(labels, 1)
samples = image_data.get_image().zip(label_rdd).map(
        lambda tuple: Sample.from_ndarray(tuple[0], tuple[1]))

model_path = "hdfs:///user/example/dogscats/bigdl_inception-v1_imagenet_0.4.0.model"
full_model = Net.load_bigdl(model_path)
#model = full_model.new_graph(["pool5/drop_7x7_s1"])
#model.freeze_up_to(["pool4/3x3_s2"])
#inputNode = Input(name="input", shape=(3, 256, 256))
#inception = full_model.to_keras()#(inputNode)
#flatten = Flatten()(inception)
#logits = Dense(2)(flatten)
#lrModel = Model(inception, logits)
lrModel = Sequential()
lrModel.add(Input(name="input", shape=(3, 256, 256)))
lrModel.add(full_model.to_keras())
lrModel.add(Flatten())
lrModel.add(Dense(5, activation='softmax'))


lrModel.compile(loss='binary_crossentropy',
              optimizer="sgd",
              metrics=['acc'])
lrModel.summary()
#lrModel.save('hdfs:///lr/zoo_keras_test.h5')
print("model save success")
lrModel.fit(x = samples, batch_size=1, nb_epoch=2)
lrModel.summary()
lrModel.save('hdfs:///lr/zoo_keras_test_trained.h5')

