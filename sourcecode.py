# Using the high level transfer learning APIs, you can easily customize pretrained models for feature extraction or fine-tuning. 
# In this notebook, we will use a pre-trained Inception_V1 model. But we will operate on the pre-trained model to freeze first few layers, replace the classifier on the top, then fine tune the whole model. And we use the fine-tuned model to solve the dogs-vs-cats classification problem,
# ## Preparation
# ### 1. Get the dogs-vs-cats datasets
# The following commands copy about 1100 images of cats and dogs into demo/cats and demo/dogs separately. 
# ### 2. Get the pre-trained Inception-V1 model
# Download the pre-trained Inception-V1 model from [Zoo](https://s3-ap-southeast-1.amazonaws.com/bigdl-models/imageclassification/imagenet/bigdl_inception-v1_imagenet_0.4.0.model) 
#  Alternatively, user may also download pre-trained caffe/Tensorflow/keras model.

import re
#from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession 
from bigdl.util.common import *
from pyspark.sql import SQLContext

from bigdl.nn.criterion import CrossEntropyCriterion
from pyspark.ml import Pipeline
from pyspark.sql.functions import col, udf, substring
from pyspark.sql.types import DoubleType, StringType
from zoo.feature.text import TextSet
from zoo.common.nncontext import *
from zoo.feature.image import *
from zoo.pipeline.api.keras.layers import Dense, Input, Flatten
from zoo.pipeline.api.keras.models import *
from zoo.pipeline.api.net import *
from zoo.pipeline.nnframes import *


from pyspark.sql import Row
#conf = SparkConf().setAppName("lucky").setMaster("yarn")
#conf = create_spark_conf()
#sc = SparkContext(conf)
#init_engine()
#spark = SparkSession.builder.master("yarn").appName("Lucky").config(conf=conf).getOrCreate()
sc = init_nncontext("HowCute_train")

# manually set model_path and image_path for training
# 
# 1. model_path = path to the pre-trained models. (E.g. path/to/model/bigdl_inception-v1_imagenet_0.4.0.model)
# 
# 2. image_path = path to the folder of the training images. (E.g. path/to/data/dogs-vs-cats/demo/\*/\*)

model_path = "hdfs:///user/example/dogscats/bigdl_inception-v1_imagenet_0.4.0.model"
image_path = "hdfs:///project_data/pets/train_images/*"
csv_path = "hdfs:///project_data/pets/train/train.csv"
sql_sc = SQLContext(sc)
#pandas_df = pd.read_csv("/home/hduser/pets/train/train.csv")  # assuming the file contains a header
# pandas_df = pd.read_csv('file.csv', names = ['column 1','column 2']) # if no header
csv_df = sql_sc.read.format("csv").option("header","true").load(csv_path)
#csv_df = TextSet.read_parquet(csv_path, sc)
csv_df.printSchema()
image_DF = NNImageReader.readImages(image_path, sc).withColumn("id",substring("image.origin",50,9))
labelDF = image_DF.join(csv_df, image_DF.id == csv_df.PetID, "left").select("image","AdoptionSpeed")
labelDF.count()
labelDF = labelDF.na.drop()
labelDF.count()

#getName = udf(lambda row:
#                  re.search(r'(cat|dog)\.([\d]*)\.jpg', row[0], re.IGNORECASE).group(0),
#                  StringType())
#getLabel = udf(lambda name: 1.0 if name.startswith('cat') else 2.0, DoubleType())

#labelDF = imageDF.withColumn("name", getName(col("image"))).withColumn("label", getLabel(col('name')))
(trainingDF, validationDF) = labelDF.randomSplit([0.1, 0.9])
labelDF.show(10)

# ## Fine-tune a pre-trained model
# We fine-tune a pre-trained model by removing the last few layers, freezing the first few layers, and adding some new layers.

transformer = ChainedPreprocessing(
        [RowToImageFeature(), ImageResize(256, 256), ImageCenterCrop(224, 224),
         ImageChannelNormalize(123.0, 117.0, 104.0), ImageMatToTensor(), ImageFeatureToTensor()])

# ### Load a pre-trained model
# We use the Net API to load a pre-trained model, including models saved by Analytics Zoo, BigDL, Torch, Caffe and Tensorflow. Please refer to [Net API Guide](https://analytics-zoo.github.io/master/#APIGuide/PipelineAPI/net/).

full_model = Net.load_bigdl(model_path)

# ### Remove the last few layers
# Here we print all the model layers and you can choose which layer(s) to remove.
# 
# When a model is loaded using Net, we can use the newGraph(output) api to define a Model with the output specified by the parameter. 

for layer in full_model.layers:
    print (layer.name())
model = full_model.new_graph(["pool5/drop_7x7_s1"])

# The returning model's output layer is "pool5/drop_7x7_s1".
# ### Freeze some layers
# We freeze layers from input to pool4/3x3_s2 inclusive.

model.freeze_up_to(["pool4/3x3_s2"])

# ### Add a few new layers

inputNode = Input(name="input", shape=(3, 224, 224))
inception = model.to_keras()(inputNode)
flatten = Flatten()(inception)
logits = Dense(5)(flatten)
lrModel = Model(inputNode, logits)
classifier = NNClassifier(lrModel, CrossEntropyCriterion(), transformer).setLearningRate(0.003).setBatchSize(56).setMaxEpoch(1).setFeaturesCol("image").setCachingSample(False)
pipeline = Pipeline(stages=[classifier])


# # Train the model
# The transfer learning can finish in a few minutes. 

catdogModel = pipeline.fit(trainingDF)
predictionDF = catdogModel.transform(validationDF).cache()


predictionDF.select("AdoptionSpeed","prediction").sort("AdoptionSpeed", ascending=False).show(10)
predictionDF.select("AdoptionSpeed","prediction").show(10)
correct = predictionDF.filter("AdoptionSpeed=prediction").count()
overall = predictionDF.count()
accuracy = correct * 1.0 / overall
print("Test Error = %g " % (1.0 - accuracy))

# As we can see, the model from transfer learning can achieve over 95% accuracy on the validation set.
# ## Visualize result
# We randomly select some images to show, and print the prediction results here. 
# 
# cat: prediction = 1.0
# dog: prediction = 2.0


#samplecat=predictionDF.filter(predictionDF.prediction==1.0).limit(3).collect()
#sampledog=predictionDF.filter(predictionDF.prediction==2.0).sort("label", ascending=False).limit(3).collect()



#from IPython.display import Image, display
#for cat in samplecat:
#    print ("prediction:"), cat.prediction
#    display(Image(cat.image.origin[5:]))



#for dog in sampledog:
#    print ("prediction:"), dog.prediction
#    display(Image(dog.image.origin[5:]))



