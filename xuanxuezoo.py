from zoo.common.nncontext import *
from zoo.feature.common import *
from zoo.feature.image.imagePreprocessing import *
from zoo.pipeline.api.keras.layers import Dense, Input, Flatten
from zoo.pipeline.api.keras.models import *
from zoo.pipeline.api.net import *
from zoo.pipeline.nnframes import *
from bigdl.optim.optimizer import *
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf, substring

sc = init_nncontext("train keras")
img_path="hdfs:///project_data/pets/train_images"
image_set = ImageSet.read(img_path,sc, 12)
#image_set.show()
transformer = ChainedPreprocessing(
        [ImageResize(256, 256), ImageCenterCrop(224, 224),
         ImageChannelNormalize(123.0, 117.0, 104.0), ImageMatToTensor(),
         ImageSetToSample()])
image_data = transformer(image_set)
#print("img rdd count", image_data.get_image().count())
image_DF = NNImageReader.readImages(img_path, sc).withColumn("id",substring("image.origin",50,9)).select("id").toPandas()
print(image_DF.head(5))
sqlCtx = SQLContext(sc)
csv_df = sqlCtx.read.csv('hdfs:///project_data/pets/train/train.csv',header=True,inferSchema='True').toPandas()
print(csv_df.head(5))
lst = []
for i in image_DF["id"]:
    print(i)
    label = csv_df[csv_df.PetID == i]["AdoptionSpeed"]
    if len(label) == 0:
        lst.append(6)
    else:
        lst.append(int(label.iloc[0]))
print(lst[-10:])
print("label len:", len(lst))
labels = np.array(lst)
label_rdd = sc.parallelize(labels, 12)
label_rdd = sc.parallelize(label_rdd.take(4800), 12)
image_rdd = sc.parallelize(image_data.get_image().take(4800), 12)
print("label rdd count", label_rdd.count())
print("image_rdd count", image_rdd.count())
samples = image_rdd.zip(label_rdd).filter(lambda tuple: tuple[1] != 6).map(
        lambda tuple: Sample.from_ndarray(tuple[0], tuple[1]))
#cnt = samples.filter(lambda x: x[1] == 6).count()
#samples = samples.takeOrdered(1200-cnt)
#samples = samples.filter(lambda tuple: tuple[1] != 6)
print("samples cnt:", samples.count())

for i in samples.collect():
        print(i)
print("zip success!")
# create model
model_path="hdfs:///user/example/dogscats/bigdl_inception-v1_imagenet_0.4.0.model"
full_model = Net.load_bigdl(model_path)
# create a new model by remove layers after pool5/drop_7x7_s1
model = full_model.new_graph(["pool5/drop_7x7_s1"])
# freeze layers from input to pool4/3x3_s2 inclusive
model.freeze_up_to(["pool4/3x3_s2"])

inputNode = Input(name="input", shape=(3, 224, 224))
inception = model.to_keras()(inputNode)
flatten = Flatten()(inception)
logits = Dense(5)(flatten)
lrModel = Model(inputNode, logits)
lrModel.summary()
batchsize = 28
nEpochs = 10
lrModel.compile(optimizer=Adam(),
                  loss='mean_squared_error',
                  metrics=['accuracy'])
lrModel.fit(x = samples, batch_size=batchsize, nb_epoch=nEpochs)
lrModel.summary()
lrModel.save('hdfs:///lr/pet_adoption_speed2.h5')
print('save success!')
