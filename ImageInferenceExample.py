from bigdl.nn.layer import Model
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType

from zoo.common.nncontext import *
from zoo.feature.image import *
from zoo.pipeline.nnframes import *


def inference(image_path, model_path, sc):
    imageDF = NNImageReader.readImages(image_path, sc, resizeH=300, resizeW=300, image_codec=1)
    getName = udf(lambda row: row[0], StringType())
    transformer = ChainedPreprocessing(
        [RowToImageFeature(), ImageResize(256, 256), ImageCenterCrop(224, 224),
         ImageChannelNormalize(123.0, 117.0, 104.0), ImageMatToTensor(), ImageFeatureToTensor()])

    model = Model.loadModel(model_path)
    classifier_model = NNClassifierModel(model, transformer)\
        .setFeaturesCol("image").setBatchSize(4)
    predictionDF = classifier_model.transform(imageDF).withColumn("name", getName(col("image")))
    return predictionDF


if __name__ == "__main__":
#    if len(sys.argv) != 3:
#        print("Need parameters: <modelPath> <imagePath>")
#        exit(-1)

    sc = init_nncontext("image_inference")

    model_path = "hdfs:///user/example/dogscats/bigdl_inception-v1_imagenet_0.4.0.model"
    image_path = "hdfs:///project_data/pets/train_images/*"

    predictionDF = inference(image_path, model_path, sc)
    predictionDF.select("name", "prediction").orderBy("name").show(20, False)
