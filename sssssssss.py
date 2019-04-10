from zoo.pipeline.nnframes import *
from zoo.common.nncontext import *
import pandas as pd
from pyspark.sql import SQLContext
from pyspark.sql.functions import col, udf, substring
sc = init_nncontext("read image")
image_DF = NNImageReader.readImages("hdfs:///test_imagedir", sc).withColumn("id",substring("image.origin",33,9)).select("id").toPandas()
print(image_DF)
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
print(lst)
