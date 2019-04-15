from pyspark.ml.classification import DecisionTreeClassifier,DecisionTreeClassificationModel
#from zoo.common.nncontext import *
from pyspark.ml import Pipeline,PipelineModel
import sparkdl as dl
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
from pyspark.sql import SQLContext
import pandas as pd
import json

from pyspark.sql.functions import col, udf, substring
from pyspark.sql.types import DoubleType, StringType

from pyspark.sql import Row
import sys
from pyspark.ml.feature import VectorAssembler
conf = SparkConf().setAppName("loadmodeltest").setMaster("yarn")
sc = SparkContext(conf=conf)

sql_sc = SQLContext(sc)

data = [(2,3,299,0,1,1,7,0,1,1,2,2,2,1,1,100,41326,0,1,2)]
#print('param', str(sys.argv[1]))
#csv_df = sql_sc.read.format("csv").option("header","true").load("hdfs:///project_data/pets/train/train.csv")
#image_path = str(sys.argv[1])
#mage_DF = dl.readImages(image_path)
#image_DF.printSchema()
#image_DF.show()

#df=pd.DataFrame(data=datainput)
header1 = ['Type','Breed1','Breed2','Gender','Color1','Color2','Color3','MaturitySize','FurLength','Vaccinated','Dewormed','Sterilized','Health','Quantity','Fee','State','VideoAmt','PhotoAmt']
#feature = VectorAssembler(inputCols=input_cols,outputCol="features")
#feature_vector= feature.transform(df)
df1 = spark.createDataFrame(data,header1)
df1.show(n=2)
df1.first()
df1.count()
df1.printSchema()
df11.show()
df3 = VectorAssembler(inputCols=['Type','Breed1','Breed2','Gender','Color1','Color2','Color3','MaturitySize','FurLength','Vaccinated','Dewormed','Sterilized','Health','Quantity','Fee','State','VideoAmt','PhotoAmt'],outputCol='Features').transform(df3)
df3.show()
##lr_test = DecisionTreeClassificationModel.load("treemodelofcsv")

#p_lr_test = PipelineModel()
#tested_lr_test = p_lr_test.transform(image_DF)
#predict_value = tested_lr_test.select('prediction').head()[0]


