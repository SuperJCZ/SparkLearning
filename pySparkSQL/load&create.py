import os

import pandas
import pyspark
from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *

testFilePath = "/home/sam/pysparkTestFiles/sql/"

# TODO 1.pySpark入口

# 1.1 SparkSession

spark = SparkSession.builder.appName("session").\
    master("local[1]").\
    config("spark.sql.shuffle.partitions","2").\
    getOrCreate()


# 1.2 SparkContext

## 1.2.1
# conf = SparkConf().setAppName("context").setMaster("local[4]")
# sc = SparkContext(conf=conf)

## 1.2.2
sc = spark.sparkContext


# TODO 2.创建DataFrame

# 2.1 RDD方式
rdd_read_file = sc.textFile(
    os.path.join(testFilePath,"people.txt")
)

## 2.1.1 自动探测StructType

print("2.1.1 自动探测StructType")
rdd_split = rdd_read_file.map(
    lambda x:x.split(" ")
).map(
    lambda x:[x[0],int(x[1])]
)

df = spark.createDataFrame(rdd_split,schema=["name","age"])
print(df.show())

## 2.1.2 通过StructType定义df表结构

schema = StructType().\
    add("name",StringType(),nullable=False).\
    add("age",IntegerType(),nullable=False)

rdd_split = rdd_read_file.map(
    lambda x:x.split(" ")
).map(
    lambda x:[x[0],x[1]]
)

df = spark.createDataFrame(rdd_split,schema=schema)
print("通过StructType定义df表结构")
df.printSchema()

## 2.1.3 rdd.toDF方法

rdd_split.toDF(schema=schema)

# 2.2 基于pandas.DataFrame

schema = StructType().add("name",StringType()).\
    add("age",IntegerType()).\
    add("type",StringType())

df = pandas.DataFrame(
    {
        "name":["zjc","zyl","zjl"],
        "age":[1,2,3],
        "type":["a","b","c"]
    }
)
df = spark.createDataFrame(df)
print("pandas.DataFrame")
print(df.show())


# 2.3 基于外部文件

## csv, json, txt, parquet
print("基于外部文件")
df = spark.read.format("csv").\
    option("sep",";").\
    load(os.path.join(testFilePath,"people.csv"),
)
print(df.show())


spark.stop()
sc.stop()