import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

# TODO 需求3: TOP3 省份中 各个省份的平均订单价格(单单价)

# 启动spark
spark = SparkSession.builder.\
    appName("local").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions","4").\
    getOrCreate()
sc = spark.sparkContext

# 读取文件
path = os.path.join("/home/sam/pysparkTestFiles","mini.json")


df = spark.read.format("json").\
    load(path).\
    dropna(thresh=1,subset=["storeProvince"]).\
    filter("storeProvince != 'null'").filter("receivable < 10000").\
    select(["storeProvince", "storeID", "receivable", "dateTS", "payType"])

df.cache()

top3_province_df = df.groupBy("storeProvince").\
    sum("receivable").\
    withColumnRenamed("sum(receivable)","money").\
    orderBy("money",ascending=False).\
    limit(3).\
    select("storeProvince")

top3_province_sales_df = top3_province_df.join(df,"storeProvince","left")

province_avg_unit_price_df = top3_province_sales_df.groupBy("storeProvince").\
    avg("receivable").\
    withColumnRenamed("avg(receivable)","avg_money").\
    withColumn("avg_money",F.round("avg_money",1)).\
    orderBy("avg_money",ascending=False)

province_avg_unit_price_df.show()


df.collect()