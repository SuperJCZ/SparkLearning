import os
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
import pyspark.sql.functions as F

spark = SparkSession.builder.\
    appName("local").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions","4").\
    getOrCreate()

sc = spark.sparkContext

path = os.path.join("/home/sam/pysparkTestFiles","mini.json")

df = spark.read.format("json").load(path).\
    dropna(thresh=1,subset=["storeProvince"]).\
    filter("storeProvince != 'null'").filter("receivable < 10000").\
    select(["storeProvince", "storeID", "receivable", "dateTS", "payType"])

# TODO 2. TOP3 销售省份中, 有多少家店铺 日均销售额 1000+

df.createOrReplaceTempView("sales")

sales_df = df.groupBy("storeProvince").\
    sum("receivable").\
    withColumnRenamed("sum(receivable)","money").\
    withColumn("money",F.round("money",2)).\
    orderBy(["money","storeProvince"],ascending=False)

top3_province_df = sales_df.select("storeProvince").limit(3)
top3_province_sales_df = top3_province_df.join(df,"storeProvince","left")

top3_province_sales_df.persist(StorageLevel.MEMORY_AND_DISK)

top3_province_sales_df.\
    groupBy(
    "storeProvince",
    "storeID",
    F.from_unixtime(top3_province_sales_df["dateTS"].substr(0,10),"yyyy-MM-dd").alias("day")
    ).\
    sum("receivable").\
    withColumnRenamed("sum(receivable)", "money").\
    filter("money > 1000").\
    dropDuplicates(subset=["storeID"]).\
    groupBy("storeProvince").\
    count().\
    show()


spark.stop()