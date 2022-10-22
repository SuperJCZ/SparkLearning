import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.\
    appName("local").\
    master("local[*]").\
    config("spark.sql.shuffle.partitions","4").\
    getOrCreate()

sc = spark.sparkContext

path = os.path.join("/home/sam/pysparkTestFiles","mini.json")

# dropna(how:str,thresh:int,subset:str/list/tuple)
## how : any, all
## thresh: default None. If specified, drop rows that have less than thresh non-null values. This overwrites the how parameter.
## subset: optional list of column names to consider.
df = spark.read.format("json").load(path).\
    dropna(thresh=1,subset=["storeProvince"]).\
    filter("storeProvince != 'null'").filter("receivable < 10000").\
    select(["storeProvince", "storeID", "receivable", "dateTS", "payType"])

# TODO 1.销售额统计

# SQL
df.createOrReplaceTempView("sales")
query = spark.sql(
    """
    select
    storeProvince,ROUND(sum(receivable),2) as money
    from sales
    group by storeProvince
    order by money desc,storeProvince
    """
)
query.show(truncate=False)  # truncate=False: tell the output sink to display the full column.

## DSL
query = df.groupBy("storeProvince").\
    sum("receivable").\
    withColumnRenamed("sum(receivable)","money").\
    withColumn("money",F.round("money",2)).\
    orderBy(["money","storeProvince"],ascending=False)
query.show()


spark.stop()