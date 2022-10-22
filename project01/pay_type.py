import os

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import time

from pyspark.sql.types import StringType

# TODO 需求4: TOP3 省份中 各个省份的支付类型比例
# 湖南省 支付宝 33%
# 湖南省 现金 36%
# 广东省 微信 33%

# 启动spark
spark = SparkSession.builder.appName("local").master("local[*]").config("spark.sql.shuffle.partitions","4").getOrCreate()
sc = spark.sparkContext

# 读取文件
path = os.path.join("/home/sam/pysparkTestFiles","mini.json")
df = spark.read.format("json").\
    load(path).\
    dropna(thresh=1,subset=["storeProvince"]).\
    filter("storeProvince != 'null'").filter("receivable < 10000").\
    select(["storeProvince", "storeID", "receivable", "dateTS", "payType"])

# 缓存
df.cache()

# top3sales省份
top3_province_df = df.groupBy("storeProvince").\
    sum("receivable").\
    withColumnRenamed("sum(receivable)","money").\
    orderBy("money",ascending=False).\
    limit(3).\
    select("storeProvince")

# top3省份销售记录
top3_province_sales_df = top3_province_df.join(df,"storeProvince","left")
top3_province_sales_df.cache()

# top3省份记录cnt
top3_province_sales_cnt_df = top3_province_sales_df.\
    groupBy("storeProvince").\
    count().\
    withColumnRenamed("count","total")

# top3省份各type记录cnt
top3_province_sales_type_cnt_df = top3_province_sales_df.\
    groupBy("storeProvince","payType").\
    count()

# final
final_df = top3_province_sales_type_cnt_df.\
    join(top3_province_sales_cnt_df,"storeProvince","left").\
    withColumn("percentage",top3_province_sales_type_cnt_df["count"]/top3_province_sales_cnt_df["total"])

# 注册一个udf函数
def to_percentage(percentage):
    return round(percentage*100,2).__str__() + "%"
udf = F.udf(to_percentage, StringType())

# 转换percent格式
final_df = final_df.withColumn("percentage",udf("percentage"))

final_df.show()

df.collect()