from pyspark.sql import SparkSession
from utils import FileReader

testFilePath = "/home/sam/pysparkTestFiles/sql/"
spark = SparkSession.builder.appName("local").master("local[1]").getOrCreate()

# TODO 3.操作DataFrame

## 3.1 DSL 领域特定语言
## show,select,filter,where,printSchema,groupBy
file = "people.csv"
df = FileReader().read_csv(spark,file,sep=";",header=True)
df.show()

df.printSchema()

df.select("name","age")
# df.select(["name"])

df.filter("name == 'Bob'")
df.filter(df["age"] < 99)

df.groupBy("name").count().show()

## 3.2 SQL
df.createTempView("people") # 临时表，当前sparkSession可用
df.createOrReplaceTempView("people")  # 如存在则替换
df.createGlobalTempView("people")  # 全局表，跨SparkSession可用

temp_query = spark.sql(
    """select * from people"""
)
temp_query.show()


## 3.3 Example
## SQL
sc = spark.sparkContext
rdd_read = sc.textFile("/home/sam/pysparkTestFiles/"+"words.txt")
rdd_split = rdd_read.flatMap(
    lambda x:x.split(" ")
).map(
    lambda x:[x]  # string类型不能做自动推断，但是[str]可以?
)
df = rdd_split.toDF(["word"])
df.createTempView("words")
spark.sql("select word,count(*) from words group by word").show()




