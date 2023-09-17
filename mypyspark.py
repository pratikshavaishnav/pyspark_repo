from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

inputrawpath="s3://inprawbucket/inputdata/world_bank.json"
df=spark.read.format("json").option("mode","DROPMALFORMED").load(inputrawpath)



final=res.select("theme1_Name","theme1_Percent","theme_namecode_code","theme_namecode_name")
final.write.format("csv").option("header","true").save("s3://oplandingbucket/outputdata/snowfile")
