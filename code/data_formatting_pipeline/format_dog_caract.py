from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import IntegerType
import os
import json
import ast

"""
Spark session variables declaration and spark initialization
"""

path = os.getcwd()

conf = SparkConf() \
    .setAppName("PostgreSQL Writing to Formatted Table") \
    .set("spark.jars", path+"/../.."+"/driver/postgresql-42.7.3.jar")

spark = SparkSession.builder \
    .config(conf=conf) \
    .getOrCreate()

sc = spark.sparkContext

"""
Data collected in json format reading
"""

df = sc.wholeTextFiles(path+"/../.."+"/data/landing_zone/dog_caract.json").map(lambda x:ast.literal_eval(x[1]))\
                            .map(lambda x: json.dumps(x))

df = spark.read.json(df)


"""
Writing dataframe to existing postgres table with variables set in the format we wish them to be...
"""

jdbc_url = "jdbc:postgresql://localhost:5432/bda_project1_db"
driver_class = "org.postgresql.Driver"
user = "postgres"
password = "hola123"
connectionProperties = {"user": "postgres", "password": "hola123"}


df.write \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("driver", "org.postgresql.Driver") \
  .option("dbtable", "dog_caract_formatted") \
  .option("user", connectionProperties["user"]) \
  .option("password", connectionProperties["password"]) \
  .mode("append") \
  .jdbc(jdbc_url,"dog_caract_formatted")