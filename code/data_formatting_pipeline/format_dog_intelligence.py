from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import IntegerType
import os

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

"""
Data collected in csv format reading
"""

df = spark.read.format("csv").option("header", True).load(path+"/../.."+"/data/landing_zone/dog_intelligence.csv")

"""
In order to correctly represent the obey column we must get rid of the percentage string
"""

name = 'obey'

def format_obey_column(x):
    if x == "n/a":
        return 101 # Set to 101 as it is the first integer that cannot be a percentage and thus, it is a unique value that means
                   # for us not a number
    else:
        return int(x.replace("%",""))

udf = UserDefinedFunction(format_obey_column,IntegerType())

formatted_df1 = df.select(*[udf(column).alias(name) if column == name else column for column in df.columns])

name = 'reps_upper'

def format_reps_column(x):
    if x == "n/a":
        return 101 # Set to 101 as it is the first integer that cannot be a percentage and thus, it is a unique value that means
                   # for us not a number
    else:
        return int(x)

udf = UserDefinedFunction(format_reps_column,IntegerType())

formatted_df2 = formatted_df1.select(*[udf(column).alias(name) if column == name else column for column in formatted_df1.columns])

name = 'reps_lower'

udf = UserDefinedFunction(format_reps_column,IntegerType())

formatted_df3 = formatted_df2.select(*[udf(column).alias(name) if column == name else column for column in formatted_df2.columns])


"""
Writing dataframe to existing postgres table with variables set in the format we wish them to be...
"""

jdbc_url = "jdbc:postgresql://localhost:5432/bda_p1"
driver_class = "org.postgresql.Driver"
user = "postgres"
password = "postgres" #pel marcel era hola123
connectionProperties = {"user": user, "password": password}


formatted_df3.write \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("driver", "org.postgresql.Driver") \
  .option("dbtable", "dog_intelligence_formatted") \
  .option("user", connectionProperties["user"]) \
  .option("password", connectionProperties["password"]) \
  .mode("append") \
  .jdbc(jdbc_url,"dog_intelligence_formatted")