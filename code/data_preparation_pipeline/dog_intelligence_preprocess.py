from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import UserDefinedFunction, col, mean

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
Connection details for table from formatted zone read
"""

jdbc_url = "jdbc:postgresql://localhost:5432/bda_project1_db"
driver_class = "org.postgresql.Driver"
user = "postgres"
password = "hola123"
connectionProperties = {"user": "postgres", "password": "hola123"}


df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/bda_project1_db") \
    .option("dbtable", "dog_intelligence_formatted") \
    .option("user", connectionProperties["user"]) \
    .option("password", connectionProperties["password"]) \
    .option("driver", "org.postgresql.Driver") \
    .load()


try:
    total_missings, missing_cols = 0, []
    filtered_df = df.filter(col("classification") == "n/a")
    class_count = filtered_df.groupBy('classification').count()
    number_of_101 = class_count.first()["count"]
    missing_cols.append("classification")
    total_missings += number_of_101
except TypeError:
    print("No Missing Values found on variable classification")


try:
    filtered_df = df.filter(col("obey") == 101)
    reps_upper_count = filtered_df.groupBy('obey').count()
    number_of_101 = reps_upper_count.first()["count"]
    missing_cols.append("obey")
    total_missings += number_of_101
except TypeError:
    print("No Missing Values found on variable obey")


try:
    filtered_df = df.filter(col("reps_upper") == 101)
    reps_upper_count = filtered_df.groupBy('reps_upper').count()
    number_of_101 = reps_upper_count.first()["count"]
    missing_cols.append("reps_upper")
    total_missings += number_of_101
except TypeError:
    print("No Missing Values found on variable reps_upper")

try:
    filtered_df = df.filter(col("reps_lower") == 101)
    reps_lower_count = filtered_df.groupBy('reps_lower').count()
    number_of_101 = reps_lower_count.first()["count"]
    missing_cols.append("reps_lower")
    total_missings += number_of_101
except TypeError:
    print("No Missing Values found on variable reps_lower")


print("Total Number of Rows before missings removal:", df.count())
missings_percentage = (total_missings/df.count())*100
print("Percentage of Missings over whole dataset:",missings_percentage)
for name, data_type in df.dtypes:
    if missings_percentage > 10: # If more than 10% of data contains missings a missing imputation method is used
        
            if name in missing_cols:
                if data_type == 'string':

                    # Mode extraction
                    grouped_df = df.groupBy(col(name))

                    value_counts = grouped_df.count()

                    sorted_counts = value_counts.orderBy(col('count').desc())

                    # We need to count again the missings from the column to know if the most common value is a missing
                    filtered_df = df.filter(col("classification") == "n/a")
                    class_count = filtered_df.groupBy('classification').count()
                    number_of_missings = class_count.first()["count"]

                    if number_of_missings/df.count() < 0.5: # If the missing value is more than half, the second most common value will be taken as the first will be the missing value
                        most_common_value = sorted_counts.first()[name]
                    else:
                        most_common_value = sorted_counts.rdd.skip(1).take(1)[0][name]

                    udf = UserDefinedFunction(lambda x: most_common_value if x=="n/a" else x, data_type)

                    df = df.select(*[udf(column).alias(name) if column == name else column for column in df.columns])
                
                elif data_type == 'int':
                    # Mean Extraction
                    mean_value = df.select(mean(col(name))).first()[0]

                    udf = UserDefinedFunction(lambda x: mean_value if x==101 else x, data_type)

                    df = df.select(*[udf(column).alias(name) if column == name else column for column in df.columns])
        
    else: # If missings do not take more than 10% of data it is deemed safe to remove them
        if data_type == 'string':
            df = df.filter(col(name) != "n/a")

        elif data_type == 'int':
            df = df.filter(col(name) != 101)

print("Total Number of Rows after missings removal:", df.count())


"""
Outliers Detection and removal
"""

print("Number of rows prior to outliers removal:",df.count())

factor = 1.5
for name, data_type in df.dtypes:

    if data_type == "int":

        quantiles = df.approxQuantile(name, [0.25, 0.75], 0.01)
        q1, q3 = quantiles[0], quantiles[1]
        iqr = q3 - q1

        lower_bound = q1 - factor * iqr
        upper_bound = q3 + factor * iqr

        df = df.filter((col(name) >= lower_bound) & (col(name) <= upper_bound))

print("Number of rows after outliers removal:",df.count())


df.write \
  .format("jdbc") \
  .option("url", jdbc_url) \
  .option("driver", "org.postgresql.Driver") \
  .option("dbtable", "dog_intelligence_trusted") \
  .option("user", connectionProperties["user"]) \
  .option("password", connectionProperties["password"]) \
  .mode("append") \
  .jdbc(jdbc_url,"dog_intelligence_trusted")