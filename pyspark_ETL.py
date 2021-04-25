from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

import pymysql
# import mysql.connector

# create spark session
scSpark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("readingcsv") \
        .config("spark.driver.extraClassPath", "C:/tmp/spark/db_driver/mysql-connector-java-8.0.23.jar") \
        .getOrCreate()

# read csv data
data_file = 'c:/tmp/spark/input/electric-chargepoints-2017.csv'
sdf = scSpark.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(sdf.count()))
sdf.show(10)

# aggregation / transformation
cpid = sdf.groupBy('CPID').count()
print(cpid.show(10))

cpid.write.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/spark',
          driver='com.mysql.jdbc.Driver',
          dbtable='logging',
          user='root',
          password='******').mode('append').save()

# Run the pyspark_ETL.py file with the path  " C:\spark\spark-3.0.1-bin-hadoop2.7\bin> spark-submit --jars external/mysql-connector-java-8.0.23.jar /c:/python/pyspark_ETL.py"

source_df = sqlContext.read.format('jdbc').options(
          url='jdbc:mysql://localhost:3306/spark',
          driver='com.mysql.jdbc.Driver',
          dbtable='logging',
          user='root',
          password='******').load()

# Preview dataframe

source_df.show(8)

