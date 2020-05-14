from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
import os 
import json

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'


if __name__ == '__main__':
    secrets = json.load(open('secrets.json'))
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .getOrCreate()
    scSpark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    scSpark._jsc.hadoopConfiguration().set("fs.s3a.access.key", secrets["fs.s3a.access.key"])
    scSpark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secrets["fs.s3a.secret.key"])

cols = ['id', 'loc', 'size', 'rooms', 'bathrooms', 'year', 'price']

# data_file = 'housing_data_*.csv'
data_file = 's3a://housing-prices-data/current-data/housing_data_*.csv'
df = scSpark.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(df.count()))
df = df.drop('index')
df.show(5, True)

loc_file = 's3a://housing-prices-data/loc_data*.csv'
locDF = scSpark.read.csv(loc_file, header=True, sep=",").cache()
print('Total Records = {}'.format(locDF.count()))
locDF = locDF.drop('index')
locDF.show(5, True)

joinDF = df.join(locDF, on=['loc'], how="inner")#.selectExpr("acc_id", "name", "salary", "dept_id", "phone", "address", "email")
# joinDF = joinDF.drop('index')
joinDF = joinDF.select(cols)
joinDF.show(5)
joinDF.write.csv('s3a://housing-prices-data/enriched/enriched.csv', header="true", mode="overwrite")