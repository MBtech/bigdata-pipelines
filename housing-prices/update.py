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

data_file = 's3a://housing-prices-data/enriched/enriched.csv'
df = scSpark.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(df.count()))
df = df.drop('index')
df.show(5, True)

# hist_file = 'historical_data_*.csv'
hist_file = 's3a://housing-prices-data/all-data/data.csv'
histDF = scSpark.read.csv(hist_file, header=True, sep=",").cache()
print('Total Records = {}'.format(histDF.count()))
histDF = histDF.drop('index')
histDF = histDF.select(cols)
histDF.show(5, True)

replaceDf = histDF.alias('a').join(df.alias('b'), on=['id'], how='inner').select('a.*').select(cols)
resultDF = histDF.subtract(replaceDf).union(df)
resultDF.show()
resultDF.write.csv('s3a://housing-prices-data/all-data/data.csv', header="true", mode="overwrite")