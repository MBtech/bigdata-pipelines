from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
import os 
import json

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" \
         --conf "spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true" \
        --conf "spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true" \
        --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
        --conf spark.speculation=false \
        pyspark-shell \
        '


if __name__ == '__main__':
    configs = json.load(open('config.json'))
    scSpark = SparkSession \
        .builder \
        .master(configs["master"]) \
        .config("com.amazonaws.services.s3.enableV4", "true") \
        .config("spark.driver.memory", configs["driver.memory"]) \
        .config("spark.executor.memory", configs["executor.memory"]) \
        .appName("update data") \
        .getOrCreate()
    scSpark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    scSpark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    scSpark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")

    scSpark._jsc.hadoopConfiguration().set("fs.s3a.access.key", configs["fs.s3a.access.key"])
    scSpark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", configs["fs.s3a.secret.key"])
    scSpark.conf.set('spark.executor.memory', configs["executor.memory"])
    scSpark.conf.set('spark.driver.memory', configs["driver.memory"])

cols = ['id', 'loc', 'size', 'rooms', 'bathrooms', 'year', 'price']

data_file = 's3a://'+configs["bucket.name"]+'/enriched/enriched.csv'
df = scSpark.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(df.count()))
df = df.drop('index')
df.show(5, True)

# hist_file = 'historical_data_*.csv'
hist_file = 's3a://'+configs["bucket.name"]+'/base-data/data.csv'
histDF = scSpark.read.csv(hist_file, header=True, sep=",").cache()
print('Total Records = {}'.format(histDF.count()))
histDF = histDF.drop('index')
histDF = histDF.select(cols)
histDF.show(5, True)

replaceDf = histDF.alias('a').join(df.alias('b'), on=['id'], how='inner').select('a.*').select(cols)
resultDF = histDF.subtract(replaceDf).union(df)
resultDF.show()
resultDF.write.csv('s3a://'+configs["bucket.name"]+'/all-data/data.csv', header="true", mode="overwrite")