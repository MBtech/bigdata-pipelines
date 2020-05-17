from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F
import os 
import json

def transData(data):
    return data.rdd.map(lambda r: [Vectors.dense(r[1:-1]),r[-1]]).toDF(['features','label'])


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
        .appName("training and eval") \
        .getOrCreate()
    scSpark._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    scSpark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    scSpark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")

    scSpark._jsc.hadoopConfiguration().set("fs.s3a.access.key", configs["fs.s3a.access.key"])
    scSpark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", configs["fs.s3a.secret.key"])
    scSpark.conf.set('spark.executor.memory', configs["executor.memory"])
    scSpark.conf.set('spark.driver.memory', configs["driver.memory"])

data_file = 's3a://'+configs["bucket.name"]+'/all-data/data.csv'
df = scSpark.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(df.count()))
df = df.drop('index')
df.show(5, True)

df = df.drop('id')

# Covert data to dense vector
transformed= transData(df)
transformed.show(5)

# Cast label column to double otherwise it is considered string
transformed = transformed.withColumn("label", transformed.label.cast(DoubleType()))

# Split the data into training and test sets (40% held out for testing)
(trainingData, testData) = transformed.randomSplit([0.6, 0.4])
trainingData.show(5)
testData.show(5)

rf = RandomForestRegressor() # featuresCol="indexedFeatures",numTrees=2, maxDepth=2, seed=42
# Chain indexer and tree in a Pipeline
pipeline = Pipeline(stages=[rf])
model = pipeline.fit(trainingData)


predictions = model.transform(testData)
# Select example rows to display.
predictions.select("features", "label", "prediction").show(5)


# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="label", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
