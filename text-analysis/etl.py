from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.types import DoubleType
import pyspark.sql.functions as F


def transData(data):
    return data.rdd.map(lambda r: [Vectors.dense(r[1:-1]),r[-1]]).toDF(['features','label'])


if __name__ == '__main__':
    scSpark = SparkSession \
        .builder \
        .appName("reading csv") \
        .getOrCreate()

cols = ['id', 'loc', 'size', 'rooms', 'bathrooms', 'year', 'price']

# data_file = 'housing_data_*.csv'
data_file = 'housing_test_data.csv'
df = scSpark.read.csv(data_file, header=True, sep=",").cache()
print('Total Records = {}'.format(df.count()))
df = df.drop('index')
df.show(5, True)

loc_file = 'loc_data*.csv'
locDF = scSpark.read.csv(loc_file, header=True, sep=",").cache()
print('Total Records = {}'.format(locDF.count()))
locDF = locDF.drop('index')
locDF.show(5, True)

joinDF = df.join(locDF, on=['loc'], how="inner")#.selectExpr("acc_id", "name", "salary", "dept_id", "phone", "address", "email")
# joinDF = joinDF.drop('index')
joinDF = joinDF.select(cols)
joinDF.show(5)


# hist_file = 'historical_data_*.csv'
hist_file = 'historical_housing_prices.csv'
histDF = scSpark.read.csv(hist_file, header=True, sep=",").cache()
print('Total Records = {}'.format(histDF.count()))
histDF = histDF.drop('index')
histDF = histDF.select(cols)
histDF.show(5, True)

# ijoinDF = histDF.join(locDF, histDF.loc == locDF.loc, how="inner")#.selectExpr("acc_id", "name", "salary", "dept_id", "phone", "address", "email")
# ijoinDF.show(5)

# mergeDF = histDF.join(joinDF, histDF.loc == joinDF.loc, "left_outer") \
#    .select(histDF.loc, joinDF.loc, F.when(joinDF.value.isNull(), histDF.value).otherwise(joinDF.value).alias("value"))
replaceDf = histDF.alias('a').join(joinDF.alias('b'), on=['id'], how='inner').select('a.*').select(cols)
resultDF = histDF.subtract(replaceDf).union(joinDF)
resultDF = resultDF.drop('id')
resultDF.show()


resultDF.write.csv('historical_housing_prices.csv', header="true", mode="overwrite")

# Covert data to dense vector
transformed= transData(resultDF)
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