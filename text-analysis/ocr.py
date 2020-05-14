import pytesseract
from PIL import Image
import pyspark
import json
import os 
import boto3
from io import BytesIO

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages "org.apache.hadoop:hadoop-aws:2.7.3" pyspark-shell'
secrets = json.load(open('secrets.json'))

sc = pyspark.SparkContext()
sc._jsc.hadoopConfiguration().set("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", secrets["fs.s3a.access.key"])
sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secrets["fs.s3a.secret.key"])

path='s3a://ocr-pipeline-data/img*'

s3=boto3.client('s3')
l=s3.list_objects(Bucket='ocr-pipeline-data')['Contents']
paths = list()
for key in l:
    paths.append(key['Key'])

rdd = sc.binaryFiles(path)
# rdd.take(1)
# rdd.foreach(print)



# rdd = sc.parallelize(paths)

# rdd.collect()
# rdd.foreach(print)

# # input=rdd.keys()#.map(lambda s: s.replace("file:",""))

def read(x,y):
    import pytesseract
    import boto3
    # s3=boto3.client('s3')
    # s3.download_file('ocr-pipeline-data', x, x)
    image = Image.open(BytesIO(y))

    # image=Image.open(x)
    text=pytesseract.image_to_string(image)

    print(x, text)
    return text

# # for p in paths:
# #     read(p)

newRdd= rdd.map(lambda x : read(x[0],x[1]))
newRdd.collect()
newRdd.foreach(print)