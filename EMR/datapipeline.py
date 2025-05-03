import pymongo
from bson.decimal128 import Decimal128
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType, IntegerType

from pyspark.sql.functions import  expr
import boto3
import json

secrets_client = boto3.client("secretsmanager", region_name="ap-south-1")

secret_name = "<secret_name>"
response = secrets_client.get_secret_value(SecretId=secret_name)
secret_dict = json.loads(response['SecretString'])

S3_URI = secret_dict["S3_URI"]
MONGO_URI = secret_dict["MONGO_URI"]
DB_NAME = secret_dict["DB_NAME"]
COL_NAME = secret_dict["COL_NAME"]

client = pymongo.MongoClient(MONGO_URI)
DB_NAME = client[DB_NAME]
collection = DB_NAME[COL_NAME]
data = list(collection.find({}, {"_id": 0, "currency": 0, "timezone": 0}))

schema = StructType([
    StructField("datetime", TimestampType(), nullable=False),
    StructField("symbol", StringType(), nullable=False),
    StructField("open", DecimalType(15, 10), nullable=False),
    StructField("high", DecimalType(15, 10), nullable=False),
    StructField("low", DecimalType(15, 10), nullable=False),
    StructField("close", DecimalType(15, 10), nullable=False),
    StructField("volume", IntegerType(), nullable=False)
])

for doc in data:
    for key in ["open", "high", "low", "close"]:
        if isinstance(doc.get(key), Decimal128):
            doc[key] = doc[key].to_decimal()  # Convert Decimal128 to Decimal

spark = SparkSession.builder \
    .appName("ReadFromMongoDBAtlas") \
    .getOrCreate()

df = spark.createDataFrame(data, schema=schema)

df_with_interval = df.withColumn(
    "interval_start",
    expr("date_trunc('hour', datetime) + floor(minute(datetime)/30)*interval 30 minutes")) \
    .withColumn(
        "day_hour", expr("date_trunc('hour', datetime)")
    )

df_with_interval.write.mode("overwrite") \
                .partitionBy("symbol") \
                .parquet(S3_URI)

