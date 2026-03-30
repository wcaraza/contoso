import json
from pyspark.sql import SparkSession
from reader import read_source
from writer import write_raw, write_datahub
from transformer import transform

spark = SparkSession.builder \
    .appName("MetadataDrivenIngestion") \
    .getOrCreate()

with open("config/sources.json") as f:
    sources = json.load(f)

for source in sources:
    print(f"Processing {source['name']}")

    df = read_source(spark, source)

    write_raw(df, source["name"])

    df_transformed = transform(df)

    write_datahub(df_transformed, source["name"])
