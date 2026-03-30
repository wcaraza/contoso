from pyspark.sql import SparkSession

def read_source(spark, source):
    if source["type"] == "file":
        return spark.read.format(source["format"]).load(source["path"])
    else:
        raise ValueError(f"Unsupported source type: {source['type']}")
