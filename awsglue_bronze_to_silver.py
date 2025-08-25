import sys
from awsglue.transforms import *
#fromlue.utils import getResolvedOptions
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Leer archivo Parquet desde S3 (bronze)
df = spark.read.parquet("s3://pi-m3-bronze/airbyte_landing_zone/mongodb_dataset_henry/")

# Esquemas para columnas JSON
main_schema = StructType([
    StructField("temp", DoubleType()),
    StructField("temp_min", DoubleType()),
    StructField("temp_max", DoubleType()),
    StructField("feels_like", DoubleType()),
    StructField("pressure", IntegerType()),
    StructField("humidity", IntegerType()),
    StructField("dew_point", DoubleType())
])

wind_schema = StructType([
    StructField("speed", DoubleType()),
    StructField("deg", IntegerType())
])

clouds_schema = StructType([
    StructField("all", IntegerType())
])

# Parsear columnas JSON
df_clean = df \
    .withColumn("main_parsed", from_json(col("main"), main_schema)) \
    .withColumn("wind_parsed", from_json(col("wind"), wind_schema)) \
    .withColumn("clouds_parsed", from_json(col("clouds"), clouds_schema))

# Seleccionar y renombrar columnas
df_final = df_clean.select(
    col("dt_iso").alias("timestamp"),
    col("city_name"),
    col("lat"),
    col("lon"),
    col("main_parsed.temp"),
    col("main_parsed.feels_like"),
    col("main_parsed.pressure"),
    col("main_parsed.humidity"),
    col("wind_parsed.speed"),
    col("wind_parsed.deg"),
    col("clouds_parsed.all").alias("cloudiness")
).dropna()

# Guardar como Parquet en S3 (silver)
df_final.write.mode("overwrite").parquet("s3://pi-m3-silver/clima/cleaned/")

job.commit()