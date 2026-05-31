import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME", "input_path", "output_path", "warehouse_path"])

spark = (
    SparkSession.builder
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.glue_iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue_iceberg.warehouse", args["warehouse_path"])
    .config("spark.sql.catalog.glue_iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .getOrCreate()
)

glueContext = GlueContext(spark)
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

df = spark.read.option("recursiveFileLookup", "true").json(args["input_path"])
df.write.mode("overwrite").parquet(args["output_path"])

database_name = "glue_iceberg.my_iceberg_db"
table_name = f"{database_name}.my_iceberg_table"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
spark.sql(f"""
    CREATE TABLE {table_name}
    USING iceberg
    AS SELECT * FROM parquet.`{args["output_path"]}`
""")

job.commit()
