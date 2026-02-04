import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql.functions import col, coalesce, lower, when, lit, current_timestamp

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'INGESTION_DATE',
    'RAW_DB',
    'RAW_TABLE',
    'PROCESSED_BUCKET',
    'PROCESSED_DB',
    'PROCESSED_TABLE'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


spark.conf.set("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.iceberg_catalog.warehouse", args['PROCESSED_BUCKET'])
spark.conf.set("spark.sql.catalog.iceberg_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")

raw_df = spark.read.table(f"{args['RAW_DB']}.{args['RAW_TABLE']}").filter(col("ingestion_date")==args['INGESTION_DATE'])
    
print("raw_df")
raw_df.printSchema()


cleaned_df = (
    raw_df
    .withColumn("ad_cost", coalesce(col("spend"), col("cost")))
    .withColumn("ad_cost", col("ad_cost").cast("decimal(10,2)"))
    .withColumn("revenue", col("revenue").cast("decimal(10,2)"))

    .withColumn("device", lower(col("device")))
    .withColumn("country_code",
                when(col("country_code").isin(["USA", "US"]), "US")
                .when(col("country_code").isin(["UK", "GB"]), "GB")
                .when(col("country_code").isin(["CAN", "CA"]), "CA")
                .otherwise(col("country_code")))
    .withColumn("campaign_name",
                when(col("campaign_name").isNull(), lit("Unknown"))
                .otherwise(col("campaign_name")))
)

enriched_df = (
    cleaned_df
    .withColumn("ctr", 
                when(col("impressions")> 0, (col("clicks") / col("impressions") * 100))
                .otherwise(lit(0))
                .cast("decimal(10,4)"))
    .withColumn("cpc",
                when(col("clicks") > 0, col("ad_cost") / col("clicks"))
                .otherwise(lit(None))
                .cast("decimal(10,2)"))
    .withColumn("cpa",
                when(col("conversions") > 0, col("ad_cost") / col("conversions"))
                .otherwise(lit(None))
                .cast("decimal(10,2)"))
    .withColumn("roas",
                when(col("ad_cost") > 0, col("revenue") / col("ad_cost"))
                .otherwise(lit(0))
                .cast("decimal(10,2)"))
    .withColumn("conversion_rate",
                when(col("clicks") > 0, (col("conversions") / col("clicks") * 100))
                .otherwise(lit(0))
                .cast("decimal(10,4)"))

    .withColumn("is_reconciled", lit(False))
    .withColumn("reconciliation_count", lit(0))
    .withColumn("processed_at", current_timestamp())
)


final_df = enriched_df.select(
    col("campaign_id"),
    col("ad_platform"),
    col("event_date"),
    col("campaign_name"),
    col("device"),
    col("country_code"),
    col("ad_cost"),
    col("impressions"),
    col("clicks"),
    col("conversions"),
    col("revenue"),
    col("ctr").cast("decimal(10,4)"),
    col("cpc").cast("decimal(10,2)"),
    col("cpa").cast("decimal(10,2)"),
    col("roas").cast("decimal(10,2)"),
    col("conversion_rate").cast("decimal(10,4)"),
    col("is_reconciled"),
    col("reconciliation_count"),
    col("processed_at"),
    col("created_at")
)

print("final_df")
final_df.printSchema()

today_df = final_df.filter(
    col("event_date") == (args["INGESTION_DATE"])
)

reconcile_df = final_df.filter(
    col("event_date") < (args["INGESTION_DATE"])
)

reconcile_df.createOrReplaceTempView("reconcile_df")


# ---- ICEBERG table creation ----

spark.sql(f"""
CREATE TABLE IF NOT EXISTS iceberg_catalog.{args['PROCESSED_DB']}.{args['PROCESSED_TABLE']} (
    campaign_id INT,
    ad_platform STRING,
    event_date DATE,
    campaign_name STRING,
    device STRING,
    country_code STRING,
    ad_cost DECIMAL(10,2),
    impressions BIGINT,
    clicks BIGINT,
    conversions BIGINT,
    revenue DECIMAL(10,2),
    ctr DECIMAL(10,4),
    cpc DECIMAL(10,2),
    cpa DECIMAL(10,2),
    roas DECIMAL(10,2),
    conversion_rate DECIMAL(10,4),
    is_reconciled BOOLEAN,
    reconciliation_count INT,
    processed_at TIMESTAMP,
    created_at DATE
)
USING iceberg 
PARTITIONED BY (days(event_date))
LOCATION '{args['PROCESSED_BUCKET']}/{args['PROCESSED_TABLE']}/'
TBLPROPERTIES (
    'format-version'='2',
    'write.format.default'='parquet',
    'write.parquet.compression-codec'='snappy'
)
""")


# ---- ICBERG table add new data ----
today_df.writeTo(
    f"iceberg_catalog.{args['PROCESSED_DB']}.{args['PROCESSED_TABLE']}"
).overwritePartitions()

# ---- ICEBERG table update old data ----

spark.sql(f"""
MERGE INTO iceberg_catalog.{args['PROCESSED_DB']}.{args['PROCESSED_TABLE']} t
USING reconcile_df s
ON t.campaign_id = s.campaign_id
   AND t.ad_platform = s.ad_platform
   AND t.event_date = s.event_date
   AND t.event_date >= DATE('{args['INGESTION_DATE']}') - INTERVAL '3' DAY
   AND t.event_date < DATE('{args['INGESTION_DATE']}')

WHEN MATCHED 
AND (
       NOT(t.ad_cost <=> s.ad_cost)
    OR NOT(t.impressions <=> s.impressions)
    OR NOT(t.clicks <=> s.clicks)
    OR NOT(t.conversions <=> s.conversions)
    OR NOT(t.revenue <=> s.revenue)
)
THEN UPDATE SET 
    ad_cost = s.ad_cost,
    impressions = s.impressions,
    clicks = s.clicks,
    conversions = s.conversions,
    revenue = s.revenue,
    ctr = s.ctr,
    cpc = s.cpc,
    cpa = s.cpa,
    roas = s.roas,
    is_reconciled = TRUE,
    conversion_rate = s.conversion_rate,
    reconciliation_count = t.reconciliation_count + 1,
    processed_at = s.processed_at
          
WHEN NOT MATCHED THEN INSERT (
    campaign_id,
    ad_platform,
    event_date,
    campaign_name,
    device,
    country_code,
    ad_cost,
    impressions,
    clicks,
    conversions,
    revenue,
    ctr,
    cpc,
    cpa,
    roas,
    conversion_rate,
    is_reconciled,
    reconciliation_count,
    processed_at,
    created_at
)
VALUES (
    s.campaign_id,
    s.ad_platform,
    s.event_date,
    s.campaign_name,
    s.device,
    s.country_code,
    s.ad_cost,
    s.impressions,
    s.clicks,
    s.conversions,
    s.revenue,
    s.ctr,
    s.cpc,
    s.cpa,
    s.roas,
    s.conversion_rate,
    s.is_reconciled,
    s.reconciliation_count,
    s.processed_at,
    s.created_at
)
""")


job.commit()