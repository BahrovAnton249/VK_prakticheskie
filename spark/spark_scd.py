
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, lit, timestamp_micros,
    row_number
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType,
    StringType, LongType
)

spark = (
    SparkSession.builder
    .appName("CDC_to_SCD2")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    .config("spark.sql.catalog.lakekeeper", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakekeeper.type", "rest")
    .config("spark.sql.catalog.lakekeeper.uri", "http://lakekeeper:8181/catalog")
    .config("spark.sql.catalog.lakekeeper.warehouse", "demo")
    .config("spark.sql.catalog.lakekeeper.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.lakekeeper.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.lakekeeper.s3.path-style-access", "true")
    .config("spark.sql.catalog.lakekeeper.s3.access-key-id", "minioadmin")
    .config("spark.sql.catalog.lakekeeper.s3.secret-access-key", "minioadmin")
    .config("spark.sql.catalog.lakekeeper.s3.region", "us-east-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("id", IntegerType()),
            StructField("owner", StringType()),
            StructField("status", StringType()),
            StructField("updated_at", LongType())
        ])),
        StructField("after", StructType([
            StructField("id", IntegerType()),
            StructField("owner", StringType()),
            StructField("status", StringType()),
            StructField("updated_at", LongType())
        ])),
        StructField("op", StringType())
    ]))
])

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "dbserver1.public.accounts")
    .option("startingOffsets", "earliest")
    .load()
)

parsed = (
    df.select(from_json(col("value").cast("string"), schema).alias("data"))
    .select("data.payload.*")
    .filter(col("after.id").isNotNull())
)

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    changes = (
        batch_df
        .select(
            col("after.id").cast("long").alias("id"),
            col("after.owner").alias("owner"),
            col("after.status").alias("status"),
            timestamp_micros(col("after.updated_at")).alias("valid_from")
        )
    )

    window = Window.partitionBy("id").orderBy(col("valid_from").desc())

    changes = (
        changes
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    changes.createOrReplaceTempView("incoming_changes")

    # Закрываем предыдущую версию, если данные действительно изменились.
    batch_df.sparkSession.sql("""
        MERGE INTO lakekeeper.demo.accounts_history AS target
        USING incoming_changes AS source
        ON target.id = source.id
           AND target.is_current = true
           AND (
               NOT (target.owner <=> source.owner)
               OR NOT (target.status <=> source.status)
           )
        WHEN MATCHED THEN UPDATE SET
            target.valid_to = source.valid_from,
            target.is_current = false
    """)

    # Добавляем новую версию, если её ещё нет.
    batch_df.sparkSession.sql("""
        MERGE INTO lakekeeper.demo.accounts_history AS target
        USING incoming_changes AS source
        ON target.id = source.id
           AND target.is_current = true
        WHEN NOT MATCHED THEN INSERT (
            id, owner, status, valid_from, valid_to, is_current
        ) VALUES (
            source.id,
            source.owner,
            source.status,
            source.valid_from,
            TIMESTAMP '9999-12-31 23:59:59',
            true
        )
    """)

    print(f"Batch {batch_id} processed")

query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "/opt/spark-app/checkpoint_scd2")
    .outputMode("append")
    .start()
)

print("Spark Streaming started")
query.awaitTermination()
