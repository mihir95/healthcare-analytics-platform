import re
from pyspark.sql.functions import current_timestamp, col, lit

def clean_cols(df):
    return df.toDF(*[
        re.sub(r'_+', '_',  # collapse multiple _
            re.sub(r'[^a-zA-Z0-9]', '_', c.strip().lower())
        ).strip('_')  # remove leading/trailing _
        for c in df.columns
    ])

def load_bronze(spark, input_path, schema_path, checkpoint_path, table_name, source_name):

        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "none")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("cloudFiles.schemaLocation", schema_path)
            .load(input_path)
            .withColumn("source_data", col("_metadata.file_name"))
            .withColumn("source_code", lit(source_name))
            .withColumn("load_timestamp", current_timestamp())
        )

        df = clean_cols(df)

        autoloader_write = df.writeStream \
            .format("delta") \
            .option("checkpointLocation", checkpoint_path) \
            .trigger(availableNow=True) \
            .table(table_name)
        
        return autoloader_write