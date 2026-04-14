from pyspark.sql.functions import current_timestamp, input_file_name

def load_bronze(spark, input_path, schema_path, checkpoint_path, table_name, source_name):

        df = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "false")
            .option("cloudFiles.schemaEvolutionMode", "none")
            .option("cloudFiles.rescuedDataColumn", "_rescued_data")
            .option("schemaLocation", schema_path)
            .load(input_path)
            .withCooulmn("file_name", input_file_name())
            .withColumn("source_name", source_name)
            .withColumn("load_timestamp", current_timestamp())
        )

        autoloader_write = df.writestream \
            .format("delta") \
            .option("checkpoinLocation", checkpoint_path) \
            .trigger(availableNow=True) \
            .table(table_name)
        
        return autoloader_write