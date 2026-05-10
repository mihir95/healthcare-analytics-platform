from databricks.sdk.runtime import*
from pyspark.sql.functions import*
from pyspark.sql.functions import*
from pyspark.sql.window import*

# -----------------------------
# Rename columns
# -----------------------------
def rename_columns(df: DataFrame, column_rename_map: dict) -> DataFrame:
    for old, new in column_rename_map.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df

# -----------------------------
# Standardize nulls
# -----------------------------
def standardize_nulls(df: DataFrame, columns: list, null_values: list) -> DataFrame:
    df = df.na.replace(null_values, None, subset=columns)
    return df

# -----------------------------
# Apply casing
# -----------------------------
def apply_case(df: DataFrame, case_map: dict) -> DataFrame:
    for case_type, cols in case_map.items():
        for c in cols:
            if c in df.columns:
                if case_type == "upper":
                    df = df.withColumn(c, upper(col(c)))
                elif case_type == "lower":
                    df = df.withColumn(c, lower(col(c)))
                elif case_type == "proper":
                    df = df.withColumn(c, initcap(col(c)))
    return df

# -----------------------------
# Cast columns
# -----------------------------
def cast_columns(df: DataFrame, schema_map: dict) -> DataFrame:
    for c, dtype in schema_map.items():
        if c in df.columns:
            if dtype == "date":
                df = df.withColumn(
                    c, coalesce(
                        try_to_date(col(c).cast("string"), 'MM/dd/yyyy'),
                        try_to_date(col(c).cast("string"), 'yyyy/MM/dd'),
                        try_to_date(col(c).cast("string"), 'yyyyMMdd')
                    )
                )
            else:
                df = df.withColumn(c, col(c).cast(dtype))
    return df

# -----------------------------
# Hashing business keys
# -----------------------------
def hash_bk(df: DataFrame, bk_col: str, bk: list) -> DataFrame:
    hash_key_df = df.withColumn(
        bk_col,
        md5(concat_ws("|", *[col(c) for c in bk]))
    )
    return hash_key_df

# -----------------------------
# Deduplication
# -----------------------------
def deduplicate(df: DataFrame, key: str, order_col: str) -> DataFrame:
    window_spec = Window.partitionBy(key).orderBy(desc(order_col))
    dedup_df = (
        df
            .withColumn("row_num", row_number().over(window_spec))
            .filter(col("row_num") == 1)
            .drop("row_num")
    )
    return dedup_df

# -----------------------------
# Add audit columns
# -----------------------------
def add_audit_columns(df: DataFrame, source_data: str) -> DataFrame:
    audit_cols_df = (
        df
            .withColumn("source_data", lit(source_data))
            .withColumn("load_timestamp", current_timestamp())
    )
    return audit_cols_df
