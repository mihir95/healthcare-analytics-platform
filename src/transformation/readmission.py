from databricks.sdk.runtime import*
from pyspark.sql.functions import*
from pyspark.sql.window import*
from pyspark.sql.types import*
from src.transformation.common import*

COLUMNS = [
    "facility_name"
    ,"facility_id"
    ,"state"
    ,"measure_name"
    ,"number_of_discharges"
    ,"footnote"
    ,"excess_readmission_ratio"
    ,"predicted_readmission_rate"
    ,"expected_readmission_rate"
    ,"number_of_readmissions"
    ,"start_date"
    ,"end_date"
    ,"source_data"
    ,"source_code"
    ,"load_timestamp"
]

NULL_VALUES = [
    "NA"
    ,"N/A"
    ,""
    ,"Too Few to Report"
]

COLUMN_RENAME_MAP = {
    "state": "state_code"
}

SCHEMA_MAP = {
    "start_date": "date"
    ,"end_date": "date"
    ,"number_of_discharges": "int"
    ,"number_of_readmissions": "int"
    ,"performance_year": "int"
    ,"excess_readmission_ratio": "float"
    ,"predicted_readmission_rate": "float"
    ,"expected_readmission_rate": "float"
    ,"is_excess_readmission": "boolean"
}

BUSINESS_KEYS = [
    "facility_id"
    ,"condition_code"
    ,"start_date"
    ,"end_date"
]

# CASE_CONFIG = {
#     "upper": ["facility_id", "state_code", "measure_name", "condition_code"],
#     "lower": [],
#     "proper": ["facility_name", "condition_name"]
# }

def standardize_condition_code(df):
    df = df.withColumn("condition_code",
        regexp_extract(col("measure_name"), r"READM-\d+-(.*)-HRRP", 1)     
    )
    return df

def standardize_condition_name(spark, df):
    condition_mapping = {
        "AMI": "Acute Myocardial Infarction",
        "COPD": "Chronic Obstructive Pulmonary Disease",
        "HF": "Heart Failure",
        "PN": "Pneumonia",
        "CABG": "Coronary Artery Bypass Graft Surgery",
        "THA/TKA": "Total Hip and/or Total Knee Arthroplasty",
        "HIP-KNEE": "Total Hip and/or Total Knee Arthroplasty"
    }
    condition_mapping_df = spark.createDataFrame(
        [(k, v) for k, v in condition_mapping.items()],
        ["condition_code", "condition_name"]
    )
    df = df.join(broadcast(condition_mapping_df),
            on="condition_code",
            how="left"
    )
    return df

def performance_year(df):
    df = df.withColumn("performance_year", year(to_date(col("end_date"), 'MM/dd/yyyy')))
    return df

def is_excess_readmission(df):
    df = df.withColumn("is_excess_readmission", 
                                        when(col("excess_readmission_ratio").cast('float') > 1, True)
                                        .when(col("excess_readmission_ratio").cast('float') < 1, False)
                                        .otherwise(None)
                                    )
    return df

def transformation_readmission(spark, df) -> DataFrame:
    df = df.select(*COLUMNS)
    df = standardize_nulls(df, COLUMNS, NULL_VALUES)
    df = standardize_condition_code(df)
    df = standardize_condition_name(spark, df)
    df = performance_year(df)
    df = is_excess_readmission(df)
    df = rename_columns(df, COLUMN_RENAME_MAP)
    df = cast_columns(df, SCHEMA_MAP)
    df = hash_bk(df, "readmission_bk_hash", BUSINESS_KEYS)
    df = deduplicate(df, "readmission_bk_hash", "load_timestamp")
    df = add_audit_columns(df, "bronze.readmission")
    return df
