from databricks.sdk.runtime import*
from pyspark.sql.functions import*
from pyspark.sql.window import*
from pyspark.sql.types import*
from src.transformation.common import*

COLUMNS = [
    "year"
    ,"bene_geo_lvl"
    ,"bene_geo_desc"
    ,"bene_geo_cd"
    ,"bene_age_lvl"
    ,"benes_total_cnt"
    ,"benes_ffs_cnt"
    ,"bene_dual_pct"
    ,"bene_feml_pct"
    ,"bene_male_pct"
    ,"bene_race_wht_pct"
    ,"bene_race_black_pct"
    ,"bene_race_hspnc_pct"
    ,"bene_race_othr_pct"
    ,"acute_hosp_readmsn_cnt"
    ,"acute_hosp_readmsn_pct"
    ,"tot_mdcr_pymt_pc"
    ,'tot_mdcr_stdzd_pymt_pc'
    ,"er_visits_per_1000_benes"
    ,"source_data"
    ,"source_code"
    ,"load_timestamp"
]

NULL_VALUES = [
    "NA"
    ,"N/A"
    ,"NULL"
    ,"null"
    ,""
    ," "
    ,"*"
]

COLUMN_RENAME_MAP = {
    "year": "performance_year"
    ,"bene_geo_lvl": "geography_level"
    ,"bene_geo_desc": "geography_name"
    ,"bene_geo_cd": "geography_code"
    ,"bene_age_lvl": "beneficiary_age_level"
    ,"benes_total_cnt": "total_beneficiaries"
    ,"benes_ffs_cnt": "ffs_beneficiaries_count"
    ,"bene_dual_pct": "dual_eligible_pct"
    ,"bene_feml_pct": "female_beneficiary_pct"
    ,"bene_male_pct": "male_beneficiary_pct"
    ,"bene_race_wht_pct": "white_beneficiary_pct"
    ,"bene_race_black_pct": "black_beneficiary_pct"
    ,"bene_race_hspnc_pct": "hispanic_beneficiary_pct"
    ,"bene_race_othr_pct": "other_race_beneficiary_pct"
    ,"acute_hosp_readmsn_cnt": "acute_readmission_count"
    ,"acute_hosp_readmsn_pct": "acute_readmission_pct"
    ,"tot_mdcr_pymt_pc": "medicare_payment_per_capita"
    ,"tot_mdcr_stdzd_pymt_pc": "standardized_medicare_payment_per_capita"
   ,"er_visits_per_1000_benes": "er_visits_per_1000_beneficiaries"
}

SCHEMA_MAP = {
    "performance_year": "int"
    ,"total_beneficiaries": "bigint"
    ,"ffs_beneficiaries_count": "bigint"
    ,"dual_eligible_pct": "double"
    ,"female_beneficiary_pct": "double"
    ,"male_beneficiary_pct": "double"
    ,"white_beneficiary_pct": "double"
    ,"black_beneficiary_pct": "double"
    ,"hispanic_beneficiary_pct": "double"
    ,"other_race_beneficiary_pct": "double"
    ,"acute_readmission_count": "bigint"
    ,"acute_readmission_pct": "double"
    ,"medicare_payment_per_capita": "double"
    ,"standardized_medicare_payment_per_capita": "double"
    ,"er_visits_per_1000_beneficiaries": "double"
}

BUSINESS_KEYS = [
    "performance_year"
    ,"geography_name"
    ,"beneficiary_age_level"
]

def standardize_state_code(df) -> DataFrame:
    df = (
        df.withColumn(
            "state_code",
            when(lower(col("bene_geo_lvl")) == "state", df.bene_geo_desc)
            .when(lower(col("bene_geo_lvl")) == "county", split(df.bene_geo_desc, "-").getItem(0))
            .otherwise(None)
        )
    )
    return df

def standardize_county_name(df) -> DataFrame:
    df = (
        df.withColumn(
            "county_name",
            when(lower(col("bene_geo_lvl")) == "county", split(df.bene_geo_desc, "-").getItem(1))
            .otherwise(None)
        )
    )
    return df

def transformation_medicare(df) -> DataFrame:
    df = df.select(*COLUMNS)
    df = standardize_state_code(df)
    df = standardize_county_name(df)
    df = standardize_nulls(df, COLUMNS, NULL_VALUES)
    df = rename_columns(df, COLUMN_RENAME_MAP)
    df = cast_columns(df, SCHEMA_MAP)
    df = hash_bk(df, "medicare_bk_hash", BUSINESS_KEYS)
    df = deduplicate(df, "medicare_bk_hash", "load_timestamp")
    df = add_audit_columns(df, "bronze.medicare")
    return df
