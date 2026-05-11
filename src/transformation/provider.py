from databricks.sdk.runtime import*
from pyspark.sql.functions import*
from pyspark.sql.window import*
from pyspark.sql.types import*
from src.transformation.common import*

COLUMNS = [
    "prvdr_num"
    ,"fac_name"
    ,"city_name"
    ,"state_cd"
    ,"zip_cd"
    ,"fips_state_cd"
    ,"fips_cnty_cd"
    ,"cbsa_cd"
    ,"cbsa_urbn_rrl_ind"
    ,"prvdr_ctgry_cd"
    ,"prvdr_ctgry_sbtyp_cd"
    ,"gnrl_fac_type_cd"
    ,"bed_cnt"
    ,"crtfd_bed_cnt"
    ,"crtfctn_dt"
    ,"orgnl_prtcptn_dt"
    ,"pgm_trmntn_cd"
    ,"source_data"
    ,"source_code"
    ,"load_timestamp"
]

COLUMN_RENAME_MAP = {
    "prvdr_num": "provider_number"
    ,"fac_name": "facility_name"
    ,"state_cd": "state_code"
    ,"zip_cd": "zip_code"
    ,"fips_state_cd": "fips_state_code"
    ,"fips_cnty_cd": "fips_county_code"
    ,"cbsa_cd": "cbsa_code"
    ,"cbsa_urbn_rrl_ind": "cbsa_urban_rural_ind"
    ,"prvdr_ctgry_cd": "provider_category_code"
    ,"prvdr_ctgry_sbtyp_cd": "provider_category_subtype_code"
    ,"gnrl_fac_type_cd": "general_facility_type_code"
    ,"bed_cnt": "bed_count"
    ,"crtfd_bed_cnt": "certified_bed_count"
    ,"crtfctn_dt": "certification_date"
    ,"orgnl_prtcptn_dt": "original_participation_date"
    ,"pgm_trmntn_cd": "program_termination_code"
}

NULL_VALUES = [
    "NA"
    ,"N/A"
    ,"N"
]

CASE_MAP = {
    "upper": ["provider_number", "state_code", "zip_code", "fips_state_code", "fips_county_code", "cbsa_code", "cbsa_urban_rural_ind", "provider_category_code", "provider_category_subtype_code", "general_facility_type_code", "program_termination_code"]
    ,"lower": ["bed_count", "certified_bed_count", "certification_date", "original_participation_date"]
    ,"proper": ["facility_name", "city_name"]
}

SCHEMA_MAP = {
    "certification_date": "date"
    ,"original_participation_date": "date"
    ,"bed_count": "int"
    ,"certified_bed_count": "int"
}

BUSINESS_KEYS = ["provider_number"]

def standardize_cbsa(df) -> DataFrame:
    df = (
        df.withColumn(
            "cbsa_urban_rural_ind_std",
            when(col("cbsa_urbn_rrl_ind") == "001", "U")
            .when(col("cbsa_urbn_rrl_ind") == "002", "R")
            .otherwise(col("cbsa_urbn_rrl_ind"))
        )
    )
    return df

def transformation_provider(df) -> DataFrame:
    df = df.select(*COLUMNS)
    df = standardize_cbsa(df)
    df = standardize_nulls(df, COLUMNS, NULL_VALUES)
    df = rename_columns(df, COLUMN_RENAME_MAP)
    #df = apply_case(df, CASE_MAP) 
    df = cast_columns(df, SCHEMA_MAP)
    df = hash_bk(df, "provider_bk_hash", BUSINESS_KEYS)
    df = deduplicate(df, "provider_bk_hash", "load_timestamp")
    df = add_audit_columns(df, "bronze.provider")
    return df
