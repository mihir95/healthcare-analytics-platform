from pyspark.sql.function import upper, lower, initcap

CASE_FUNCS = {
    "lower": lower,
    "upper": upper,
    "proper": initcap
}

def 

def rename_cols(spark, df):
    return

def apply_case(spark, df, case_config):
    for case_type, columns in case_config.items():
        func = CASE_FUNCS.get(case_type)


def convert_nulls(spark, df):
    df = df.
    