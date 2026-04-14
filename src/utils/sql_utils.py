import os

def run_sql_file (spark, path, params=None):
    try:
        with open(path, "r") as f:
            sql_text = f.read().strip()
            if params:
                sql_text = sql_text.format(**params)
            spark.sql(sql_text)
    except Exception as e:
        print(f"Error executing SQL file: {path}")
        raise e
        