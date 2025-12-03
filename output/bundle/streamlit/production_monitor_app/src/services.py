from snowflake.snowpark import Session
import pandas as pd
import duckdb

def get_data(session:Session):
    return session.sql("SELECT * FROM  DASHBOARD_DB.APP_SCHEMA.PIPELINE_RUNS").to_pandas()

def analyze_data(df:pd.DataFrame):
    query = """
    SELECT
        PIPELINE_NAME, 
        COUNT(*) AS TOTAL_RUNS,
        SUM(CASE WHEN STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESSFUL_RUNS,
        SUM(CASE WHEN STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_RUNS
    FROM df 
    GROUP BY PIPELINE_NAME
    ORDER BY FAILED_RUNS DESC
    """

    return duckdb.query(query).to_df()