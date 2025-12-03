import pandas as pd
import numpy as np
import streamlit as st
from snowflake.snowpark import Session
import time 

def get_session():

    secrets = st.secrets["connections"]["snowflake"]

    connection_parameters = {
        "account": secrets["account"],
        "user": secrets["user"],
        "password": secrets["password"],
        "role": secrets["role"],
        "warehouse": secrets["warehouse"],
        "database": secrets["database"],
        "schema": secrets["schema"],
        "insecure_mode": True
    }

    return Session.builder.configs(connection_parameters).create()



def generate_data():
    rows = 1_000_000

    pipelines = np.random.choice(['Sales','Finance','Inventory','HR','Logistics'],rows)
    statuses = np.random.choice(['SUCCESS','FAILED'],rows, p=[0.92,0.08])

    timestamps = pd.to_datetime(int(time.time()) - np.random.randint(0, 5184000, rows), unit='s')

    df = pd.DataFrame({
        'PIPELINE_NAME': pipelines,
        'STATUS': statuses,
        'RUN_DATE': timestamps,
        'ROWS_PROCESSED': np.random.randint(100, 50000, rows)
    })

    # df.to_csv('data/pipeline_runs.csv', index=False)
    # print("Creted log file at data/pipeline_runs.csv")

    print("Uploading data to Snowflake...")

    session = get_session()

    session.write_pandas(df, 'PIPELINE_RUNS', auto_create_table=True, overwrite=True)

    print("Data uploaded to Snowflake table PIPELINE_RUNS")
    session.close()

if __name__ == "__main__":
    generate_data()