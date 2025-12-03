import streamlit as st
import plotly.express as px
from snowflake.snowpark.context import get_active_session
from src.services import get_date, analyze_data

st.set_page_config(page_title="Pipeline Monitoring Dashboard", layout="wide")
st.title("🚀 Pipeline Monitoring Dashboard")

try:
    session = get_active_session()
except:
    conn = st.connection("snowflake")
    session = conn.session()

@st.cache_data
def load():
    return  get_date(session)

with st.spinner("Loading 1 Million Rows...."):
    df = load()

stats = analyze_data(df)

col1, col2 = st.columns(2)
with col1:
    st.dataframe(stats, use_container_width=True)
with col2:
    fig = px.bar(stats,x = 'PIPELINE_NAME', y='FAILED_RUNS', title='Failed Runs by Pipeline', labels={'FAILED_RUNS':'Number of Failed Runs','PIPELINE_NAME':'Pipeline Name'})
    st.plotly_chart(fig, use_container_width=True)