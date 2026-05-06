# DASHBOARD STREAMLIT (STABLE)
import streamlit as st
from pyspark.sql import SparkSession
import plotly.express as px
import pandas as pd
from sklearn.linear_model import LinearRegression
import os

# 1. Dynamic Path Config[cite: 1]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

st.set_page_config(page_title="Traffic Dashboard", layout="wide")
st.title("🏙️ Smart City AI Traffic Dashboard")

# 2. Init Spark[cite: 1]
@st.cache_resource
def get_spark():
    return SparkSession.builder.appName("Dashboard_App").getOrCreate()

spark = get_spark()

# 3. Data Loading[cite: 1]
def load_parquet(folder_name):
    path = os.path.join(OUTPUT_DIR, folder_name)
    if not os.path.exists(path):
        st.error(f"⚠️ Folder data '{folder_name}' tidak ditemukan! Jalankan main script dulu.")
        st.stop()
    return spark.read.parquet(path).toPandas()

try:
    pdf = load_parquet("traffic")
    pdf_time = load_parquet("traffic_time")
    pdf_ml = load_parquet("ml_data")
except Exception as e:
    st.error(f"Gagal memuat data: {e}")
    st.stop()

# 4. Sidebar & Filter[cite: 1]
locations = pdf["location"].unique()
selected_loc = st.sidebar.selectbox("Pilih Lokasi Analisis", locations)
filtered_pdf = pdf[pdf["location"] == selected_loc]

# 5. KPI Metrics[cite: 1]
st.subheader("Key Performance Indicators")
col1, col2 = st.columns(2)
with col1:
    st.metric("Total Kendaraan (All)", int(pdf["total_vehicle"].sum()))
with col2:
    st.metric(f"Total di {selected_loc}", int(filtered_pdf["total_vehicle"].sum()))

# 6. Visualization[cite: 1]
st.markdown("---")
c1, c2 = st.columns(2)

with c1:
    st.subheader("Traffic Time Series")
    # Memperbaiki format window/timestamp[cite: 1]
    pdf_time["start_time"] = pdf_time["window"].apply(lambda x: x[0] if isinstance(x, tuple) else x.start)
    fig_line = px.line(pdf_time, x="start_time", y="total_vehicle", color="location")
    st.plotly_chart(fig_line, use_container_width=True)

# 7. AI Prediction[cite: 1]
with c2:
    st.subheader("AI Prediction (Linear Regression)")
    X = pdf_ml[["hour"]]
    y = pdf_ml["vehicle_count"]
    
    model = LinearRegression()
    model.fit(X, y)
    
    hour_input = st.slider("Prediksi Jam Ke-", 0, 23, 12)
    pred = model.predict([[hour_input]])
    st.success(f"Prediksi jumlah kendaraan pada jam {hour_input}:00 adalah **{int(pred[0])}**")