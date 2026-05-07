import streamlit as st
import pandas as pd
import plotly.express as px
from sklearn.linear_model import LinearRegression 
import os

st.set_page_config(page_title="Energy Dashboard", layout="wide")
st.title("⚡ Smart Energy Consumption Analytics")

# Path Absolute untuk membaca data Parquet 
BASE_PATH = os.path.abspath(os.getcwd())
df_total = pd.read_parquet(os.path.join(BASE_PATH, "output/energy_total"))
df_time = pd.read_parquet(os.path.join(BASE_PATH, "output/energy_time"))
df_ml = pd.read_parquet(os.path.join(BASE_PATH, "output/ml_energy"))

# 1. Sidebar: Dropdown Sektor
st.sidebar.header("Filter")
selected_sector = st.sidebar.selectbox("Pilih Kawasan", df_total['sector'].unique())

# 2. KPI Total Konsumsi
total_val = df_total[df_total['sector'] == selected_sector]['total_usage'].values[0]
st.metric(label=f"Total Konsumsi {selected_sector}", value=f"{total_val:,} kWh")

# 3. Grafik Line Plotly
st.subheader("Tren Penggunaan Energi (10 Menit)")
df_filtered = df_time[df_time['sector'] == selected_sector]
fig = px.line(df_filtered, x="start", y="avg_usage", markers=True)
st.plotly_chart(fig, use_container_width=True)

# 4. AI Forecasting: Linear Regression
st.divider()
st.subheader("🤖 Prediksi Lonjakan Konsumsi (AI)")
X = df_ml[['hour']] 
y = df_ml['power_usage'] 

model = LinearRegression().fit(X, y)

hour_input = st.slider("Prediksi untuk Jam", 0, 23, 12)
prediction = model.predict([[hour_input]])

st.success(f"Hasil Prediksi jam {hour_input}:00 adalah {prediction[0]:.2f} kWh")