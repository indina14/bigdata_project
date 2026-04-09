import streamlit as st
import pandas as pd
import joblib
import matplotlib.pyplot as plt

# Konfigurasi halaman dashboard
st.set_page_config(page_title="Smart Traffic AI", layout="wide")
st.title("🚗 Smart City Traffic Dashboard") 

# 1. Load data yang sudah bersih
df = pd.read_csv('data/clean/traffic_smartcity_clean_v1.csv')

# 2. Load model yang sudah dilatih sebelumnya
model = joblib.load('models/traffic_model_v1.pkl')
# 3. Persiapan data untuk visualisasi
df['datetime'] = pd.to_datetime(df['datetime']) 
df['hour'] = df['datetime'].dt.hour 
df['day'] = df['datetime'].dt.dayofweek 
df['lag1'] = df['traffic'].shift(1) 
df = df.dropna() 
# 4. Menampilkan Metrik Utama
col1, col2 = st.columns(2) 
col1.metric("Avg Traffic", int(df['traffic'].mean())) 
col2.metric("Max Traffic", int(df['traffic'].max())) 

# 5. Visualisasi Tren Traffic
st.subheader("Traffic Trend")
fig, ax = plt.subplots() 
ax.plot(df['traffic'].values) 
st.pyplot(fig) 

# 6. Bagian Prediksi Interaktif
st.subheader("🔮 Prediksi Traffic") 

# Input dari user menggunakan slider dan number input
hour = st.slider("Jam", 0, 23, 17) 
day = st.slider("Hari (0=Senin, 6=Minggu)", 0, 6, 2) 
lag1 = st.number_input("Traffic sebelumnya (Kendaraan)", 50, 300, 120) 

# Tombol untuk menjalankan prediksi
if st.button("Prediksi"): 
    pred = model.predict([[hour, day, lag1]]) 
    st.success(f"Prediksi: {int(pred[0])} kendaraan") 