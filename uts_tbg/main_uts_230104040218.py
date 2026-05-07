import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Gunakan Absolute Path agar rapi
BASE_PATH = os.path.abspath(os.getcwd())
OUTPUT_DIR = os.path.join(BASE_PATH, "output")

# Pastikan folder output ada
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# 1. Generate Data (Simulasi 150 Menit)
def generate_data():
    sectors = ['Industrial_A', 'Industrial_B', 'Residential_C']
    data = []
    start_time = datetime.now()
    
    for i in range(150):
        timestamp = start_time + timedelta(minutes=i)
        for sector in sectors:
            usage = np.random.randint(100, 1000)
            data.append([timestamp, sector, usage])
    return pd.DataFrame(data, columns=['timestamp', 'sector', 'power_usage'])

df = generate_data()

# 2. Processing (Mirip Spark tapi pakai Pandas)
# A. Total konsumsi per sektor
energy_total = df.groupby('sector')['power_usage'].sum().reset_index(name='total_usage')

# B. Agregasi tiap 10 menit
df['start'] = df['timestamp'].dt.floor('10min')
energy_time = df.groupby(['start', 'sector'])['power_usage'].mean().reset_index(name='avg_usage')

# C. Dataset AI berdasarkan hour
df['hour'] = df['timestamp'].dt.hour
ml_energy = df.groupby('hour')['power_usage'].mean().reset_index()

# 3. Simpan ke Parquet (PENTING: Agar Dashboard bisa baca)
energy_total.to_parquet(os.path.join(OUTPUT_DIR, "energy_total"))
energy_time.to_parquet(os.path.join(OUTPUT_DIR, "energy_time"))
ml_energy.to_parquet(os.path.join(OUTPUT_DIR, "ml_energy"))

print(f"Data Pipeline Berhasil! File Parquet tersimpan di: {OUTPUT_DIR}")