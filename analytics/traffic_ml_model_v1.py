import pandas as pd
from sklearn.ensemble import RandomForestRegressor
import joblib
import os

# Buat folder models jika belum ada
if not os.path.exists('models'):
    os.makedirs('models')

# 1. Load data yang sudah dibersihkan
df = pd.read_csv('data/clean/traffic_smartcity_clean_v1.csv') 

# 2. Konversi kolom datetime
df['datetime'] = pd.to_datetime(df['datetime']) 

# 3. Feature Engineering
df['hour'] = df['datetime'].dt.hour 
df['day'] = df['datetime'].dt.dayofweek 
df['lag1'] = df['traffic'].shift(1) 
df = df.dropna()

# 4. Menentukan Fitur (X) dan Target (y)
X = df[['hour', 'day', 'lag1']] 
y = df['traffic']

# 5. Inisialisasi dan Pelatihan Model
model = RandomForestRegressor(n_estimators=100, random_state=42) 
model.fit(X, y) 

# 6. Menyimpan model
joblib.dump(model, 'models/traffic_model_v1.pkl') 

print("Model berhasil disimpan di folder models/")