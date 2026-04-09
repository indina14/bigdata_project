import pandas as pd

# Membaca dataset dari folder raw 
df = pd.read_csv('data/raw/traffic_smartcity_v1.csv') 

# Mengonversi kolom datetime menjadi tipe data datetime agar bisa diolah [cite: 63]
df['datetime'] = pd.to_datetime(df['datetime']) 
# Mengurutkan data berdasarkan waktu untuk memastikan urutan yang benar 
df = df.sort_values('datetime') 

# Menghapus baris yang memiliki nilai kosong (missing values) [cite: 67]
df = df.dropna() 

# Menyimpan hasil pembersihan ke dalam folder data/clean/ [cite: 68]
df.to_csv('data/clean/traffic_smartcity_clean_v1.csv', index=False) 

# Memberikan notifikasi bahwa proses pembersihan selesai [cite: 69]
print("Data cleaning selesai") 