import pandas as pd
import os

# ==========================================
# 1. LOAD DATA
# ==========================================
def load_data(path):
    """Memuat semua file parquet dari folder tertentu ke dalam satu DataFrame."""
    if not os.path.exists(path):
        return pd.DataFrame()
    
    files = [f for f in os.listdir(path) if f.endswith(".parquet")]
    if not files:
        return pd.DataFrame()
    
    df_list = [pd.read_parquet(os.path.join(path, f)) for f in files]
    return pd.concat(df_list, ignore_index=True)

# ==========================================
# 2. PREPROCESS
# ==========================================
def preprocess(df):
    """Membersihkan data dan memastikan format timestamp benar."""
    if df.empty:
        return df
    
    df = df.copy()  # Hindari SettingWithCopyWarning
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
    return df.dropna(subset=["timestamp"])

# ==========================================
# 3. METRICS & ANALYSIS
# ==========================================
def compute_metrics(df):
    """Menghitung ringkasan metrik utama."""
    if df.empty:
        return {
            "total_trips": 0,
            "total_fare": 0,
            "top_location": "-"
        }
    
    return {
        "total_trips": len(df),
        "total_fare": df["fare"].sum(),
        "top_location": df.groupby("location")["fare"].sum().idxmax()
    }

def detect_peak_hour(df):
    """Mendeteksi jam dengan volume perjalanan tertinggi."""
    if df.empty:
        return None
    return df["timestamp"].dt.hour.value_counts().idxmax()

# ==========================================
# 4. VISUALIZATION DATA (OPTIMIZED)
# ==========================================
def fare_per_location(df):
    """Agregasi fare berdasarkan lokasi."""
    if df.empty:
        return pd.Series(dtype=float)
    return df.groupby("location")["fare"].sum().sort_values(ascending=False)

def vehicle_distribution(df):
    """Distribusi jumlah jenis kendaraan."""
    if df.empty:
        return pd.Series(dtype=int)
    return df["vehicle_type"].value_counts()

def mobility_trend(df):
    """Tren fare dalam interval 10 detik (Real-time trend)."""
    if df.empty:
        return pd.Series(dtype=float)
    return df.set_index("timestamp")["fare"].resample("10s").sum()

def traffic_per_window(df): 
    """
    Window Aggregation: Menghitung jumlah kendaraan per 1 menit.
    Teknik ini digunakan agar dashboard tidak lag saat data membesar.
    """
    if df.empty: 
        return None 
    
    # Pastikan data terurut berdasarkan waktu sebelum resampling
    df_sorted = df.sort_values('timestamp')
    return df_sorted.set_index('timestamp').resample('1min').size()

# ==========================================
# 5. ANOMALY DETECTION
# ==========================================
def detect_anomaly(df, threshold=80000):
    """Mendeteksi transaksi dengan fare yang tidak wajar."""
    if df.empty:
        return pd.DataFrame()
    return df[df["fare"] > threshold]