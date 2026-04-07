import streamlit as st
import time
import sys
import os

# ==========================================
# FIX MODULE PATH
# ==========================================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if BASE_DIR not in sys.path:
    sys.path.append(BASE_DIR)

# IMPORT CUSTOM MODULES
try:
    from analytics import transportation_analytics as ta
    from alerts import transportation_alert as alert
except ImportError as e:
    st.error(f"Gagal mengimpor modul: {e}")

# ==========================================
# CONFIG & UI SETUP
# ==========================================
DATA_PATH = "data/serving/transportation"
REFRESH_INTERVAL = 5

st.set_page_config(
    page_title="Smart Transportation Dashboard",
    layout="wide"
)

st.title("🚗 Smart Transportation Real-Time Analytics")
placeholder = st.empty()

# ==========================================
# MAIN STREAMING LOOP
# ==========================================
while True:
    with placeholder.container():
        # 1. LOAD & PREPROCESS
        df_raw = ta.load_data(DATA_PATH)
        
        if df_raw.empty:
            st.warning("🔄 Waiting for streaming transportation data...")
            time.sleep(REFRESH_INTERVAL)
            continue
            
        df = ta.preprocess(df_raw)

        # 2. KEY METRICS
        try:
            metrics = ta.compute_metrics(df)
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Trips", f"{metrics['total_trips']:,}")
            col2.metric("Total Fare", f"Rp {int(metrics['total_fare']):,}")
            col3.metric("Top Location", metrics["top_location"])
        except Exception as e:
            st.error(f"Error computing metrics: {e}")

        st.divider()

        # 3. ANALYSIS & ALERTS (Horizontal Layout)
        row_top_col1, row_top_col2 = st.columns([1, 2])
        
        with row_top_col1:
            st.subheader("⏰ Peak Hour")
            try:
                peak_hour = ta.detect_peak_hour(df)
                st.info(f"Peak traffic hour: **{peak_hour}:00**")
            except Exception:
                st.warning("Tidak dapat menghitung peak hour")

        with row_top_col2:
            st.subheader("🚨 Traffic Alerts")
            try:
                alerts = alert.generate_alert(df)
                if alerts:
                    for a in alerts:
                        st.error(a)
                else:
                    st.success("All systems normal - No alerts")
            except Exception as e:
                st.warning(f"Alert error: {e}")

        st.divider()

        # 4. VISUALIZATIONS
        st.subheader("📊 Fleet & Revenue Analytics")
        try:
            v_col1, v_col2 = st.columns(2)
            with v_col1:
                st.write("**Fare per Location**")
                st.bar_chart(ta.fare_per_location(df))
            with v_col2:
                st.write("**Vehicle Distribution**")
                st.bar_chart(ta.vehicle_distribution(df))
            
            st.write("**Mobility Trend (Fare 10s)**")
            st.line_chart(ta.mobility_trend(df))
            
            st.write("**Real-Time Traffic (Windowed 1min)**")
            traffic_window = ta.traffic_per_window(df)
            if traffic_window is not None:
                st.line_chart(traffic_window)
        except Exception as e:
            st.warning(f"Visualization error: {e}")

        st.divider()

        # 5. ANOMALY & RAW DATA
        tab1, tab2 = st.tabs(["⚠️ Abnormal Trips", "📋 Live Raw Data"])
        
        with tab1:
            try:
                anomaly_df = ta.detect_anomaly(df)
                if not anomaly_df.empty:
                    st.dataframe(anomaly_df.tail(20), use_container_width=True)
                else:
                    st.success("No anomalies detected in the current window.")
            except Exception as e:
                st.error(f"Anomaly error: {e}")
                
        with tab2:
            st.dataframe(df.tail(50), use_container_width=True)

    # 6. WAIT FOR NEXT REFRESH
    time.sleep(REFRESH_INTERVAL)