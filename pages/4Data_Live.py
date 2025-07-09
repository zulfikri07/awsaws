import streamlit as st
import pandas as pd
import numpy as np
import paho.mqtt.client as mqtt
import threading
import time
import re
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# ==================== KONFIGURASI MQTT ====================
MQTT_BROKER = "mqtt-dashboard.com"
MQTT_PORT = 1883
MQTT_TOPIC = "AWS@port"

# ==================== AUTH GOOGLE SHEETS ====================
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials_dict = dict(st.secrets["gcp_service_account"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
sheet_client = gspread.authorize(creds)

# Buka spreadsheet
spreadsheet = sheet_client.open("aws brin 2025")  # Nama Google Sheet
sheet = spreadsheet.sheet1  # Atau pakai .worksheet("NamaSheet")

# ==================== GLOBAL DATA ====================
mqtt_data = {"message": "Menunggu data..."}
data_cache = set()  # Hindari duplikat

# ==================== PARSE DATA ====================
def parse_sensor_data(text):
    try:
        data = {}
        # Ambil waktu dan tanggal
        time_match = re.search(r'(\d{2}:\d{2}:\d{2}) (\d{2}-\d{2}-\d{4})', text)
        if time_match:
            data["waktu"] = time_match.group(1)
            data["tanggal"] = time_match.group(2)
        data["temp"] = float(re.search(r"Temp\s*=\s*([\d.]+)", text).group(1))
        data["kelembaban"] = int(re.search(r"Kelembaban\s*=\s*(\d+)", text).group(1))
        data["w_speed"] = float(re.search(r"W\.Speed\s*=\s*([\d.]+)", text).group(1))
        data["w_dir"] = int(re.search(r"W\.Dir\s*=\s*(\d+)", text).group(1))
        data["press"] = float(re.search(r"Press\s*=\s*([\d.]+)", text).group(1))
        data["hujan"] = float(re.search(r"Hujan\s*=\s*([\d.]+)", text).group(1))
        data["rad"] = float(re.search(r"Rad\s*=\s*([\d.]+)", text).group(1))
        data["signal"] = int(re.search(r"Signal\s*=\s*(\d+)", text).group(1))
        return data
    except Exception as e:
        return {"error": str(e)}

# ==================== SIMPAN KE GOOGLE SHEET ====================
def save_to_google_sheet(data):
    row_key = f"{data['tanggal']} {data['waktu']}"
    if row_key in data_cache:
        return  # Hindari duplikat
    data_cache.add(row_key)
    row = [
        data.get("tanggal"), data.get("waktu"), data.get("temp"),
        data.get("kelembaban"), data.get("w_speed"), data.get("w_dir"),
        data.get("press"), data.get("hujan"), data.get("rad"), data.get("signal")
    ]
    sheet.append_row(row)

# ==================== MQTT CALLBACK ====================
def on_message(client, userdata, msg):
    payload = msg.payload.decode()
    parsed = parse_sensor_data(payload)
    mqtt_data.clear()
    mqtt_data.update(parsed)
    if "error" not in parsed:
        save_to_google_sheet(parsed)

def mqtt_thread():
    client = mqtt.Client()
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC)
    client.loop_forever()

# ==================== STREAMLIT START ====================
if "mqtt_started" not in st.session_state:
    threading.Thread(target=mqtt_thread, daemon=True).start()
    st.session_state["mqtt_started"] = True

st.set_page_config(page_title="Data_Live", layout="wide")
st.title("LIVE DATA MONITORING AWS")
st.caption(f"Topik MQTT: {MQTT_TOPIC}")
st.sidebar.image("pages/logommi.jpeg")

placeholder = st.empty()

# Tambahkan header jika belum ada (cek kolom pertama)
if len(sheet.row_values(1)) < 10:
    headers = ["Tanggal", "Waktu", "Suhu", "Kelembaban", "W.Speed", "W.Dir", "Tekanan", "Hujan", "Rad", "Signal"]
    sheet.insert_row(headers, 1)

while True:
    with placeholder.container():
        if "error" in mqtt_data:
            st.error(f"❌ Parsing Error: {mqtt_data['error']}")
        else:
            st.subheader("")
            col1, col2, col3 = st.columns(3)
            col1.metric("🕒 Waktu", mqtt_data.get("waktu", "-"))
            col2.metric("📅 Tanggal", mqtt_data.get("tanggal", "-"))
            col3.metric("📶 Signal", mqtt_data.get("signal", "-"))

            st.divider()
            col1, col2, col3 = st.columns(3)
            col1.metric("🌡️ Suhu", f"{mqtt_data.get('temp', '-')} °C")
            col2.metric("💧 Kelembaban", f"{mqtt_data.get('kelembaban', '-')} %")
            col3.metric("🌧️ Curah Hujan", f"{mqtt_data.get('hujan', '-')} mm")

            col1, col2, col3 = st.columns(3)
            col1.metric("💨 Kecepatan Angin", f"{mqtt_data.get('w_speed', '-')} m/s")
            col2.metric("🧭 Arah Angin", f"{mqtt_data.get('w_dir', '-')}°")
            col3.metric("📈 Tekanan", f"{mqtt_data.get('press', '-')} hPa")

            st.metric("☀️ Radiasi", f"{mqtt_data.get('rad', '-')} W/m²")
    time.sleep(1)