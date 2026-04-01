import json
import random
import time
import os
from datetime import datetime

# Konfigurasi folder output
output_folder = "data/raw/transportation"
os.makedirs(output_folder, exist_ok=True)

# Data simulasi
locations = ["Jakarta", "Bandung", "Surabaya", "Medan", "Makassar"]
drivers = ["Andi", "Budi", "Citra", "Dedi", "Eka"]
status_list = ["Lancar", "Padat", "Macet Total"]

counter = 1

print("Starting Transportation Data Generator... (Ctrl+C to stop)")

while True:
    # Simulasi data trip (Perbaikan Indentasi & Kolom Lengkap)
    trip_data = {
        "trip_id": f"TRP-{random.randint(1000, 9999)}",
        "driver_name": random.choice(drivers),
        "location": random.choice(locations),
        "speed": random.randint(5, 80),
        "status": random.choice(status_list),
        "fare": random.randint(10000, 100000), 
        "vehicle_type": random.choice(["Motor", "Mobil", "Bus"]),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Path file JSON
    filename = f"trip_{counter}.json"
    filepath = os.path.join(output_folder, filename)
    
    # Simpan ke file JSON
    with open(filepath, "w") as f:
        json.dump(trip_data, f)
        
    print(f"[{counter}] Generated: {trip_data['trip_id']} - {trip_data['location']} ({trip_data['speed']} km/h)")
    
    counter += 1
    time.sleep(3) # Generate data setiap 3 detik