import json
import time
import random
from kafka import KafkaProducer

# Inisialisasi Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Data simulasi
names = ["Indina", "Andi", "Budi", "Siti", "Rudi"]
locations = ["Jakarta", "Surabaya", "Luar Negeri", "Banjarbaru", "Medan"]

print("Kirim data transaksi ke Kafka... (Ctrl+C untuk stop)")

try:
    while True:
        data = {
            "nama": random.choice(names),
            "no_rekening": f"123-456-{random.randint(100, 999)}",
            "jumlah": random.randint(1000000, 70000000),
            "lokasi": random.choice(locations),
            "timestamp": time.strftime('%Y-%m-%d %H:%M:%S')
        }
        producer.send('bank_topic', value=data)
        print(f"Sent: {data}")
        time.sleep(2)
except KeyboardInterrupt:
    print("Selesai.")
