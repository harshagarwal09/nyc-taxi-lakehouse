import os
import time
import urllib.request

os.makedirs("data/raw", exist_ok=True)
BASE = "https://d37ci6vzurychx.cloudfront.net"

def download(url, path, tries=5):
    if os.path.exists(path):
        print("already have:", path)
        return
    for attempt in range(1, tries + 1):
        try:
            tmp = path + ".part"
            urllib.request.urlretrieve(url, tmp)
            os.replace(tmp, path)
            print("ok:", path, f"({os.path.getsize(path) / 1e6:.1f} MB)")
            return
        except Exception as e:
            print(f"attempt {attempt} failed: {e}")
            time.sleep(5 * attempt)
    raise SystemExit(f"giving up on {url}")

for m in ["2024-01", "2024-02", "2024-03"]:
    name = f"yellow_tripdata_{m}.parquet"
    download(f"{BASE}/trip-data/{name}", f"data/raw/{name}")

download(f"{BASE}/misc/taxi_zone_lookup.csv", "data/raw/taxi_zone_lookup.csv")