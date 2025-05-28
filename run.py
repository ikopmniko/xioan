import requests
import time
from concurrent.futures import ThreadPoolExecutor
from queue import Queue, Empty
from supabase import create_client, Client

SUPABASE_URL = "https://cqakrownxujefhtmsefa.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNxYWtyb3dueHVqZWZodG1zZWZhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzIyNjMyMzMsImV4cCI6MjA0NzgzOTIzM30.E9jJxNBxFsVZsndwhsMZ_2hXaeHdDTLS7jZ50l-S72U"
SUPABASE_TABLE_NAME = "dos2"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

API_KEY = "af272f81d0526485fe9c97aecd4403cc4a676a63"
URL_API = "https://google.serper.dev/search"

mulai = 2500
endnya = 5000

headers = {
    "X-API-KEY": API_KEY,
    "Content-Type": "application/json"
}

def search_url(idx, target_url):
    query = f"site:{target_url}"
    payload = {"q": query}

    try:
        response = requests.post(URL_API, headers=headers, json=payload)
        data = response.json()

        if "organic" in data and len(data["organic"]) > 0:
            top_result = data["organic"][0]
            title = top_result.get("title", "")
            link = top_result.get("link", "")
            print(f"[{idx}] ✔ {title} -> {link}")
            return {"title": title, "url": link}
        else:
            print(f"[{idx}] ❌ Tidak ditemukan: {target_url}")
            return None

    except Exception as e:
        print(f"[{idx}] ⚠ Error: {e}")
        return None

    finally:
        time.sleep(1.2)

def insert_to_supabase(data):
    if data is None:
        return
    try:
        res = supabase.table(SUPABASE_TABLE_NAME).insert(data).execute()
        if res.status_code != 201:
            print(f"⚠ Gagal insert ke Supabase: {res}")
        else:
            print("✔ Data berhasil disimpan ke Supabase")
    except Exception as e:
        print(f"⚠ Error insert ke Supabase: {e}")

def worker(idx, url):
    res = search_url(idx, url)
    if res:
        insert_to_supabase(res)

def main(start_line=1, end_line=None):
    with open("list_url.csv", "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    urls = urls[start_line - 1 : end_line]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, idx + 1, url) for idx, url in enumerate(urls)]

        for f in futures:
            f.result()  # tunggu semua selesai

if __name__ == "__main__":
    # Contoh scrap dari baris 10 sampai 20
    main(start_line=mulai, end_line=endnya)
