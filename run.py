import requests
import time
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client, Client

# Konfigurasi Supabase
SUPABASE_URL = "https://cqakrownxujefhtmsefa.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNxYWtyb3dueHVqZWZodG1zZWZhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzIyNjMyMzMsImV4cCI6MjA0NzgzOTIzM30.E9jJxNBxFsVZsndwhsMZ_2hXaeHdDTLS7jZ50l-S72U"
SUPABASE_TABLE_NAME = "scpok"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Konfigurasi Apify
APIFY_API_TOKEN = "apify_api_KYYLgCawu5TDZgBEf9vRt82OvLZWnH4CL93x"  # Ganti dengan token Apify kamu
APIFY_API_URL = f"https://api.apify.com/v2/actor-tasks/apify/google-search-scraper/run-sync-get-dataset-items?token={APIFY_API_TOKEN}"

# Rentang baris yang ingin diproses
mulai = 0
endnya = 2500

def search_url(idx, target_url):
    query = f"site:{target_url}"
    payload = {
        "searchTerms": [query],
        "resultsPerPage": 1,
        "pageNumber": 1,
        "language": "en",
        "countryCode": "us"
    }

    headers = {
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(APIFY_API_URL, json=payload, headers=headers)
        if response.status_code != 200:
            print(f"[{idx}] ⚠ HTTP error {response.status_code}: {response.text}")
            return None

        results = response.json()

        if results and len(results) > 0:
            top_result = results[0]
            title = top_result.get("title", "")
            link = top_result.get("url", "")
            snippet = top_result.get("description", "tidak ada snippet")

            print(f"[{idx}] ✔ {title} -> {link}")
            return {
                "title": title,
                "url": link,
                "snippet": snippet
            }
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
        if res.error:
            print(f"⚠ Gagal insert ke Supabase: {res.error}")
        else:
            print(f"✔ Data berhasil disimpan ke Supabase: {res.data}")
    except Exception as e:
        print(f"⚠ Error insert ke Supabase: {e}")

def worker(idx, url):
    result = search_url(idx, url)
    if result:
        insert_to_supabase(result)

def main(start_line=1, end_line=None):
    with open("list_url.csv", "r", encoding="utf-8") as f:
        urls = [line.strip() for line in f if line.strip()]

    urls = urls[start_line - 1 : end_line]

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(worker, idx + start_line, url) for idx, url in enumerate(urls)]
        for f in futures:
            f.result()

if __name__ == "__main__":
    main(start_line=mulai, end_line=endnya)
