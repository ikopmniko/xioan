import requests
import time
from concurrent.futures import ThreadPoolExecutor
from supabase import create_client, Client
from bs4 import BeautifulSoup

# Konfigurasi Supabase
SUPABASE_URL = "https://cqakrownxujefhtmsefa.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImNxYWtyb3dueHVqZWZodG1zZWZhIiwicm9sZSI6ImFub24iLCJpYXQiOjE3MzIyNjMyMzMsImV4cCI6MjA0NzgzOTIzM30.E9jJxNBxFsVZsndwhsMZ_2hXaeHdDTLS7jZ50l-S72U"
SUPABASE_TABLE_NAME = "scpok"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Konfigurasi ScraperAPI
SCRAPER_API_KEY = "d6dc15f2ac972c5af6f6ff499594fa7e"
SCRAPER_URL = "http://api.scraperapi.com"

# Rentang baris yang ingin diproses
mulai = 0
endnya = 2500

def search_url(idx, target_url):
    params = {
        "api_key": SCRAPER_API_KEY,
        "url": f"https://www.google.com/search?q=site:{target_url}"
    }

    try:
        response = requests.get(SCRAPER_URL, params=params)
        soup = BeautifulSoup(response.text, "html.parser")

        # Ambil hasil pencarian pertama
        result = soup.find("div", class_="tF2Cxc") or soup.find("div", class_="g")

        if result:
            title_tag = result.find("h3")
            link_tag = result.find("a", href=True)

            if title_tag and link_tag:
                title = title_tag.text.strip()
                link = link_tag["href"].strip()
                snippet = result.find("span", class_="aCOpRe")
                snippet_text = snippet.text.strip() if snippet else "tidak ada snippet"

                print(f"[{idx}] ✔ {title} -> {link}")
                return {
                    "title": title,
                    "url": link,
                    "snippet": snippet_text
                }

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

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(worker, idx + start_line, url) for idx, url in enumerate(urls)]
        for f in futures:
            f.result()

if __name__ == "__main__":
    main(start_line=mulai, end_line=endnya)
