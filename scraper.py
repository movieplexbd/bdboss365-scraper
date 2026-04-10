import requests
from bs4 import BeautifulSoup
import firebase_admin
from firebase_admin import credentials, db
import json
import os
from datetime import datetime
import time
import logging

# ====================== CONFIG ======================
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def init_firebase():
    try:
        if os.path.exists("service-account.json"):
            cred = credentials.Certificate("service-account.json")
            logger.info("✅ Service account loaded from service-account.json")
        else:
            raise Exception("service-account.json ফাইল পাওয়া যায়নি!")
        
        firebase_admin.initialize_app(cred, {
            "databaseURL": "https://movies-bee24-default-rtdb.firebaseio.com"
        })
        logger.info("🔥 Firebase Connected Successfully!")
    except Exception as e:
        logger.error(f"❌ Firebase Error: {e}")
        raise

def scrape_homepage():
    url = "https://bdboss365.com/"
    logger.info(f"🌐 Scraping homepage: {url}")
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        thumbs = soup.find_all("div", class_="thumb")
        
        movies = []
        for thumb in thumbs[:35]:
            try:
                img = thumb.find("img")
                if not img: continue
                title = img.get("alt") or img.get("title") or "Unknown"
                image_url = img.get("src", "")
                a_tag = thumb.find("a", href=True)
                detail_url = a_tag["href"] if a_tag else None
                
                if detail_url and "bdboss365.com" in detail_url:
                    movies.append({
                        "title": title.strip(),
                        "image_url": image_url,
                        "detail_url": detail_url
                    })
            except:
                continue
        logger.info(f"✅ Found {len(movies)} movies/serials")
        return movies
    except Exception as e:
        logger.error(f"Homepage error: {e}")
        return []

def scrape_detail(detail_url, title):
    try:
        r = requests.get(detail_url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        
        full_title = title
        og_img = soup.find("meta", property="og:image")
        image_url = og_img["content"] if og_img else ""
        
        content = soup.find("div", class_="entry-content") or soup.find("article")
        description = content.get_text(strip=True)[:1500] if content else ""
        
        download_links = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True)
            if any(x in href.lower() for x in ["drive", "gdrive", "mega", "mediafire", "1click", "download"]) or "download" in text.lower():
                download_links.append({"text": text[:100], "url": href})
        
        return {
            "title": full_title,
            "image_url": image_url,
            "detail_url": detail_url,
            "description": description,
            "download_links": download_links,
            "scraped_at": datetime.now().isoformat(),
            "source": "bdboss365.com"
        }
    except Exception as e:
        logger.warning(f"Detail skipped: {title}")
        return None

def upload_to_firebase(all_data):
    ref = db.reference("/movies_bdboss")
    for item in all_data:
        if not item: continue
        safe_key = "".join(c for c in item["title"] if c.isalnum() or c in " -_")[:120]
        ref.child(safe_key).set(item)
        logger.info(f"✅ Uploaded: {item['title'][:70]}...")
    logger.info("🎉 All data uploaded to Firebase!")

def main():
    logger.info("🚀 BDboss365 Scraper Started")
    init_firebase()
    
    homepage_items = scrape_homepage()
    all_data = []
    
    for idx, item in enumerate(homepage_items, 1):
        data = scrape_detail(item["detail_url"], item["title"])
        if data:
            all_data.append(data)
        time.sleep(1.3)
        if idx % 10 == 0:
            logger.info(f"Processed {idx}/{len(homepage_items)}")
    
    upload_to_firebase(all_data)
    logger.info("🎉 FINISHED! Check your Firebase.")

if __name__ == "__main__":
    main()
