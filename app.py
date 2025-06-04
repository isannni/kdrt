import requests as req
from bs4 import BeautifulSoup as bs
from datetime import datetime
from pymongo import MongoClient
import time
import schedule
import logging
import sys
import re

# Setup logging (tetap sama)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("scraper.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("KDRT_Scraper")

# Koneksi MongoDB (tetap sama)
client = MongoClient('mongodb://localhost:27017')
db = client['CrawlingScrapping']
collection = db['coba']

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
}

def normalize_indonesian_date(date_str):
    # Kamus untuk mengganti nama bulan dan hari Indonesia ke Inggris
    indonesian_months = {
        'januari': 'Jan', 'februari': 'Feb', 'maret': 'Mar', 'april': 'Apr',
        'mei': 'May', 'juni': 'Jun', 'juli': 'Jul', 'agustus': 'Aug',
        'september': 'Sep', 'oktober': 'Oct', 'november': 'Nov', 'desember': 'Dec',
        'jan': 'Jan', 'feb': 'Feb', 'mar': 'Mar', 'apr': 'Apr', 'mei': 'May',
        'jun': 'Jun', 'jul': 'Jul', 'agu': 'Aug', 'aug': 'Aug', 'sep': 'Sep',
        'okt': 'Oct', 'nov': 'Nov', 'des': 'Dec', 'dec': 'Dec'
    }
    
    indonesian_days = {
        'senin': 'Mon', 'selasa': 'Tue', 'rabu': 'Wed', 'kamis': 'Thu',
        'jumat': 'Fri', 'sabtu': 'Sat', 'minggu': 'Sun'
    }
    
    # Normalisasi hari
    for id_day, en_day in indonesian_days.items():
        if id_day in date_str.lower():
            date_str = re.sub(id_day, en_day, date_str, flags=re.IGNORECASE)
    
    # Normalisasi bulan
    for id_month, en_month in indonesian_months.items():
        if id_month in date_str.lower():
            date_str = re.sub(id_month, en_month, date_str, flags=re.IGNORECASE)
    
    return date_str

def parse_date(date_str):
    try:
        # Normalisasi tanggal Indonesia ke format Inggris
        normalized_date = normalize_indonesian_date(date_str)
        
        # Format tanggal yang mungkin setelah normalisasi
        date_formats = [
            "%a, %d %b %Y %H:%M WIB",  # Fri, 16 May 2025 17:21 WIB
            "%d %b %Y %H:%M WIB",       # 16 May 2025 17:21 WIB
            "%a, %d %b %Y %H:%M",       # Fri, 16 May 2025 17:21
            "%Y-%m-%d %H:%M:%S",        # ISO format
            "%d/%m/%Y %H:%M"            # DD/MM/YYYY HH:MM
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(normalized_date, fmt)
            except ValueError:
                continue
        
        # Fallback: Cari pola tanggal dengan regex
        match = re.search(r'(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})', normalized_date)
        if match:
            day, month, year = match.groups()
            month = normalize_indonesian_date(month).capitalize()
            try:
                return datetime.strptime(f"{day} {month} {year}", "%d %b %Y")
            except:
                pass
        
        logger.warning(f"Format tanggal tidak dikenali: {date_str}")
        return datetime.now()
    
    except Exception as e:
        logger.error(f"Gagal parsing tanggal: {e}")
        return datetime.now()

def scrape_detik(jumlah_halaman):
    a = 1
    for page in range(1, jumlah_halaman + 1):
        try:
            logger.info(f"Scraping halaman {page}")
            url = f'https://www.detik.com/search/searchall?query=KDRT&siteid=2&source_kanal=true&page={page}'            
            res = req.get(url, headers=headers)
            
            if res.status_code != 200:
                logger.warning(f"Gagal mengambil halaman {page}. Kode status: {res.status_code}")
                continue

            soup = bs(res.text, 'html.parser')
            articles = soup.find_all('article', class_='list-content__item')

            if not articles:
                logger.info(f"Halaman {page} kosong.")
                continue

            for article in articles:
                try:
                    title_tag = article.find('h3', class_='media__title')
                    if not title_tag:
                        continue
                    a_tag = title_tag.find('a')
                    if not a_tag or 'href' not in a_tag.attrs:
                        continue
                    
                    link = a_tag['href']
                    title = a_tag.get_text(strip=True)
                    
                    # Ekstraksi tanggal
                    date_tag = article.find('div', class_='media_date')
                    date_str = date_tag.find('span')['title'] if date_tag and date_tag.find('span') else None
                    
                    if not date_str:
                        logger.warning("Tidak menemukan elemen tanggal")
                        parsed_date = datetime.now()
                    else:
                        parsed_date = parse_date(date_str)
                    
                    # Scrape konten artikel
                    detail_page = req.get(link, headers=headers)
                    detail_soup = bs(detail_page.text, 'html.parser')
                    body = detail_soup.find('div', class_='detail__body-text itp_bodycontent')
                    content = ' '.join([p.get_text(strip=True) for p in body.find_all('p')]) if body else ""
                    content = content.replace('ADVERTISEMENT', '').replace('\n', '')
                    
                    # Cek duplikat di database
                    if collection.find_one({'link': link}):
                        logger.info(f"Artikel sudah ada: {title[:40]}...")
                        continue
                    
                    # Simpan ke MongoDB
                    document = {
                        'judul': title,
                        'tanggal': parsed_date,
                        'link': link,
                        'isi': content
                    }
                    collection.insert_one(document)
                    logger.info(f'Data tersimpan [{a}] > {title[:40]}...')
                    a += 1
                    
                except Exception as e:
                    logger.error(f"Error pada artikel: {e}")
                
            time.sleep(1)  # Jeda antar halaman
            
        except Exception as e:
            logger.error(f"Error pada halaman {page}: {e}")

def run_scraper():
    """Fungsi untuk menjalankan scraper dengan jumlah halaman yang ditentukan"""
    logger.info("Memulai job scraping...")
    try:
        # Ganti jumlah halaman sesuai kebutuhan
        scrape_detik(10)  # Default scrape 10 halaman setiap kali dijalankan
        logger.info("Job scraping selesai!")
    except Exception as e:
        logger.error(f"Error dalam menjalankan job scraping: {e}")

# Fungsi untuk mengatur jadwal
def schedule_jobs():
    """Configure the scheduler with various job options and display next run time."""
    # Calculate and display next run times
    def display_next_run(job_name, schedule_time):
        next_run = schedule_time.next_run
        logger.info(f"Job '{job_name}' dijadwalkan berjalan berikutnya pada: {next_run}")
    
    # Clear any existing jobs first
    schedule.clear()
    
    # Jalankan setiap hari pada pukul 02:00 (daily job)
    daily_job = schedule.every().day.at("02:00").do(run_scraper)
    display_next_run("Daily Scraping (2 AM)", daily_job)
    
    # Jalankan setiap Senin pukul 08:00 (weekly job)
    weekly_job = schedule.every().monday.at("08:00").do(run_scraper)
    display_next_run("Weekly Scraping (Monday 8 AM)", weekly_job)
    
    # Jalankan setiap 6 jam sekali (interval job)
    interval_job = schedule.every(6).hours.do(run_scraper)
    display_next_run("6-Hour Interval Scraping", interval_job)
    
    logger.info("Scheduler telah diatur! Program akan melakukan scraping berdasarkan jadwal.")
    logger.info("Tekan Ctrl+C untuk menghentikan program.")
    
    # Jalankan scraper saat pertama kali program dijalankan
    logger.info("Menjalankan scraping awal...")
    run_scraper()
    
    # Loop untuk menjalankan scheduler dengan proper error handling
    while True:
        try:
            # Check if any jobs are due and run them
            schedule.run_pending()
            
            # Sleep for 60 seconds before checking again
            for i in range(60):
                time.sleep(1)
                # Periodically check if anything needs to be run (allows for more responsive shutdown)
                if i % 15 == 0 and schedule.jobs:
                    pending = [job.job_func.__name__ for job in schedule.jobs if job.should_run]
                    if pending:
                        logger.info(f"Jobs yang akan segera dijalankan: {pending}")
                        break
                        
        except KeyboardInterrupt:
            logger.info("Program dihentikan oleh pengguna. Selesai.")
            break
            
        except Exception as e:
            logger.error(f"Terjadi kesalahan dalam scheduler loop: {e}")
            # Continue running despite errors
            time.sleep(60)

if __name__ == "__main__":
    schedule_jobs()