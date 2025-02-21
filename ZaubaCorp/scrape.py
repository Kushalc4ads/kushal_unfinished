import requests
import json
import time
from bs4 import BeautifulSoup
import csv
from pathlib import Path
import logging
import concurrent.futures
import queue
import threading

# Set up logging
logging.basicConfig(
    filename='scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class CompanyScraper:
    def __init__(self, max_workers=3):
        self.zyte_api_key = "Replace with a Zyte API key"
        self.base_url = "https://www.zaubacorp.com/company-list/p-{}-company.html"
        self.companies_file = "companies.csv"
        self.progress_file = "progress.txt"
        self.total_companies_target = 3_000_000
        self.companies_found = 0
        self.current_page = self.load_progress()
        self.max_workers = max_workers
        self.results_queue = queue.Queue()
        self.csv_lock = threading.Lock()

    def load_progress(self):
        try:
            with open(self.progress_file, 'r') as f:
                return int(f.read().strip())
        except FileNotFoundError:
            return 1

    def save_progress(self):
        with open(self.progress_file, 'w') as f:
            f.write(str(self.current_page))

    def count_existing_companies(self):
        try:
            with open(self.companies_file, 'r') as f:
                return sum(1 for _ in f) - 1
        except FileNotFoundError:
            return 0

    def scrape_page(self, page_number):
        url = self.base_url.format(page_number)

        for attempt in range(3):  # Maximum 3 retries
            try:
                response = requests.post(
                    "https://api.zyte.com/v1/extract",
                    auth=(self.zyte_api_key, ""),
                    json={
                        "url": url,
                        "browserHtml": True,
                    },
                    timeout=30
                )

                if response.status_code != 200:
                    logging.error(f"Error fetching page {page_number}: {response.text}")
                    time.sleep(2 ** attempt)  # Exponential backoff
                    continue

                html_content = response.json()["browserHtml"]
                soup = BeautifulSoup(html_content, 'lxml')  # Using lxml parser for better performance

                companies = []
                table = soup.find('table', {'id': 'table'})
                if not table:
                    logging.error(f"No table found on page {page_number}")
                    return None

                for row in table.find_all('tr')[1:]:
                    cols = row.find_all('td')
                    if len(cols) >= 2:
                        cin = cols[0].text.strip()
                        company_name = cols[1].find('a').text.strip() if cols[1].find('a') else cols[1].text.strip()
                        companies.append((cin, company_name))

                return companies

            except Exception as e:
                logging.error(f"Error processing page {page_number}: {str(e)}")
                if attempt < 2:  # Don't sleep on last attempt
                    time.sleep(2 ** attempt)

        return None

    def save_companies_batch(self, companies):
        if not companies:
            return

        with self.csv_lock:
            mode = 'a' if Path(self.companies_file).exists() else 'w'
            with open(self.companies_file, mode, newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                if mode == 'w':
                    writer.writerow(['CIN', 'Company Name'])
                writer.writerows(companies)

    def process_page_range(self, start_page, end_page):
        for page in range(start_page, end_page + 1):
            companies = self.scrape_page(page)
            if companies:
                self.results_queue.put((page, companies))
            time.sleep(1)  # Minimal delay between requests

    def run(self):
        self.companies_found = self.count_existing_companies()
        logging.info(f"Starting from page {self.current_page} with {self.companies_found} companies already scraped")

        while self.companies_found < self.total_companies_target:
            batch_size = self.max_workers * 2
            end_page = self.current_page + batch_size - 1

            # Create thread pool for parallel processing
            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit page ranges to thread pool
                futures = []
                for i in range(self.max_workers):
                    start = self.current_page + (i * (batch_size // self.max_workers))
                    end = start + (batch_size // self.max_workers) - 1
                    if end > end_page:
                        end = end_page
                    futures.append(executor.submit(self.process_page_range, start, end))

                # Process results as they come in
                completed_pages = set()
                while len(completed_pages) < batch_size:
                    try:
                        page, companies = self.results_queue.get(timeout=60)
                        if companies:
                            self.save_companies_batch(companies)
                            self.companies_found += len(companies)
                            completed_pages.add(page)
                            logging.info(f"Processed page {page}. Total companies: {self.companies_found}")
                    except queue.Empty:
                        break

            self.current_page = max(completed_pages) + 1 if completed_pages else self.current_page + batch_size
            self.save_progress()

            if not completed_pages:
                logging.error(f"No pages successfully processed in batch. Waiting before retry...")
                time.sleep(60)

if __name__ == "__main__":
    scraper = CompanyScraper(max_workers=3)  # Adjust number of workers as needed
    try:
        scraper.run()
    except KeyboardInterrupt:
        logging.info("Scraping interrupted by user")
        scraper.save_progress()
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        scraper.save_progress()