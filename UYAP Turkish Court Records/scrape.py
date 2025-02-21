import os
import csv
import json
import base64
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# -----------------------------
# Configuration
# -----------------------------
ZYTE_API_KEY = ""

TOTAL_PAGES_TO_FETCH = 63815      # Number of pages to fetch
PAGE_SIZE = 10                    # Records per page
CONCURRENT_REQUESTS = 5           # Number of threads for document downloads
CSV_FILE = "downloaded_docs.csv"
PROGRESS_FILE = "progress.json"

# Columns to store in CSV (besides the downloaded text)
CSV_COLUMNS = [
    "id",
    "daire",
    "esasNo",
    "kararNo",
    "kararTarihi",
    "arananKelime",
    "durum",
    "index",
    "doc_text",  # We’ll store the downloaded text here
]

# -----------------------------
# Helper functions
# -----------------------------
def load_progress(filepath=PROGRESS_FILE):
    """
    Load existing progress from a JSON file.
    Returns a dict with keys:
      - pages_done: set of page numbers completed
      - ids_downloaded: set of document IDs completed
    """
    if not os.path.exists(filepath):
        return {"pages_done": set(), "ids_downloaded": set()}

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    # Convert lists back to sets
    data["pages_done"] = set(data.get("pages_done", []))
    data["ids_downloaded"] = set(data.get("ids_downloaded", []))
    return data

def save_progress(progress, filepath=PROGRESS_FILE):
    """
    Save progress to a JSON file.
    Convert sets to lists for JSON serialization.
    """
    data = {
        "pages_done": list(progress["pages_done"]),
        "ids_downloaded": list(progress["ids_downloaded"]),
    }
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def init_csv_file(csv_path=CSV_FILE, columns=CSV_COLUMNS):
    """
    Initialize the CSV file if it doesn't exist.
    Writes the header row once.
    """
    if not os.path.exists(csv_path):
        with open(csv_path, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=columns, quoting=csv.QUOTE_ALL)
            writer.writeheader()

def append_row_to_csv(row_data, csv_path=CSV_FILE, columns=CSV_COLUMNS):
    """
    Appends a single row of data to the CSV file.
    row_data is a dictionary with keys matching columns.
    """
    with open(csv_path, mode="a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns, quoting=csv.QUOTE_ALL)
        writer.writerow(row_data)


# -----------------------------
# Zyte Helper
# -----------------------------
def zyte_request(payload):
    """
    Generic helper to send a POST to Zyte’s /v1/extract endpoint using Basic Auth.
    `payload` is a dict describing the extraction request (url, httpRequestMethod, etc.).
    Returns the JSON-decoded response from Zyte.

    If 'httpResponseBody' is True, we decode the resulting base64 body from
    'httpResponseBody' in the response and return it as response["decodedBody"] for convenience.
    """
    # 1) Prepare headers with Basic Auth
    auth_string = f"{ZYTE_API_KEY}:".encode("utf-8")
    auth_token = base64.b64encode(auth_string).decode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {auth_token}",
    }

    # 2) Make the request
    resp = requests.post("https://api.zyte.com/v1/extract", headers=headers, json=payload)
    resp.raise_for_status()

    # 3) Parse JSON
    data = resp.json()

    # 4) If the user asked for httpResponseBody, decode it from base64
    if payload.get("httpResponseBody") and "httpResponseBody" in data:
        raw_b64 = data["httpResponseBody"]
        if raw_b64:
            decoded = base64.b64decode(raw_b64).decode("utf-8", errors="replace")
        else:
            decoded = ""
        # Store as an extra key for convenience
        data["decodedBody"] = decoded

    return data


def post_aramadetaylist(page_number=1, page_size=10):
    """
    Makes the POST request to get a page of data (document IDs & metadata)
    using Zyte’s /v1/extract for a POST-based extraction.
    """
    # The original code had a JSON payload for the site:
    original_site_payload = {
        "data": {
            "arananKelime": "",
            "Bam Hukuk Mahkemeleri": "İstanbul Bölge Adliye Mahkemesi 46. Hukuk Dairesi",
            "Hukuk Mahkemeleri": "Bakırköy 7. Asliye Ticaret Mahkemesi",
            "esasYil": "",
            "esasIlkSiraNo": "",
            "esasSonSiraNo": "",
            "kararYil": "",
            "kararIlkSiraNo": "",
            "kararSonSiraNo": "",
            "baslangicTarihi": "",
            "bitisTarihi": "",
            "siralama": "3",
            "siralamaDirection": "desc",
            "birimHukukMah": (
                "Istanbul Bölge Adliye Mahkemesi 1. Hukuk Dairesi+Adana Bölge Adliye Mahkemesi 1. Hukuk Dairesi+"
                "Adana Bölge Adliye Mahkemesi 11. Hukuk Dairesi+Adana Bölge Adliye Mahkemesi 4. Hukuk Dairesi+"
                "Ankara Bölge Adliye Mahkemesi 20. Hukuk Dairesi+...+Bakırköy 7. Asliye Ticaret Mahkemesi"
            ),
            "pageSize": page_size,
            "pageNumber": page_number
        }
    }

    # Construct the request body for Zyte
    # We'll do an HTTP POST to https://emsal.uyap.gov.tr/aramadetaylist
    # and supply the above JSON as the request body.
    zyte_payload = {
        "url": "https://emsal.uyap.gov.tr/aramadetaylist",
        "httpRequestMethod": "POST",
        "httpResponseBody": True,  # We want the raw JSON from the response
        "customHttpRequestHeaders": [
            {"name": "User-Agent", "value": "Mozilla/5.0"},
            {"name": "Accept", "value": "*/*"},
            {"name": "Accept-Language", "value": "en-US,en;q=0.9"},
            {"name": "Content-Type", "value": "application/json; charset=UTF-8"},
            {"name": "Origin", "value": "https://emsal.uyap.gov.tr"},
            {"name": "Referer", "value": "https://emsal.uyap.gov.tr/"},
            {"name": "X-Requested-With", "value": "XMLHttpRequest"},
        ],
        # The POST body: pass as either httpRequestText (UTF-8) or httpRequestBody (base64).
        "httpRequestText": json.dumps(original_site_payload),
    }

    # Send request via Zyte
    data = zyte_request(zyte_payload)

    # The original site returns JSON in the body. So parse the "decodedBody" as JSON:
    raw_body = data.get("decodedBody", "")
    try:
        parsed_json = json.loads(raw_body)
        return parsed_json  # We'll return the parsed response from the site
    except json.JSONDecodeError as e:
        # If there's an error decoding the JSON, raise or handle it
        raise RuntimeError(f"Could not parse JSON from page {page_number}: {e}")


def download_document(doc_id):
    """
    Downloads the document text for the given doc_id via Zyte,
    returning the decoded text content.
    """
    # The site requires a GET to https://emsal.uyap.gov.tr/getDokuman?id={doc_id}
    zyte_payload = {
        "url": f"https://emsal.uyap.gov.tr/getDokuman?id={doc_id}",
        "httpRequestMethod": "GET",
        "httpResponseBody": True,  # we want raw text
        "customHttpRequestHeaders": [
            {"name": "User-Agent", "value": "Mozilla/5.0"},
        ],
    }

    data = zyte_request(zyte_payload)
    # data["decodedBody"] is the raw text we want
    doc_text = data.get("decodedBody", "")
    print(f"[download_document] doc_id={doc_id}, length={len(doc_text)} chars")

    return doc_text


# -----------------------------
# Main script logic
# -----------------------------
def main():
    # 1. Load or create the progress data
    progress = load_progress(PROGRESS_FILE)
    pages_done = progress["pages_done"]
    ids_downloaded = progress["ids_downloaded"]

    # 2. Initialize the CSV file (write header if not exists)
    init_csv_file(CSV_FILE, CSV_COLUMNS)

    # 3. Iterate over the pages
    for page_number in range(1, TOTAL_PAGES_TO_FETCH + 1):
        if page_number in pages_done:
            print(f"Page {page_number} already processed; skipping.")
            continue

        print(f"\n=== Fetching page {page_number} ===")
        try:
            data = post_aramadetaylist(page_number=page_number, page_size=PAGE_SIZE)
        except Exception as e:
            print(f"Error fetching page {page_number}: {e}")
            continue

        # The JSON structure is expected as in your original code:
        # data["data"]["data"] is a list of records with keys like "id", "daire", etc.
        # Adjust if the structure has changed.
        records = data.get("data", {}).get("data", [])

        if not records:
            print(f"No records found on page {page_number}.")
            # Still mark the page as done to avoid re-checking
            pages_done.add(page_number)
            save_progress({"pages_done": pages_done, "ids_downloaded": ids_downloaded}, PROGRESS_FILE)
            continue

        print(f"Page {page_number} returned {len(records)} records.")

        # 4. Download documents (concurrently) only for IDs not yet downloaded
        metadata_by_id = {}
        for r in records:
            doc_id = str(r.get("id", ""))
            if doc_id:
                metadata_by_id[doc_id] = r

        futures = {}
        with ThreadPoolExecutor(max_workers=CONCURRENT_REQUESTS) as executor:
            for doc_id, record in metadata_by_id.items():
                if doc_id not in ids_downloaded:
                    future = executor.submit(download_document, doc_id)
                    futures[future] = doc_id

            # Collect results
            for future in as_completed(futures):
                doc_id = futures[future]
                try:
                    doc_text = future.result()
                except Exception as e:
                    print(f"Error downloading doc_id {doc_id}: {e}")
                    continue

                # 5. Prepare the row for CSV
                row = {}
                row["id"] = doc_id
                row["daire"] = metadata_by_id[doc_id].get("daire", "")
                row["esasNo"] = metadata_by_id[doc_id].get("esasNo", "")
                row["kararNo"] = metadata_by_id[doc_id].get("kararNo", "")
                row["kararTarihi"] = metadata_by_id[doc_id].get("kararTarihi", "")
                row["arananKelime"] = metadata_by_id[doc_id].get("arananKelime", "")
                row["durum"] = metadata_by_id[doc_id].get("durum", "")
                row["index"] = metadata_by_id[doc_id].get("index", "")
                row["doc_text"] = doc_text

                # 6. Append row to CSV
                append_row_to_csv(row, CSV_FILE, CSV_COLUMNS)

                # Mark doc_id as downloaded
                ids_downloaded.add(doc_id)

        # Mark the page as completed
        pages_done.add(page_number)
        save_progress({"pages_done": pages_done, "ids_downloaded": ids_downloaded}, PROGRESS_FILE)
        print(f"Finished page {page_number}, progress saved.")

    print("\nAll requested pages have been processed!")


if __name__ == "__main__":
    main()
