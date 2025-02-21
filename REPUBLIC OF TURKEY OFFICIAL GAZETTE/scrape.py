import os
import base64
import json
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# === Configuration ===
ZYTE_API_KEY = ""  # Replace with your API key
auth_header = base64.b64encode(f"{ZYTE_API_KEY}:".encode()).decode()
ZYTE_API_URL = "https://api.zyte.com/v1/extract"

TARGET_URL = "https://www.resmigazete.gov.tr/03.09.2020"
OUTPUT_DIR = "pdf_downloads"
os.makedirs(OUTPUT_DIR, exist_ok=True)

def zyte_extract(url, browserHtml=False, screenshot=False, httpResponseBody=False):
    """
    Uses Zyte API to extract the page with the requested options.
    Set httpResponseBody=True to return the full binary content (base64-encoded)
    of the response. This is useful for PDF URLs.
    """
    payload = {
        "url": url,
    }
    if browserHtml:
        payload["browserHtml"] = True
    if screenshot:
        payload["screenshot"] = True
    if httpResponseBody:
        payload["httpResponseBody"] = True

    headers = {
        "Authorization": f"Basic {auth_header}",
        "Content-Type": "application/json"
    }
    response = requests.post(ZYTE_API_URL, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching {url}:\nStatus Code: {response.status_code}\n{response.text}")
        return None

def parse_links_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    links = [a.get("href") for a in soup.find_all("a", href=True)]
    return links

def sanitize_filename(url, default="download"):
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path)
    if not filename or filename in ["/", "#"]:
        filename = default
    if not filename.lower().endswith(".pdf"):
        filename += ".pdf"
    return filename

# === Main Logic ===

# First, get the main page rendered via Zyte so that we can extract links
extraction = zyte_extract(TARGET_URL, browserHtml=True)
if extraction and "browserHtml" in extraction:
    html = extraction["browserHtml"]
    print("Fetched page HTML successfully.")
    links = parse_links_from_html(html)
    print(f"Found {len(links)} links.")

    for idx, link in enumerate(links, start=1):
        # Skip empty or invalid links
        if not link or link.strip() in ["#", "javascript:"]:
            continue

        normalized_url = urljoin(TARGET_URL, link)
        print(f"[{idx}] Processing URL: {normalized_url}")
        filename = os.path.join(OUTPUT_DIR, sanitize_filename(normalized_url, default=f"file_{idx}"))
        
        # For PDF URLs, use Zyte to get the full binary content
        if normalized_url.lower().endswith(".pdf"):
            page_data = zyte_extract(normalized_url, httpResponseBody=True)
            if page_data and "httpResponseBody" in page_data:
                try:
                    pdf_binary = base64.b64decode(page_data["httpResponseBody"])
                    with open(filename, "wb") as f:
                        f.write(pdf_binary)
                    print(f"Saved full PDF to: {filename}")
                except Exception as e:
                    print(f"Failed to save PDF from {normalized_url}: {e}")
            else:
                print(f"No httpResponseBody returned for {normalized_url}")
        else:
            # For non-PDF URLs, you can decide whether to use a screenshot or other extraction.
            # For example, if you wanted a screenshot, you could do:
            page_data = zyte_extract(normalized_url, screenshot=True)
            if page_data and "screenshot" in page_data:
                # Save the screenshot as a PNG file (change the extension if desired)
                png_filename = filename.replace(".pdf", ".png")
                try:
                    image_data = base64.b64decode(page_data["screenshot"])
                    with open(png_filename, "wb") as f:
                        f.write(image_data)
                    print(f"Saved screenshot to: {png_filename}")
                except Exception as e:
                    print(f"Failed to save screenshot for {normalized_url}: {e}")
            else:
                print(f"No screenshot available for {normalized_url}")
else:
    print("Failed to fetch the main page using Zyte.")
