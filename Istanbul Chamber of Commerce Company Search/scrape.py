from playwright.sync_api import sync_playwright, TimeoutError
import json
import os
from datetime import datetime

def interact_with_all_dropdowns_and_capture_response():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        # Create output directories
        output_dir = "scraping_results"
        os.makedirs(output_dir, exist_ok=True)
        
        # Create log file
        log_file = os.path.join(output_dir, f"search_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

        # Go to the target page
        page.goto("https://bilgibankasi.ito.org.tr/tr/bilgi-bankasi/firma-bilgileri")
        page.wait_for_timeout(2000)

        # Click the "Nace Kodu Seçimine Göre" button
        page.click("span:has-text('Nace Kodu Seçimine Göre')")
        page.wait_for_timeout(2000)

        # Search button selector
        search_button_selector = (
            "xpath=/html/body/div[3]/div/div/div[2]/div/div/div/div/div/div/div/div/div[1]"
            "/div/div[4]/div/div/div/div/div/form/div/div[2]/div/label/button"
        )

        def get_dropdown_options(dropdown_index):
            dropdown = page.locator("span.k-dropdown-wrap").nth(dropdown_index)
            dropdown.click()
            page.wait_for_timeout(500)
            options_list = page.locator("ul.k-list[role='listbox']").last
            options = options_list.locator("li")
            count = options.count()
            dropdown.click()
            return dropdown, options, count

        def select_option(dropdown, options, index):
            dropdown.click()
            page.wait_for_timeout(300)
            option = options.nth(index)
            if option.is_visible():
                text = option.inner_text()
                option.click()
                page.wait_for_timeout(800)
                return text
            return None

        def collect_all_pages_data():
            all_data = []
            page_num = 1
            
            while True:
                page.wait_for_timeout(1000)
                
                try:
                    with page.expect_response("**/nace-code-select-search", timeout=30000) as resp_info:
                        next_button = page.locator("xpath=/html/body/div[3]/div/div/div[2]/div/div/div/div/div/div/div/div/div[2]/div/div/div/div/div[2]/div/div[3]/a[3]/span")
                        parent = page.locator("xpath=/html/body/div[3]/div/div/div[2]/div/div/div/div/div/div/div/div/div[2]/div/div/div/div/div[2]/div/div[3]/a[3]")
                        
                        if "k-state-disabled" in (parent.get_attribute("class") or ""):
                            break
                            
                        next_button.click()
                    
                    response = resp_info.value
                    page_data = response.json()
                    all_data.append(page_data)
                    page_num += 1
                    
                except TimeoutError:
                    break
                except Exception:
                    break
            
            return all_data, page_num

        # Process all dropdown combinations
        kisim_dropdown, kisim_options, kisim_count = get_dropdown_options(0)

        for i in range(kisim_count):
            kisim_dropdown, kisim_options, _ = get_dropdown_options(0)
            kisim_text = select_option(kisim_dropdown, kisim_options, i)
            if not kisim_text:
                continue

            bolum_dropdown, bolum_options, bolum_count = get_dropdown_options(1)
            for j in range(bolum_count):
                bolum_dropdown, bolum_options, _ = get_dropdown_options(1)
                bolum_text = select_option(bolum_dropdown, bolum_options, j)
                if not bolum_text:
                    continue

                grup_dropdown, grup_options, grup_count = get_dropdown_options(2)
                for k in range(grup_count):
                    grup_dropdown, grup_options, _ = get_dropdown_options(2)
                    grup_text = select_option(grup_dropdown, grup_options, k)
                    if not grup_text:
                        continue

                    sinif_dropdown, sinif_options, sinif_count = get_dropdown_options(3)
                    for l in range(sinif_count):
                        sinif_dropdown, sinif_options, _ = get_dropdown_options(3)
                        sinif_text = select_option(sinif_dropdown, sinif_options, l)
                        if not sinif_text:
                            continue

                        try:
                            # Get initial page data
                            with page.expect_response("**/nace-code-select-search", timeout=30000) as resp_info:
                                page.click(search_button_selector)
                            
                            initial_data = resp_info.value.json()
                            
                            # Get remaining pages
                            additional_data, total_pages = collect_all_pages_data()
                            
                            # Combine all data
                            all_data = [initial_data]
                            if additional_data:
                                all_data.extend(additional_data)
                            
                            # Add metadata
                            final_data = {
                                "metadata": {
                                    "kisim": kisim_text,
                                    "bolum": bolum_text,
                                    "grup": grup_text,
                                    "sinif": sinif_text,
                                    "timestamp": datetime.now().isoformat(),
                                    "total_pages": len(all_data)
                                },
                                "pages": all_data
                            }
                            
                            # Save JSON data
                            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                            filename = f"{output_dir}/data_{kisim_text}_{bolum_text}_{grup_text}_{sinif_text}_{timestamp}.json"
                            filename = filename.replace("/", "_").replace("\\", "_")
                            
                            with open(filename, 'w', encoding='utf-8') as f:
                                json.dump(final_data, f, ensure_ascii=False, indent=2)
                            
                            # Log search combination and page count
                            with open(log_file, 'a', encoding='utf-8') as f:
                                log_entry = (
                                    f"Timestamp: {datetime.now().isoformat()}\n"
                                    f"Search Combination:\n"
                                    f"  Kısım: {kisim_text}\n"
                                    f"  Bölüm: {bolum_text}\n"
                                    f"  Grup: {grup_text}\n"
                                    f"  Sınıf: {sinif_text}\n"
                                    f"Total Pages: {total_pages}\n"
                                    f"Output File: {os.path.basename(filename)}\n"
                                    f"{'-' * 50}\n"
                                )
                                f.write(log_entry)
                            
                        except Exception as e:
                            # Log errors in the same file
                            with open(log_file, 'a', encoding='utf-8') as f:
                                error_entry = (
                                    f"ERROR - Timestamp: {datetime.now().isoformat()}\n"
                                    f"Failed combination:\n"
                                    f"  Kısım: {kisim_text}\n"
                                    f"  Bölüm: {bolum_text}\n"
                                    f"  Grup: {grup_text}\n"
                                    f"  Sınıf: {sinif_text}\n"
                                    f"Error: {str(e)}\n"
                                    f"{'-' * 50}\n"
                                )
                                f.write(error_entry)

        browser.close()

if __name__ == "__main__":
    interact_with_all_dropdowns_and_capture_response()