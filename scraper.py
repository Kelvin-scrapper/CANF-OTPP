"""
CANF_OTPP scraper — downloads the latest OTPP annual/interim PDF report
and parses the NOTE 2 investments schedule into a raw DataFrame.

URL: https://www.otpp.com/en-ca/about-us/our-results/report-archive/

Usage:
    from scraper import fetch_data
    raw_df, report_info = fetch_data("downloads")
"""

import os
import re
import glob
import time
import requests
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import fitz  # PyMuPDF

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import undetected_chromedriver as uc


# ── Downloader (original OTPPDownloader logic) ────────────────────────────────

class OTPPDownloader:
    def __init__(self, download_dir="./downloads", headless=True):
        self.base_url = "https://www.otpp.com/en-ca/about-us/our-results/report-archive/"
        self.download_dir = os.path.abspath(download_dir)
        self.driver = None
        self.headless = headless
        os.makedirs(self.download_dir, exist_ok=True)

    def setup_driver(self):
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "plugins.always_open_pdf_externally": True,
        }
        chrome_options = uc.ChromeOptions()
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")

        if self.headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            print(f"[OK] Running in headless mode (background)")

        try:
            self.driver = uc.Chrome(options=chrome_options, version_main=None)
            time.sleep(2)
            print(f"[OK] Chrome driver initialized")
            print(f"[OK] Download directory: {self.download_dir}")
        except Exception as e:
            print(f"Failed to initialize with options, trying basic setup: {e}")
            try:
                self.driver = uc.Chrome(version_main=None)
                print(f"[OK] Chrome driver initialized (basic mode)")
                print(f"[OK] Download directory: {self.download_dir}")
            except Exception as e2:
                raise Exception(f"Failed to initialize Chrome driver: {e2}")

    def navigate_to_reports(self):
        try:
            print(f"Navigating to: {self.base_url}")
            self.driver.get(self.base_url)
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            print("[OK] Successfully loaded reports archive page")
            time.sleep(2)
        except TimeoutException:
            raise Exception("Failed to load the reports archive page")

    def _collect_report_candidates(self):
        current_year = datetime.now().year
        candidates = []

        interim_selectors = [
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'interim financials')]",
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'interim financial statements')]",
            "//a[contains(@href, 'interim-financial')]",
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'interim') and contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'financial')]",
        ]
        annual_selectors = [
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'annual report')]",
            "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'annual financial statements')]",
            "//a[contains(@href, 'annual-report')]",
            "//a[contains(@href, 'annual-financial')]",
        ]

        def scan(selectors, report_type):
            found = []
            for selector in selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    for elem in elements:
                        href = elem.get_attribute('href') or ''
                        if not href or not (href.lower().endswith('.pdf') or 'pdf' in href.lower()):
                            continue
                        text = elem.text.strip()
                        match = re.search(r'20\d{2}', text + ' ' + href)
                        year = int(match.group()) if match else current_year
                        found.append((elem, report_type, year, href))
                    if found:
                        break
                except Exception:
                    continue
            candidates.extend(found)

        scan(interim_selectors, 'interim')
        scan(annual_selectors, 'annual')

        if not candidates:
            print("[WARN] No labelled report links found, falling back to generic PDF search...")
            for elem in self.driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")[:10]:
                href = elem.get_attribute('href') or ''
                text = elem.text.strip()
                match = re.search(r'20\d{2}', text + ' ' + href)
                year = int(match.group()) if match else current_year
                rtype = 'annual' if 'annual' in (text + href).lower() else 'interim'
                candidates.append((elem, rtype, year, href))

        return candidates

    def find_and_download_latest_report(self):
        try:
            candidates = self._collect_report_candidates()
            if not candidates:
                raise Exception("Could not find any report links on the page")

            candidates.sort(key=lambda x: (x[2], 1 if x[1] == 'annual' else 0), reverse=True)
            link_elem, report_type, report_year, download_url = candidates[0]
            print(f"[OK] Selected {report_type} report for {report_year}: {link_elem.text.strip() or download_url}")
            print(f"Download URL: {download_url}")

            self.download_file_directly(download_url)
            return {'type': report_type, 'year': report_year}

        except Exception as e:
            print(f"Error finding/downloading report: {e}")
            return None

    def download_file_directly(self, url):
        try:
            print("Attempting direct download...")
            cookies = {}
            for cookie in self.driver.get_cookies():
                cookies[cookie['name']] = cookie['value']

            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            response = requests.get(url, cookies=cookies, headers=headers, stream=True)
            response.raise_for_status()

            filename = url.split('/')[-1]
            if 'Content-Disposition' in response.headers:
                content_disp = response.headers['Content-Disposition']
                if 'filename=' in content_disp:
                    filename = content_disp.split('filename=')[1].strip('"')

            timestamp = datetime.now().strftime("%Y%m%d")
            if not filename.startswith('CANF_OTPP_'):
                base_name = filename.rsplit('.', 1)[0] if '.' in filename else filename
                extension = filename.rsplit('.', 1)[1] if '.' in filename else 'pdf'
                filename = f"CANF_OTPP_DATA_{timestamp}_{base_name}.{extension}"

            filepath = os.path.join(self.download_dir, filename)
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            print(f"[OK] File downloaded successfully: {filepath}")
            print(f"[OK] File size: {os.path.getsize(filepath)} bytes")

        except Exception as e:
            print(f"Direct download failed: {e}")

    def wait_for_download_completion(self, timeout=60):
        start_time = time.time()
        while time.time() - start_time < timeout:
            crdownload_files = [f for f in os.listdir(self.download_dir) if f.endswith('.crdownload')]
            if not crdownload_files:
                pdf_files = [f for f in os.listdir(self.download_dir) if f.endswith('.pdf')]
                if pdf_files:
                    print(f"[OK] Download completed: {pdf_files[-1]}")
                    return True
            time.sleep(1)
        print("Download timeout reached")
        return False

    def run(self, headless=None):
        if headless is not None:
            self.headless = headless

        report_info = None
        try:
            print("=== OTPP PDF Downloader Started ===")
            self.setup_driver()
            self.navigate_to_reports()
            report_info = self.find_and_download_latest_report()

            if report_info:
                self.wait_for_download_completion()
                print(f"=== Download Process Completed ({report_info['type'].title()} {report_info['year']}) ===")
            else:
                print("=== Download Process Failed ===")

        except Exception as e:
            print(f"Error during execution: {e}")

        finally:
            if self.driver:
                try:
                    for handle in self.driver.window_handles:
                        self.driver.switch_to.window(handle)
                        self.driver.close()
                    self.driver.quit()
                    print("[OK] Browser closed")
                except Exception:
                    try:
                        self.driver.service.stop()
                        print("[OK] Browser force closed")
                    except Exception:
                        print("[WARN] Browser cleanup had issues, but continuing")
                finally:
                    self.driver = None

        return report_info


# ── PDF parsing (original map.py logic) ──────────────────────────────────────

def find_schedule_pages(pdf_path: str) -> Tuple[int, int]:
    """Locate the 0-indexed page range containing the NOTE 2 investments table."""
    try:
        doc = fitz.open(pdf_path)
        total_pages = len(doc)

        for page_num in range(total_pages):
            text = doc[page_num].get_text("text")
            if "NOTE 2." in text and "(Canadian $ millions)" in text:
                doc.close()
                print(f"  - Found financial schedule (NOTE 2) on PDF page {page_num + 1}")
                return page_num, page_num + 4

        for page_num in range(total_pages):
            text = doc[page_num].get_text("text")
            if ("Equity" in text and "Fixed income" in text
                    and "fair value" in text.lower() and "Canadian" in text):
                doc.close()
                print(f"  - Found investments table on PDF page {page_num + 1} (fallback marker)")
                return page_num, page_num + 4

        doc.close()
        print("  - [WARN] Could not locate financial schedule -- defaulting to pages 6-9")
        return 5, 9

    except Exception as e:
        print(f"  - [WARN] Page detection error: {e} -- defaulting to pages 6-9")
        return 5, 9


_CTRL = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]')
_ILLEGAL_XML_CHARS = re.compile(r'[\x00-\x08\x0b\x0c\x0e-\x1f\ufffe\uffff]')


def parse_table(pdf_path: str, start_page: int, end_page: int) -> List[List]:
    """Parse the investments schedule from PDF pages using fitz text extraction.

    Produces one list per row: [label, fv_current, cost_current, fv_prior, cost_prior].
    Header rows have None for all value columns.
    Stops when the (b) Fair value hierarchy section begins.
    """
    doc = fitz.open(pdf_path)

    all_lines = []
    for page_num in range(start_page, min(end_page, len(doc))):
        raw = doc[page_num].get_text("text")
        for line in raw.split('\n'):
            cleaned = _CTRL.sub('', line).strip().strip('\xa0').strip()
            if cleaned:
                all_lines.append(cleaned)
    doc.close()

    SKIP_EXACT = {'$', 'Fair Value', 'Cost', '2025', '2024',
                  '(Canadian $ millions)', 'MTT_ATT'}
    SKIP_CONTAINS = [
        'ONTARIO TEACHERS', 'NOTE 2.', '(a) Investments',
        'The schedule below', "Ontario Teachers' invests",
        'including net accrued', 'Real estate is presented',
    ]
    STOP_CONTAINS = ['(b) Fair value hierarchy', 'Fair value hierarchy']

    def should_skip(line: str) -> bool:
        if line in SKIP_EXACT:
            return True
        for p in SKIP_CONTAINS:
            if p in line:
                return True
        return False

    def is_number(s: str) -> bool:
        s = s.replace(',', '').replace(' ', '')
        if s in ('\u2013', '\u2014', '-', '(\u2013)', '(\u2014)', '(-)'):
            return True
        if s.startswith('$'):
            s = s[1:]
        if s.startswith('(') and s.endswith(')'):
            s = s[1:-1]
        try:
            float(s)
            return True
        except ValueError:
            return False

    def parse_num(s: str) -> float:
        s = s.replace(',', '').strip()
        if not s or s in ('\u2013', '\u2014', '-'):
            return 0.0
        if s.startswith('$'):
            s = s[1:].strip()
        if s.startswith('(') and s.endswith(')'):
            inner = s[1:-1].replace(',', '').strip()
            try:
                return -float(inner)
            except ValueError:
                return 0.0
        try:
            return float(s)
        except ValueError:
            return 0.0

    def clean_label(s: str) -> str:
        s = re.sub(r'\s*\(NOTE \d+[a-z]?\)', '', s)
        s = re.sub(r'\d+$', '', s)
        return s.strip()

    rows: List[List] = []
    current_label: Optional[str] = None
    value_buffer: List[float] = []
    in_table = False
    skip_n = 0
    skip_next_digit = False

    for line in all_lines:
        if any(stop in line for stop in STOP_CONTAINS):
            break

        if not in_table:
            if 'As at December 31 (Canadian $ millions)' in line:
                in_table = True
                skip_n = 4
            continue

        if 'As at December 31 (Canadian $ millions)' in line:
            if current_label is not None and not value_buffer:
                rows.append([current_label, None, None, None, None])
                current_label = None
            skip_n = 4
            continue

        if skip_n > 0:
            if line in ('Fair Value', 'Cost', '2025', '2024'):
                skip_n -= 1
            continue

        if skip_next_digit:
            skip_next_digit = False
            if line.isdigit():
                continue

        if should_skip(line):
            if 'ONTARIO TEACHERS' in line:
                skip_next_digit = True
            continue

        if is_number(line):
            value_buffer.append(parse_num(line))
            if len(value_buffer) == 4:
                label = current_label if current_label is not None else ''
                rows.append([label] + value_buffer)
                current_label = None
                value_buffer = []
        else:
            if value_buffer:
                while len(value_buffer) < 4:
                    value_buffer.append(None)
                rows.append([current_label if current_label is not None else ''] + value_buffer)
                value_buffer = []
                current_label = clean_label(line)
            elif current_label is not None and line and line[0].islower():
                current_label = current_label + ' ' + line
            else:
                if current_label is not None:
                    rows.append([current_label, None, None, None, None])
                current_label = clean_label(line)

    if current_label is not None:
        if value_buffer:
            while len(value_buffer) < 4:
                value_buffer.append(None)
            rows.append([current_label] + value_buffer)
        else:
            rows.append([current_label, None, None, None, None])

    return rows


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_data(downloads_dir: str = "downloads") -> Tuple[pd.DataFrame, dict]:
    """Download latest OTPP PDF and parse the NOTE 2 investments table.

    Returns:
        raw_df      : DataFrame columns [label, fv_current, cost_current, fv_prior, cost_prior]
        report_info : {'type': 'annual'|'interim', 'year': int}
    """
    downloader = OTPPDownloader(download_dir=downloads_dir, headless=True)
    report_info = downloader.run()
    if not report_info:
        raise RuntimeError("PDF download failed")

    pdf_files = sorted(Path(downloads_dir).glob("*.pdf"), key=lambda x: x.stat().st_mtime)
    if not pdf_files:
        raise RuntimeError("No PDF found in downloads directory after download")
    pdf_path = str(pdf_files[-1])

    start, end = find_schedule_pages(pdf_path)
    print(f"  - Parsing PDF pages {start + 1}-{end} ...")
    rows = parse_table(pdf_path, start, end)
    print(f"  - Parsed {len(rows)} rows")

    raw_df = pd.DataFrame(rows, columns=['label', 'fv_current', 'cost_current', 'fv_prior', 'cost_prior'])
    return raw_df, report_info
