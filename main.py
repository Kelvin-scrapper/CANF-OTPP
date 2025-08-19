#!/usr/bin/env python3
"""
OTPP PDF Downloader Script
Downloads the latest interim financial statements from Ontario Teachers' Pension Plan
Based on the CANF_OTPP runbook requirements
"""

import os
import time
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import undetected_chromedriver as uc

class OTPPDownloader:
    def __init__(self, download_dir="./downloads", headless=False):
        """
        Initialize the OTPP downloader
        
        Args:
            download_dir (str): Directory to save downloaded files
            headless (bool): Run browser in headless mode (background)
        """
        self.base_url = "https://www.otpp.com/en-ca/about-us/our-results/report-archive/"
        self.download_dir = os.path.abspath(download_dir)
        self.driver = None
        self.headless = headless
        
        # Create download directory if it doesn't exist
        os.makedirs(self.download_dir, exist_ok=True)
        
    def setup_driver(self):
        """Setup undetected Chrome driver with download preferences"""
        
        # Set download preferences
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "plugins.always_open_pdf_externally": True  # Don't open PDF in browser
        }
        
        # Chrome options - use uc.ChromeOptions() instead of Options()
        chrome_options = uc.ChromeOptions()
        chrome_options.add_experimental_option("prefs", prefs)
        
        # Basic arguments that work with undetected_chromedriver
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        
        # Add headless mode if requested
        if self.headless:
            chrome_options.add_argument("--headless")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            print(f"[OK] Running in headless mode (background)")
        
        try:
            # Initialize undetected chrome driver with simpler options
            self.driver = uc.Chrome(options=chrome_options, version_main=None)
            
            # Wait a moment for driver to initialize
            time.sleep(2)
            
            print(f"[OK] Chrome driver initialized")
            print(f"[OK] Download directory: {self.download_dir}")
            
        except Exception as e:
            print(f"Failed to initialize with options, trying basic setup: {e}")
            # Fallback: try with minimal options
            try:
                self.driver = uc.Chrome(version_main=None)
                print(f"[OK] Chrome driver initialized (basic mode)")
                print(f"[OK] Download directory: {self.download_dir}")
            except Exception as e2:
                raise Exception(f"Failed to initialize Chrome driver: {e2}")
        
    def navigate_to_reports(self):
        """Navigate to the OTPP reports archive page"""
        try:
            print(f"Navigating to: {self.base_url}")
            self.driver.get(self.base_url)
            
            # Wait for page to load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            print("[OK] Successfully loaded reports archive page")
            time.sleep(2)  # Additional wait for dynamic content
            
        except TimeoutException:
            raise Exception("Failed to load the reports archive page")
    
    def find_and_download_latest_interim_report(self):
        """Find and download the latest interim financial statements"""
        try:
            # Look for links containing "interim" and "financial" (case insensitive)
            possible_selectors = [
                "//a[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'interim') and contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'financial')]",
                "//a[contains(@href, 'interim') and contains(@href, 'financial')]",
                "//a[contains(@href, 'interim-financial-statements')]",
                "//a[contains(text(), 'Interim Financials')]",
                "//a[contains(@title, 'interim') and contains(@title, 'financial')]"
            ]
            
            interim_link = None
            
            # Try each selector until we find a match
            for selector in possible_selectors:
                try:
                    elements = self.driver.find_elements(By.XPATH, selector)
                    if elements:
                        # Get the first (most recent) interim report
                        interim_link = elements[0]
                        print(f"[OK] Found interim report link: {interim_link.text.strip()}")
                        break
                except NoSuchElementException:
                    continue
            
            if not interim_link:
                # If no specific interim link found, look for any PDF links with recent dates
                print("Looking for recent PDF reports...")
                current_year = datetime.now().year
                pdf_links = self.driver.find_elements(By.XPATH, f"//a[contains(@href, '.pdf') and contains(@href, '{current_year}')]")
                
                if pdf_links:
                    interim_link = pdf_links[0]
                    print(f"[OK] Found recent PDF report: {interim_link.get_attribute('href')}")
            
            if not interim_link:
                raise Exception("Could not find interim financial statements link")
            
            # Get the download URL
            download_url = interim_link.get_attribute('href')
            print(f"Download URL: {download_url}")
            
            # Click the link to download
            print("Clicking download link...")
            self.driver.execute_script("arguments[0].click();", interim_link)
            
            # Wait for download to start
            time.sleep(3)
            
            # Alternatively, use requests to download directly
            self.download_file_directly(download_url)
            
            return True
            
        except Exception as e:
            print(f"Error finding/downloading interim report: {e}")
            return False
    
    def download_file_directly(self, url):
        """Download file directly using requests as backup method"""
        try:
            print("Attempting direct download...")
            
            # Get cookies from selenium session
            cookies = {}
            for cookie in self.driver.get_cookies():
                cookies[cookie['name']] = cookie['value']
            
            # Set headers to mimic browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            
            # Download the file
            response = requests.get(url, cookies=cookies, headers=headers, stream=True)
            response.raise_for_status()
            
            # Extract filename from URL or Content-Disposition header
            filename = url.split('/')[-1]
            if 'Content-Disposition' in response.headers:
                content_disp = response.headers['Content-Disposition']
                if 'filename=' in content_disp:
                    filename = content_disp.split('filename=')[1].strip('"')
            
            # Generate timestamped filename
            timestamp = datetime.now().strftime("%Y%m%d")
            if not filename.startswith('CANF_OTPP_'):
                base_name = filename.rsplit('.', 1)[0] if '.' in filename else filename
                extension = filename.rsplit('.', 1)[1] if '.' in filename else 'pdf'
                filename = f"CANF_OTPP_DATA_{timestamp}_{base_name}.{extension}"
            
            filepath = os.path.join(self.download_dir, filename)
            
            # Save the file
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"[OK] File downloaded successfully: {filepath}")
            print(f"[OK] File size: {os.path.getsize(filepath)} bytes")
            
        except Exception as e:
            print(f"Direct download failed: {e}")
    
    def wait_for_download_completion(self, timeout=60):
        """Wait for download to complete by checking for .crdownload files"""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            # Check for .crdownload files (Chrome partial downloads)
            crdownload_files = [f for f in os.listdir(self.download_dir) if f.endswith('.crdownload')]
            
            if not crdownload_files:
                # No partial downloads, check if we have new PDF files
                pdf_files = [f for f in os.listdir(self.download_dir) if f.endswith('.pdf')]
                if pdf_files:
                    print(f"[OK] Download completed: {pdf_files[-1]}")
                    return True
            
            time.sleep(1)
        
        print("Download timeout reached")
        return False
    
    def run(self, headless=None):
        """Main execution method"""
        # Override headless setting if provided
        if headless is not None:
            self.headless = headless
            
        try:
            print("=== OTPP PDF Downloader Started ===")
            
            # Setup driver
            self.setup_driver()
            
            # Navigate to reports page
            self.navigate_to_reports()
            
            # Find and download the latest interim report
            success = self.find_and_download_latest_interim_report()
            
            if success:
                # Wait for download completion
                self.wait_for_download_completion()
                print("=== Download Process Completed ===")
            else:
                print("=== Download Process Failed ===")
                
        except Exception as e:
            print(f"Error during execution: {e}")
            
        finally:
            # Improved cleanup
            if self.driver:
                try:
                    # Close all windows first
                    for handle in self.driver.window_handles:
                        self.driver.switch_to.window(handle)
                        self.driver.close()
                    
                    # Then quit the driver
                    self.driver.quit()
                    print("[OK] Browser closed")
                except Exception as e:
                    # Force kill if needed
                    try:
                        self.driver.service.stop()
                        print("[OK] Browser force closed")
                    except:
                        print("[WARN] Browser cleanup had issues, but continuing")
                    pass
                finally:
                    self.driver = None

def main():
    """Main function to run the downloader"""
    # Create downloader instance
    downloader = OTPPDownloader(download_dir="./otpp_downloads")
    
    # Run the download process
    downloader.run()

if __name__ == "__main__":
    main()