#!/usr/bin/env python3
"""
OTPP Complete Pipeline - Seamless Execution
Runs the entire OTPP data processing workflow without errors
"""

import os
import sys
import time
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Optional

class SeamlessOTPPPipeline:
    """
    Seamless OTPP pipeline that handles all edge cases and errors gracefully
    """
    
    def __init__(self, working_dir: str = "./otpp_final"):
        """Initialize the seamless pipeline"""
        self.working_dir = Path(working_dir)
        self.downloads_dir = self.working_dir / "downloads"
        self.processed_dir = self.working_dir / "processed"
        self.output_dir = self.working_dir / "output"
        
        # Create all directories
        for directory in [self.working_dir, self.downloads_dir, self.processed_dir, self.output_dir]:
            directory.mkdir(exist_ok=True)
        
        print(f"[INIT] Pipeline initialized: {self.working_dir}")
    
    def step1_download_pdf(self) -> Optional[Path]:
        """Download PDF in a separate process to avoid browser issues"""
        print("\n" + "="*60)
        print("STEP 1: DOWNLOADING LATEST OTPP PDF")
        print("="*60)
        
        try:
            # Create a simple download script
            download_script = f'''
import sys
import os
sys.path.append(r"{os.getcwd()}")

from main import OTPPDownloader
import warnings
warnings.filterwarnings("ignore")

try:
    downloader = OTPPDownloader(download_dir=r"{self.downloads_dir}", headless=True)
    downloader.run()
    print("[DOWNLOAD_SUCCESS]")
except Exception as e:
    print(f"[DOWNLOAD_ERROR] {{e}}")
'''
            
            # Write temporary script
            temp_script = self.working_dir / "temp_download.py"
            temp_script.write_text(download_script)
            
            # Run in separate process
            print("[INFO] Starting download in background process...")
            result = subprocess.run([
                sys.executable, str(temp_script)
            ], capture_output=True, text=True, timeout=300)
            
            # Clean up
            temp_script.unlink()
            
            if "[DOWNLOAD_SUCCESS]" in result.stdout:
                pdf_files = list(self.downloads_dir.glob("*.pdf"))
                if pdf_files:
                    latest_pdf = max(pdf_files, key=lambda x: x.stat().st_mtime)
                    print(f"[SUCCESS] Downloaded: {latest_pdf.name}")
                    return latest_pdf
            
            print(f"[ERROR] Download failed: {result.stderr}")
            return None
            
        except subprocess.TimeoutExpired:
            print("[ERROR] Download timeout after 5 minutes")
            return None
        except Exception as e:
            print(f"[ERROR] Download process failed: {e}")
            return None
    
    def step2_convert_pdf(self, pdf_path: Path) -> Optional[Path]:
        """Convert PDF to Excel"""
        print("\n" + "="*60)
        print("STEP 2: CONVERTING PDF TO EXCEL")
        print("="*60)
        
        try:
            # Copy PDF to working directory temporarily
            temp_pdf = self.working_dir / pdf_path.name
            temp_pdf.write_bytes(pdf_path.read_bytes())
            
            # Import map module and run conversion
            original_dir = os.getcwd()
            os.chdir(self.working_dir)
            
            try:
                from map import main as convert_pdf
                convert_pdf()
                
                # Find the output file
                extracted_files = list(Path(".").glob("*_extracted.xlsx"))
                if extracted_files:
                    latest_excel = extracted_files[0]
                    # Move to processed directory using absolute paths
                    abs_source = latest_excel.resolve()
                    final_path = self.processed_dir / latest_excel.name
                    
                    # Ensure processed directory exists
                    self.processed_dir.mkdir(exist_ok=True)
                    
                    # Copy file content
                    final_path.write_bytes(abs_source.read_bytes())
                    abs_source.unlink()  # Remove original
                    
                    print(f"[SUCCESS] Converted: {final_path.name}")
                    return final_path
                else:
                    print("[ERROR] No extracted Excel file created")
                    return None
                    
            finally:
                os.chdir(original_dir)
                # Clean up temp PDF
                if temp_pdf.exists():
                    temp_pdf.unlink()
                    
        except Exception as e:
            print(f"[ERROR] PDF conversion failed: {e}")
            return None
    
    def step3_extract_data(self, excel_path: Path) -> Optional[Path]:
        """Extract hierarchical data"""
        print("\n" + "="*60)
        print("STEP 3: EXTRACTING STRUCTURED DATA")
        print("="*60)
        
        try:
            from excel import OTTPHierarchicalExtractor
            
            # Initialize extractor
            extractor = OTTPHierarchicalExtractor()
            
            # Get absolute path
            abs_path = excel_path.resolve()
            print(f"[INFO] Processing: {excel_path.name}")
            
            # Validate file
            if not extractor.validate_source_file(str(abs_path)):
                print("[ERROR] File validation failed")
                return None
            
            print("[OK] File validation passed")
            
            # Extract data
            extracted_data = extractor.extract_data(str(abs_path))
            if not extracted_data:
                print("[ERROR] No data extracted")
                return None
            
            print(f"[OK] Extracted {len(extracted_data)} data points")
            
            # Create output
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = self.output_dir / f"CANF_OTPP_DATA_{timestamp}.xlsx"
            
            if extractor.create_output_file(extracted_data, str(output_file)):
                print(f"[SUCCESS] Created: {output_file.name}")
                return output_file
            else:
                print("[ERROR] Failed to create output file")
                return None
                
        except Exception as e:
            print(f"[ERROR] Data extraction failed: {e}")
            return None
    
    def run_pipeline(self, skip_download: bool = False, source_pdf: Optional[str] = None) -> bool:
        """Run the complete pipeline"""
        print("="*80)
        print("SEAMLESS OTPP DATA PIPELINE")
        print("="*80)
        print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # Step 1: Get PDF
            if skip_download and source_pdf:
                pdf_path = Path(source_pdf)
                if not pdf_path.exists():
                    print(f"[ERROR] Source PDF not found: {source_pdf}")
                    return False
                print(f"[INFO] Using provided PDF: {pdf_path.name}")
            else:
                pdf_path = self.step1_download_pdf()
                if not pdf_path:
                    print("[FATAL] PDF acquisition failed")
                    return False
            
            # Step 2: Convert PDF
            excel_path = self.step2_convert_pdf(pdf_path)
            if not excel_path:
                print("[FATAL] PDF conversion failed")
                return False
            
            # Step 3: Extract data
            final_output = self.step3_extract_data(excel_path)
            if not final_output:
                print("[FATAL] Data extraction failed")
                return False
            
            # Success summary
            print("\n" + "="*80)
            print("PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*80)
            print(f"Final output: {final_output}")
            print(f"File size: {final_output.stat().st_size / 1024:.1f} KB")
            print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            return True
            
        except Exception as e:
            print(f"\n[FATAL] Pipeline failed: {e}")
            return False

def main():
    """Main function"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Seamless OTPP Data Pipeline")
    parser.add_argument("--skip-download", action="store_true", help="Skip PDF download")
    parser.add_argument("--source-pdf", help="Use specific PDF file")
    parser.add_argument("--working-dir", default="./otpp_final", help="Working directory")
    
    args = parser.parse_args()
    
    # Run pipeline
    pipeline = SeamlessOTPPPipeline(working_dir=args.working_dir)
    success = pipeline.run_pipeline(
        skip_download=args.skip_download,
        source_pdf=args.source_pdf
    )
    
    if success:
        print("\n[RESULT] Pipeline executed successfully! All data extracted.")
    else:
        print("\n[RESULT] Pipeline failed. Check error messages above.")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())