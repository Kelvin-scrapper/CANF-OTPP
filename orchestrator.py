#!/usr/bin/env python3
"""
OTPP Data Processing Orchestrator
Coordinates the complete workflow from PDF download to Excel data extraction
"""

import os
import sys
import time
import glob
from pathlib import Path
from datetime import datetime
from typing import List, Optional

# Import our modules
from main import OTPPDownloader
from map import main as pdf_to_excel_converter
from excel import OTTPHierarchicalExtractor

class OTPPOrchestrator:
    """
    Main orchestrator class that coordinates the entire OTPP data processing pipeline
    """
    
    def __init__(self, working_dir: str = "./otpp_processing"):
        """
        Initialize the orchestrator
        
        Args:
            working_dir (str): Directory for all processing files
        """
        self.working_dir = Path(working_dir)
        self.downloads_dir = self.working_dir / "downloads"
        self.processed_dir = self.working_dir / "processed"
        self.output_dir = self.working_dir / "output"
        
        # Create directories
        for directory in [self.working_dir, self.downloads_dir, self.processed_dir, self.output_dir]:
            directory.mkdir(exist_ok=True)
            
        print(f"[ORCHESTRATOR] Initialized with working directory: {self.working_dir}")
    
    def step1_download_pdf(self) -> bool:
        """
        Step 1: Download the latest OTPP PDF using main.py
        
        Returns:
            bool: Success status
        """
        print("\n" + "="*80)
        print("STEP 1: DOWNLOADING LATEST OTPP PDF")
        print("="*80)
        
        try:
            # Create downloader instance with our downloads directory in headless mode
            downloader = OTPPDownloader(download_dir=str(self.downloads_dir))
            
            # Run the download process in background (headless mode)
            downloader.run(headless=True)
            
            # Check if any PDF files were downloaded
            pdf_files = list(self.downloads_dir.glob("*.pdf"))
            
            if pdf_files:
                latest_pdf = max(pdf_files, key=lambda x: x.stat().st_mtime)
                print(f"[SUCCESS] Downloaded PDF: {latest_pdf.name}")
                return True
            else:
                print("[ERROR] No PDF files found after download attempt")
                return False
                
        except Exception as e:
            print(f"[ERROR] PDF download failed: {e}")
            return False
    
    def step2_convert_pdf_to_excel(self) -> Optional[Path]:
        """
        Step 2: Convert PDF to Excel using map.py
        
        Returns:
            Optional[Path]: Path to the extracted Excel file, or None if failed
        """
        print("\n" + "="*80)
        print("STEP 2: CONVERTING PDF TO EXCEL")
        print("="*80)
        
        try:
            # Change to downloads directory for processing
            original_dir = os.getcwd()
            os.chdir(self.downloads_dir)
            
            # Run the PDF to Excel conversion
            pdf_to_excel_converter()
            
            # Return to original directory
            os.chdir(original_dir)
            
            # Find the extracted Excel file
            extracted_files = list(self.downloads_dir.glob("*_extracted.xlsx"))
            
            if extracted_files:
                latest_excel = max(extracted_files, key=lambda x: x.stat().st_mtime)
                
                # Move to processed directory
                processed_file = self.processed_dir / latest_excel.name
                latest_excel.rename(processed_file)
                
                print(f"[SUCCESS] Converted PDF to Excel: {processed_file.name}")
                return processed_file
            else:
                print("[ERROR] No extracted Excel files found")
                return None
                
        except Exception as e:
            print(f"[ERROR] PDF to Excel conversion failed: {e}")
            # Make sure we return to original directory
            try:
                os.chdir(original_dir)
            except:
                pass
            return None
    
    def step3_extract_hierarchical_data(self, source_excel_path: Path) -> Optional[Path]:
        """
        Step 3: Extract hierarchical data using excel.py
        
        Args:
            source_excel_path (Path): Path to the source Excel file
            
        Returns:
            Optional[Path]: Path to the final output file, or None if failed
        """
        print("\n" + "="*80)
        print("STEP 3: EXTRACTING HIERARCHICAL DATA")
        print("="*80)
        
        try:
            # Work with absolute paths - don't change directories
            original_dir = os.getcwd()
            
            # Initialize the extractor
            extractor = OTTPHierarchicalExtractor()
            
            # Ensure we have absolute path
            abs_source_path = source_excel_path.resolve()
            print(f"[INFO] Validating source file: {source_excel_path.name}")
            print(f"[DEBUG] Full path: {abs_source_path}")
            
            if not extractor.validate_source_file(str(abs_source_path)):
                print("[ERROR] Source file validation failed")
                return None
            
            # Extract the data
            print(f"[INFO] Extracting data from: {source_excel_path.name}")
            extracted_data = extractor.extract_data(str(abs_source_path))
            
            if not extracted_data:
                print("[ERROR] No data was extracted")
                return None
            
            # Create output file
            timestamp = datetime.now().strftime('%Y%m%d')
            output_filename = f"CANF_OTPP_DATA_{timestamp}.xlsx"
            output_path = self.output_dir / output_filename
            
            # Generate the final output with absolute path
            abs_output_path = output_path.resolve()
            if extractor.create_output_file(extracted_data, str(abs_output_path)):
                print(f"[SUCCESS] Created final output: {output_path.name}")
                print(f"[INFO] Extracted {len(extracted_data)} data points")
                return output_path
            else:
                print("[ERROR] Failed to create output file")
                return None
                
        except Exception as e:
            print(f"[ERROR] Data extraction failed: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def cleanup_intermediate_files(self):
        """
        Clean up intermediate files to save space
        """
        print("\n" + "="*40)
        print("CLEANING UP INTERMEDIATE FILES")
        print("="*40)
        
        try:
            # Clean up downloads directory (keep only the latest PDF)
            pdf_files = list(self.downloads_dir.glob("*.pdf"))
            if len(pdf_files) > 1:
                # Keep the newest, remove others
                latest_pdf = max(pdf_files, key=lambda x: x.stat().st_mtime)
                for pdf_file in pdf_files:
                    if pdf_file != latest_pdf:
                        pdf_file.unlink()
                        print(f"[CLEANUP] Removed old PDF: {pdf_file.name}")
            
            # Clean up any temporary files
            temp_files = list(self.downloads_dir.glob("*_temp.*"))
            for temp_file in temp_files:
                temp_file.unlink()
                print(f"[CLEANUP] Removed temp file: {temp_file.name}")
                
            print("[CLEANUP] Cleanup completed")
            
        except Exception as e:
            print(f"[CLEANUP] Warning: Cleanup failed: {e}")
    
    def generate_summary_report(self, final_output: Optional[Path]):
        """
        Generate a summary report of the processing
        
        Args:
            final_output (Optional[Path]): Path to the final output file
        """
        print("\n" + "="*80)
        print("PROCESSING SUMMARY REPORT")
        print("="*80)
        
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        print(f"Processing completed at: {timestamp}")
        print(f"Working directory: {self.working_dir}")
        
        # Count files in each directory
        pdf_count = len(list(self.downloads_dir.glob("*.pdf")))
        processed_count = len(list(self.processed_dir.glob("*.xlsx")))
        output_count = len(list(self.output_dir.glob("*.xlsx")))
        
        print(f"\nFile counts:")
        print(f"  - Downloaded PDFs: {pdf_count}")
        print(f"  - Processed Excel files: {processed_count}")
        print(f"  - Final outputs: {output_count}")
        
        if final_output and final_output.exists():
            file_size = final_output.stat().st_size / 1024  # KB
            print(f"\nFinal output:")
            print(f"  - File: {final_output.name}")
            print(f"  - Size: {file_size:.1f} KB")
            print(f"  - Path: {final_output}")
            print(f"  - Status: [SUCCESS]")
        else:
            print(f"\nFinal output:")
            print(f"  - Status: [FAILED]")
        
        print("\n" + "="*80)
    
    def run_complete_pipeline(self, skip_download: bool = False, source_pdf: Optional[str] = None):
        """
        Run the complete OTPP data processing pipeline
        
        Args:
            skip_download (bool): Skip the download step if True
            source_pdf (Optional[str]): Use specific PDF file instead of downloading
        """
        print("="*80)
        print("OTPP DATA PROCESSING ORCHESTRATOR")
        print("="*80)
        print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        final_output = None
        
        try:
            # Step 1: Download PDF (unless skipped)
            if skip_download:
                print("\n[INFO] Skipping PDF download as requested")
                if source_pdf:
                    # Copy specified PDF to downloads directory
                    source_path = Path(source_pdf)
                    if source_path.exists():
                        dest_path = self.downloads_dir / source_path.name
                        dest_path.write_bytes(source_path.read_bytes())
                        print(f"[INFO] Using provided PDF: {source_path.name}")
                    else:
                        print(f"[ERROR] Specified PDF not found: {source_pdf}")
                        return
            else:
                if not self.step1_download_pdf():
                    print("[FATAL] PDF download failed, aborting pipeline")
                    return
            
            # Step 2: Convert PDF to Excel
            extracted_excel = self.step2_convert_pdf_to_excel()
            if not extracted_excel:
                print("[FATAL] PDF to Excel conversion failed, aborting pipeline")
                return
            
            # Step 3: Extract hierarchical data
            final_output = self.step3_extract_hierarchical_data(extracted_excel)
            if not final_output:
                print("[FATAL] Data extraction failed, aborting pipeline")
                return
            
            # Clean up intermediate files
            self.cleanup_intermediate_files()
            
            print("\n" + "="*80)
            print("PIPELINE COMPLETED SUCCESSFULLY!")
            print("="*80)
            
        except Exception as e:
            print(f"\n[FATAL ERROR] Pipeline failed: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            # Always generate summary report
            self.generate_summary_report(final_output)

def main():
    """
    Main function with command line argument support
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="OTPP Data Processing Orchestrator")
    parser.add_argument("--working-dir", default="./otpp_processing", 
                       help="Working directory for processing (default: ./otpp_processing)")
    parser.add_argument("--skip-download", action="store_true",
                       help="Skip PDF download step")
    parser.add_argument("--source-pdf", 
                       help="Use specific PDF file instead of downloading")
    
    args = parser.parse_args()
    
    # Create and run orchestrator
    orchestrator = OTPPOrchestrator(working_dir=args.working_dir)
    orchestrator.run_complete_pipeline(
        skip_download=args.skip_download,
        source_pdf=args.source_pdf
    )

if __name__ == "__main__":
    main()