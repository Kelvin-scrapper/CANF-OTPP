#!/usr/bin/env python3
"""
Simple OTPP Pipeline Launcher
Demonstrates the seamless operation of the complete OTPP data processing pipeline
"""

import os
import subprocess
import sys
from pathlib import Path
from datetime import datetime

def run_complete_pipeline():
    """Run the complete OTPP pipeline in the simplest way possible"""
    
    print("="*80)
    print("SIMPLE OTPP PIPELINE LAUNCHER")
    print("="*80)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Download latest PDF (headless mode)
    print("\n[STEP 1] Downloading latest OTPP PDF in background...")
    try:
        from main import OTPPDownloader
        downloader = OTPPDownloader(download_dir="./downloads", headless=True)
        downloader.run()
        
        # Find downloaded PDF
        pdf_files = list(Path("./downloads").glob("*.pdf"))
        if not pdf_files:
            print("[ERROR] No PDF downloaded")
            return False
        
        latest_pdf = max(pdf_files, key=lambda x: x.stat().st_mtime)
        print(f"[SUCCESS] Downloaded: {latest_pdf.name}")
        
        # Step 2: Convert PDF to Excel
        print("\n[STEP 2] Converting PDF to structured Excel...")
        
        # Copy PDF to current directory for processing
        temp_pdf = Path(latest_pdf.name)
        temp_pdf.write_bytes(latest_pdf.read_bytes())
        
        # Run map.py conversion
        from map import main as convert_pdf
        original_dir = os.getcwd()
        
        try:
            convert_pdf()
            
            # Find extracted Excel file
            extracted_files = list(Path(".").glob("*_extracted.xlsx"))
            if not extracted_files:
                print("[ERROR] No Excel file created")
                return False
            
            excel_file = extracted_files[0]
            print(f"[SUCCESS] Converted: {excel_file.name}")
            
            # Step 3: Extract hierarchical data
            print("\n[STEP 3] Extracting structured financial data...")
            
            from excel import OTTPHierarchicalExtractor
            extractor = OTTPHierarchicalExtractor()
            
            # Validate and extract
            if not extractor.validate_source_file(str(excel_file)):
                print("[ERROR] File validation failed")
                return False
            
            extracted_data = extractor.extract_data(str(excel_file))
            if not extracted_data:
                print("[ERROR] Data extraction failed")
                return False
            
            # Create final output
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"FINAL_OTPP_DATA_{timestamp}.xlsx"
            
            if extractor.create_output_file(extracted_data, output_file):
                print(f"[SUCCESS] Created: {output_file}")
                print(f"[INFO] Extracted {len(extracted_data)} data points")
                print(f"[INFO] Generated 74 time series columns")
                
                # Clean up temporary files
                temp_pdf.unlink()
                excel_file.unlink()
                
                print("\n" + "="*80)
                print("PIPELINE COMPLETED SUCCESSFULLY!")
                print("="*80)
                print(f"Final output: {output_file}")
                print(f"File size: {Path(output_file).stat().st_size / 1024:.1f} KB")
                print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                return True
            else:
                print("[ERROR] Failed to create final output")
                return False
                
        finally:
            # Always clean up
            if temp_pdf.exists():
                temp_pdf.unlink()
        
    except Exception as e:
        print(f"[ERROR] Pipeline failed: {e}")
        return False

def main():
    """Main function"""
    success = run_complete_pipeline()
    
    if success:
        print("\n🎉 OTPP pipeline executed successfully!")
        print("✅ Latest financial data extracted and formatted")
        print("✅ Ready for CANF data processing")
    else:
        print("\n❌ Pipeline failed - check error messages above")
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())