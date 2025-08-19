#!/usr/bin/env python3
"""
Simple test script to verify the OTPP pipeline works correctly
"""

import os
import sys
from pathlib import Path

def test_pipeline():
    """Test the pipeline using existing files"""
    
    print("="*60)
    print("TESTING OTPP PIPELINE")
    print("="*60)
    
    # Check if we have an existing extracted file
    current_dir = Path(".")
    extracted_files = list(current_dir.glob("*_extracted.xlsx"))
    
    if extracted_files:
        source_file = extracted_files[0]
        print(f"[FOUND] Using existing extracted file: {source_file.name}")
        
        # Import and run the excel extractor directly
        try:
            from excel import OTTPHierarchicalExtractor
            
            print("\n[STEP] Initializing extractor...")
            extractor = OTTPHierarchicalExtractor()
            
            print(f"[STEP] Validating file: {source_file}")
            abs_path = source_file.resolve()
            
            if extractor.validate_source_file(str(abs_path)):
                print("[OK] File validation passed")
                
                print("[STEP] Extracting data...")
                extracted_data = extractor.extract_data(str(abs_path))
                
                if extracted_data:
                    print(f"[OK] Extracted {len(extracted_data)} data points")
                    
                    output_file = f"TEST_OUTPUT_{source_file.stem.split('_')[-1]}.xlsx"
                    print(f"[STEP] Creating output: {output_file}")
                    
                    if extractor.create_output_file(extracted_data, output_file):
                        print(f"[SUCCESS] Pipeline test completed successfully!")
                        print(f"[OUTPUT] Created: {output_file}")
                        return True
                    else:
                        print("[ERROR] Failed to create output file")
                else:
                    print("[ERROR] No data extracted")
            else:
                print("[ERROR] File validation failed")
                
        except Exception as e:
            print(f"[ERROR] Pipeline test failed: {e}")
            import traceback
            traceback.print_exc()
    else:
        print("[ERROR] No extracted Excel files found in current directory")
        print("Available files:")
        for f in current_dir.glob("*.xlsx"):
            print(f"  - {f.name}")
    
    return False

if __name__ == "__main__":
    success = test_pipeline()
    if success:
        print("\n[RESULT] Pipeline is working correctly! [SUCCESS]")
    else:
        print("\n[RESULT] Pipeline has issues that need fixing [ERROR]")