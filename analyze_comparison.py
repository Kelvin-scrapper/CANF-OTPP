import pandas as pd
import numpy as np

def analyze_excel_files():
    """Compare the new script output with the manual extraction reference"""
    
    # Read both Excel files
    print("Reading Excel files...")
    
    # New script output file
    new_file = "CANF_OTPP_DATA_20250819.xlsx"
    try:
        new_df = pd.read_excel(new_file)
        print(f"[OK] Successfully read {new_file}")
        print(f"  Shape: {new_df.shape}")
    except Exception as e:
        print(f"[ERROR] Error reading {new_file}: {e}")
        return
    
    # Manual extraction reference file
    ref_file = "Copy of CANF_OTPP_DATA_20250324.xlsx"
    try:
        ref_df = pd.read_excel(ref_file)
        print(f"[OK] Successfully read {ref_file}")
        print(f"  Shape: {ref_df.shape}")
    except Exception as e:
        print(f"[ERROR] Error reading {ref_file}: {e}")
        return
    
    print("\n" + "="*60)
    print("COMPARISON ANALYSIS")
    print("="*60)
    
    # Get column headers (Time_Series_Codes)
    new_columns = set(new_df.columns)
    ref_columns = set(ref_df.columns)
    
    print(f"\nCOLUMN COUNT ANALYSIS:")
    print(f"Reference file (manual extraction): {len(ref_columns)} columns")
    print(f"New script output: {len(new_columns)} columns")
    
    # Find matching and missing columns
    matching_columns = new_columns.intersection(ref_columns)
    missing_in_new = ref_columns - new_columns
    extra_in_new = new_columns - ref_columns
    
    print(f"\nMATCHING ANALYSIS:")
    print(f"Matching columns: {len(matching_columns)}")
    print(f"Missing from new file: {len(missing_in_new)}")
    print(f"Extra in new file: {len(extra_in_new)}")
    
    # Calculate success percentage
    if len(ref_columns) > 0:
        success_percentage = (len(matching_columns) / len(ref_columns)) * 100
        print(f"Success percentage: {success_percentage:.1f}%")
    else:
        print("Success percentage: N/A (no reference columns)")
    
    # List missing asset codes
    if missing_in_new:
        print(f"\nMISSING ASSET CODES ({len(missing_in_new)}):")
        for i, code in enumerate(sorted(missing_in_new), 1):
            print(f"  {i:2d}. {code}")
    else:
        print(f"\n[SUCCESS] NO MISSING ASSET CODES - All reference codes found!")
    
    # List extra asset codes (if any)
    if extra_in_new:
        print(f"\nEXTRA ASSET CODES IN NEW FILE ({len(extra_in_new)}):")
        for i, code in enumerate(sorted(extra_in_new), 1):
            print(f"  {i:2d}. {code}")
    
    # Data quality check - compare actual values for matching columns
    print(f"\nDATA QUALITY ANALYSIS:")
    if matching_columns:
        print("Checking data values for matching columns...")
        
        data_matches = 0
        data_mismatches = 0
        
        for col in matching_columns:
            if col in new_df.columns and col in ref_df.columns:
                # Compare non-null values
                new_values = new_df[col].dropna()
                ref_values = ref_df[col].dropna()
                
                if len(new_values) > 0 and len(ref_values) > 0:
                    # Check if values are similar (allowing for small numerical differences)
                    try:
                        if pd.api.types.is_numeric_dtype(new_values) and pd.api.types.is_numeric_dtype(ref_values):
                            # For numeric data, check if values are close
                            if len(new_values) == len(ref_values):
                                if np.allclose(new_values.values, ref_values.values, rtol=1e-5, atol=1e-8, equal_nan=True):
                                    data_matches += 1
                                else:
                                    data_mismatches += 1
                            else:
                                data_mismatches += 1
                        else:
                            # For non-numeric data, check exact match
                            if new_values.equals(ref_values):
                                data_matches += 1
                            else:
                                data_mismatches += 1
                    except:
                        data_mismatches += 1
        
        print(f"Columns with matching data: {data_matches}")
        print(f"Columns with different data: {data_mismatches}")
        if data_matches + data_mismatches > 0:
            data_accuracy = (data_matches / (data_matches + data_mismatches)) * 100
            print(f"Data accuracy: {data_accuracy:.1f}%")
    
    # Overall assessment
    print(f"\n" + "="*60)
    print("OVERALL ASSESSMENT")
    print("="*60)
    
    if len(missing_in_new) == 0:
        print("[SUCCESS] GOAL ACHIEVED: 100% column coverage!")
        print("[SUCCESS] All asset codes from manual extraction are present in the new file")
    else:
        print(f"[WARNING] PARTIAL SUCCESS: {success_percentage:.1f}% column coverage")
        print(f"[WARNING] {len(missing_in_new)} asset codes still missing")
    
    if len(extra_in_new) > 0:
        print(f"[INFO] BONUS: {len(extra_in_new)} additional asset codes found")
    
    print(f"\nTarget: 4 rows × 75 columns (manual extraction)")
    print(f"Achieved: {new_df.shape[0]} rows × {new_df.shape[1]} columns (new script)")
    
    if success_percentage >= 100:
        print("\n[CELEBRATION] MISSION ACCOMPLISHED: Data extraction accuracy target met!")
    elif success_percentage >= 90:
        print("\n[EXCELLENT] EXCELLENT PROGRESS: Very close to target!")
    elif success_percentage >= 75:
        print("\n[GOOD] GOOD PROGRESS: Substantial improvement achieved!")
    else:
        print("\n[PROGRESS] PROGRESS MADE: Further improvements needed!")

if __name__ == "__main__":
    analyze_excel_files()