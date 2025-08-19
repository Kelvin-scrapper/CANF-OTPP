import pandas as pd

def final_summary():
    """Final summary comparison"""
    
    # Read both files
    new_df = pd.read_excel("CANF_OTPP_DATA_20250819.xlsx")
    ref_df = pd.read_excel("Copy of CANF_OTPP_DATA_20250324.xlsx")
    
    print("FINAL COMPARISON SUMMARY")
    print("="*50)
    
    new_cols = set(new_df.columns)
    ref_cols = set(ref_df.columns)
    
    matching = new_cols.intersection(ref_cols)
    missing = ref_cols - new_cols
    extra = new_cols - ref_cols
    
    print(f"REFERENCE FILE (Manual Extraction):")
    print(f"  Shape: {ref_df.shape[0]} rows x {ref_df.shape[1]} columns")
    print()
    
    print(f"NEW SCRIPT OUTPUT FILE:")
    print(f"  Shape: {new_df.shape[0]} rows x {new_df.shape[1]} columns")
    print()
    
    print(f"COLUMN ANALYSIS:")
    print(f"  Total columns in reference: {len(ref_cols)}")
    print(f"  Total columns in new file: {len(new_cols)}")
    print(f"  Matching columns: {len(matching)}")
    print(f"  Missing columns: {len(missing)}")
    print(f"  Extra columns: {len(extra)}")
    print()
    
    coverage = (len(matching) / len(ref_cols)) * 100
    print(f"SUCCESS PERCENTAGE: {coverage:.1f}%")
    print()
    
    print(f"MISSING ASSET CODES ({len(missing)}):")
    for i, code in enumerate(sorted(missing), 1):
        print(f"  {i:2d}. {code}")
    print()
    
    print(f"ACCURACY ASSESSMENT:")
    if coverage == 100.0:
        print("  ✓ PERFECT: 100% data extraction accuracy achieved!")
        goal_achieved = True
    elif coverage >= 95.0:
        print("  ⭐ EXCELLENT: 95%+ accuracy - virtually complete!")
        goal_achieved = True
    elif coverage >= 90.0:
        print("  📈 VERY GOOD: 90%+ accuracy - close to target!")
        goal_achieved = False
    elif coverage >= 80.0:
        print("  👍 GOOD: 80%+ accuracy - substantial progress!")
        goal_achieved = False
    else:
        print("  📊 MODERATE: Further improvements needed")
        goal_achieved = False
    
    print()
    print(f"IMPROVEMENTS NEEDED:")
    if len(missing) == 0:
        print("  ✓ None - all target columns captured!")
    else:
        missing_groups = {
            'Cash Collateral': [m for m in missing if 'CASHCOLLATERAL' in m],
            'Derivatives': [m for m in missing if 'DERIVATIVES' in m and 'RECEIVABLE' not in m],
            'Securities': [m for m in missing if 'SECURITIES' in m and 'REPURCHASED' in m]
        }
        
        for group, items in missing_groups.items():
            if items:
                print(f"  • {group}: {len(items)} columns missing")
    
    print()
    print(f"OVERALL RESULT:")
    if goal_achieved:
        print("  🎯 GOAL ACHIEVED: Target accuracy reached!")
    else:
        print(f"  ⏳ IN PROGRESS: {coverage:.1f}% complete, {100-coverage:.1f}% remaining")
    
    print()
    print(f"BONUS FINDINGS:")
    if len(extra) > 0:
        print(f"  • Found {len(extra)} additional asset codes not in reference")
        print(f"  • Total data coverage expanded beyond original scope")
    else:
        print(f"  • No additional codes found beyond reference")

if __name__ == "__main__":
    final_summary()