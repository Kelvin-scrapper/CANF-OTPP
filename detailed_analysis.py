import pandas as pd
import re

def detailed_analysis():
    """Detailed analysis of the comparison between files"""
    
    # Read both files
    new_df = pd.read_excel("CANF_OTPP_DATA_20250819.xlsx")
    ref_df = pd.read_excel("Copy of CANF_OTPP_DATA_20250324.xlsx")
    
    print("DETAILED COMPARISON ANALYSIS")
    print("="*70)
    
    # Get columns
    new_cols = set(new_df.columns)
    ref_cols = set(ref_df.columns)
    
    matching = new_cols.intersection(ref_cols)
    missing = ref_cols - new_cols
    extra = new_cols - ref_cols
    
    print(f"Reference file shape: {ref_df.shape}")
    print(f"New file shape: {new_df.shape}")
    print()
    
    # Analyze missing patterns
    print("MISSING COLUMN PATTERNS:")
    print("-" * 30)
    
    missing_patterns = {}
    for col in missing:
        # Extract the main asset type
        if 'CASHCOLLATERAL' in col:
            if 'DEPOSITED' in col and 'UNDERSECURITIES' in col:
                pattern = 'CASHCOLLATERALDEPOSITEDUNDERSECURITIES'
            elif 'PAID' in col and 'UNDERCREDIT' in col:
                pattern = 'CASHCOLLATERALPAIDUNDERCREDIT'
            else:
                pattern = 'OTHER_CASHCOLLATERAL'
        elif 'DERIVATIVES' in col and col.count('.') == 5:  # Simple derivatives
            pattern = 'DERIVATIVES'
        elif 'SECURITIES' in col:
            if 'REPURCHASED' in col:
                pattern = 'SECURITIESREPURCHASED'
            elif 'SOLDNOTREPURCHASEDLIABILITIES' in col:
                pattern = 'SECURITIESSOLDNOTREPURCHASEDLIABILITIES'
            else:
                pattern = 'OTHER_SECURITIES'
        else:
            pattern = 'OTHER'
        
        if pattern not in missing_patterns:
            missing_patterns[pattern] = []
        missing_patterns[pattern].append(col)
    
    for pattern, cols in missing_patterns.items():
        print(f"{pattern}: {len(cols)} columns")
        for col in sorted(cols):
            print(f"  - {col}")
        print()
    
    # Analyze extra patterns  
    print("EXTRA COLUMN PATTERNS:")
    print("-" * 25)
    
    extra_patterns = {}
    for col in extra:
        if 'CASHCOLLATERAL' in col:
            if 'DEPOSITED' in col and 'UNDERSECURITIES' not in col:
                pattern = 'CASHCOLLATERALDEPOSITED'
            elif 'PAID' in col and 'UNDERCREDIT' not in col:
                pattern = 'CASHCOLLATERALPAID'
            else:
                pattern = 'OTHER_CASHCOLLATERAL_EXTRA'
        elif 'COMMERCIALPAPER' in col:
            pattern = 'COMMERCIALPAPER1'
        elif 'DERIVATIVES' in col and 'RECEIVABLE' in col:
            pattern = 'DERIVATIVESRECEIVABLE'
        elif 'NETINVESTMENTS' in col:
            pattern = 'NETINVESTMENTS'
        elif 'SECURITIES' in col:
            if 'PURCHASED' in col:
                pattern = 'SECURITIESPURCHASED'
            elif 'SOLD' in col and 'NOTYETPURCHASED' in col:
                pattern = 'SECURITIESSOLDNOTYETPURCHASED'
            elif 'SOLD' in col and 'NOTYETPURCHASED' not in col:
                pattern = 'SECURITIESSOLD'
            else:
                pattern = 'OTHER_SECURITIES_EXTRA'
        else:
            pattern = 'OTHER_EXTRA'
        
        if pattern not in extra_patterns:
            extra_patterns[pattern] = []
        extra_patterns[pattern].append(col)
    
    for pattern, cols in extra_patterns.items():
        print(f"{pattern}: {len(cols)} columns")
        for col in sorted(cols):
            print(f"  - {col}")
        print()
    
    # Check for potential matches (similar names)
    print("POTENTIAL MATCHES (similar naming):")
    print("-" * 40)
    
    potential_matches = []
    for missing_col in missing:
        for extra_col in extra:
            # Check for similar patterns
            missing_base = missing_col.replace('UNDERSECURITIES', '').replace('UNDERCREDIT', '')
            extra_base = extra_col
            
            if 'CASHCOLLATERAL' in missing_col and 'CASHCOLLATERAL' in extra_col:
                if 'DEPOSITED' in missing_col and 'DEPOSITED' in extra_col:
                    potential_matches.append((missing_col, extra_col, "COLLATERAL_DEPOSITED"))
                elif 'PAID' in missing_col and 'PAID' in extra_col:
                    potential_matches.append((missing_col, extra_col, "COLLATERAL_PAID"))
    
    for missing, extra, reason in potential_matches:
        print(f"Missing: {missing}")
        print(f"Extra:   {extra}")
        print(f"Reason:  {reason}")
        print()
    
    # Success metrics
    print("SUCCESS METRICS:")
    print("-" * 20)
    coverage = len(matching) / len(ref_cols) * 100
    print(f"Column coverage: {coverage:.1f}% ({len(matching)}/{len(ref_cols)})")
    print(f"Missing columns: {len(missing)}")
    print(f"Extra columns found: {len(extra)}")
    
    # Check if we're close to 100%
    if len(missing) <= 5:
        print("\nVERY CLOSE TO TARGET! Only a few columns missing.")
    elif coverage >= 90:
        print("\nEXCELLENT PROGRESS! Over 90% coverage achieved.")
    elif coverage >= 80:
        print("\nGOOD PROGRESS! Over 80% coverage achieved.")
    
    # Assessment
    print(f"\nFINAL ASSESSMENT:")
    print("-" * 20)
    if coverage == 100:
        print("PERFECT SUCCESS: 100% column coverage achieved!")
    elif coverage >= 90:
        print("NEAR PERFECT: Excellent coverage with minor gaps.")
    elif coverage >= 80:
        print("STRONG SUCCESS: Good coverage with some gaps to address.")
    else:
        print("MODERATE SUCCESS: Decent progress but significant gaps remain.")

if __name__ == "__main__":
    detailed_analysis()