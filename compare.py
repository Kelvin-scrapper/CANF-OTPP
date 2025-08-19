import pandas as pd

def compare_files():
    df_excel = pd.read_excel('CANF_OTPP_DATA_20250818_123438.xlsx', sheet_name='DATA')
    df_csv = pd.read_csv('CANF_OTPP_DATA_20250324 - DATA (2).csv')

    excel_cols = set(df_excel.columns)
    csv_cols = set(df_csv.columns)
    common_cols = list(excel_cols.intersection(csv_cols))

    all_match = True
    compared_cols = 0

    for period in ['2024-Q4', '2025-Q2']:
        for col in common_cols:
            if col == 'Unnamed: 0':
                continue

            compared_cols += 1
            excel_val = df_excel[df_excel['Unnamed: 0'] == period][col].values[0] if len(df_excel[df_excel['Unnamed: 0'] == period][col].values) > 0 else None
            csv_val = df_csv[df_csv['Unnamed: 0'] == period][col].values[0] if len(df_csv[df_csv['Unnamed: 0'] == period][col].values) > 0 else None

            if pd.isna(csv_val):
                cleaned_csv_val = None
            else:
                cleaned_csv_val = float(str(csv_val).replace(',', '')) if str(csv_val) not in ['nan', '-'] else None

            if excel_val != cleaned_csv_val and not (pd.isna(excel_val) and pd.isna(cleaned_csv_val)):
                print(f'Mismatch found for period {period} and column {col}:')
                print(f'  Excel value: {excel_val}')
                print(f'  CSV value: {cleaned_csv_val}')
                all_match = False

    print(f'Compared {compared_cols} columns.')
    if all_match:
        print('All common columns and periods match.')

if __name__ == '__main__':
    compare_files()