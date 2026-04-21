"""
CANF_OTPP mapper — transforms the raw fitz-parsed investments DataFrame
into the AfricaAI 2-header-row output format.

Raw DataFrame layout (from scraper.fetch_data):
    label         str   — asset label from the PDF
    fv_current    float — fair value, current period
    cost_current  float — cost, current period
    fv_prior      float — fair value, prior period
    cost_prior    float — cost, prior period

Output format (AfricaAI):
    Row 0 : series codes   (None, CODE1, CODE2, ...)
    Row 1 : descriptions   (None, DESC1, DESC2, ...)
    Row 2+: data rows      (period_str, val1, val2, ...)
    Periods are sorted descending (most recent first).

Row alignment: raw_df index 0 → Excel row 5 (i.e. excel_row = index + 5).
Calibrated against the 2025 OTPP annual report fitz output (47 rows, rows 5-51).
"""

import json
import os
from typing import Optional

import pandas as pd

# Load column headers from headers.json (74 series — exceeds the 20-series inline threshold)
with open(os.path.join(os.path.dirname(__file__), "headers.json")) as _f:
    COLUMN_HEADERS = json.load(_f)

# Maps Excel row number → asset metadata.
# Rows absent from this dict (16, 17, 21) are sub-items with no output series code.
_ROW_MAPPINGS = {
     5: {'asset_code': 'EQUITYHEADER',                          'has_values': False},
     6: {'asset_code': 'EQUITIES',                              'has_values': False},
     7: {'asset_code': 'DOMESTICEQUITIES',                      'has_values': True},
     8: {'asset_code': 'FOREIGNEQUITIES',                       'has_values': True},
     9: {'asset_code': 'PRIVATEEQUITY',                         'has_values': False},
    10: {'asset_code': 'DOMESTICPRIVATEEQUITY',                  'has_values': True},
    11: {'asset_code': 'FOREIGNPRIVATEEQUITY',                   'has_values': True},
    12: {'asset_code': 'EQUITY',                                 'has_values': True},
    13: {'asset_code': 'FIXEDINCOMEHEADER',                      'has_values': False},
    14: {'asset_code': 'SHORTTERMINVESTMENTS',                   'has_values': True},
    15: {'asset_code': 'BONDS',                                  'has_values': False},
    # 16: Canadian bonds sub-row — no output code
    # 17: Non-Canadian bonds sub-row — no output code
    18: {'asset_code': 'REALRATEHEADER_SKIP',                    'has_values': False},
    19: {'asset_code': 'DOMESTICREALRATEPRODUCTS',               'has_values': True},
    20: {'asset_code': 'FOREIGNREALRATEPRODUCTS',                'has_values': True},
    # 21: Other debt sub-row — no output code
    22: {'asset_code': 'FIXEDINCOME',                            'has_values': True},
    23: {'asset_code': 'ALTERNATIVES',                           'has_values': True},
    24: {'asset_code': 'INFLATIONSENSITIVEHEADER',               'has_values': False},
    25: {'asset_code': 'COMMODITIES',                            'has_values': True},
    26: {'asset_code': 'TIMBERLAND',                             'has_values': True},
    27: {'asset_code': 'NATURALRESOURCES',                       'has_values': True},
    28: {'asset_code': 'INFLATIONSENSITIVE',                     'has_values': True},
    29: {'asset_code': 'REALASSETSHEADER',                       'has_values': False},
    30: {'asset_code': 'REALESTATE',                             'has_values': True},
    31: {'asset_code': 'INFRASTRUCTURE',                         'has_values': True},
    32: {'asset_code': 'REALASSETS',                             'has_values': True},
    33: {'asset_code': 'TOTAL',                                  'has_values': True},
    34: {'asset_code': 'INVESTMENTRELATEDRECEIVABLES',           'has_values': False},
    35: {'asset_code': 'SECURITIESREPURCHASED',                  'has_values': True},
    36: {'asset_code': 'CASHCOLLATERALDEPOSITEDUNDERSECURITIES', 'has_values': True},
    37: {'asset_code': 'CASHCOLLATERALPAIDUNDERCREDIT',          'has_values': True},
    38: {'asset_code': 'DERIVATIVES',                            'has_values': True},
    39: {'asset_code': 'INVESTMENTRELATEDSECURITIES',            'has_values': True},
    40: {'asset_code': 'TOTALINVESTMENTS',                       'has_values': True},
    41: {'asset_code': 'INVESTMENTRELATEDLIABILITIES',           'has_values': False},
    42: {'asset_code': 'SECURITIESSOLD',                         'has_values': True},
    43: {'asset_code': 'SECURITIESSOLDNOTREPURCHASEDLIABILITIES','has_values': False},
    44: {'asset_code': 'EQUITIESLIABILITIES',                    'has_values': True},
    45: {'asset_code': 'FIXEDINCOMELIABILITIES',                 'has_values': True},
    46: {'asset_code': 'COMMERCIALPAPER1',                       'has_values': True},
    47: {'asset_code': 'TERMDEBTLIABILITIES',                    'has_values': True},
    48: {'asset_code': 'CASHCOLLATERALUNDERSUPPORTLIABILITIES',  'has_values': True},
    49: {'asset_code': 'DERIVATIVESLIABILITIES',                 'has_values': True},
    50: {'asset_code': 'INVESTMENTRELATEDLIABILITIESTOTAL',      'has_values': True},
    51: {'asset_code': 'NETINVESTMENTS',                         'has_values': True},
}


def parse_period(report_type: str, report_year: int):
    """Return (current_period, prior_period) strings for the given report.

    Interim (H1): current = YYYY-Q2,  prior = (Y-1)-Q4
    Annual  (H2): current = YYYY-Q4,  prior = (Y-1)-Q4
    """
    if report_type == 'interim':
        return f"{report_year}-Q2", f"{report_year - 1}-Q4"
    return f"{report_year}-Q4", f"{report_year - 1}-Q4"


def parse_value(raw) -> Optional[float]:
    """Convert a raw cell value to float, or None if missing."""
    if raw is None or raw == '':
        return None
    if isinstance(raw, (int, float)):
        v = float(raw)
        return None if (v != v) else v  # guard NaN
    s = str(raw).strip().replace(',', '')
    if s.startswith('(') and s.endswith(')'):
        try:
            return -float(s[1:-1])
        except ValueError:
            return None
    try:
        return float(s)
    except ValueError:
        return None


def _series_code(asset_code: str, is_cost: bool) -> str:
    level = 'REPORTED' if asset_code in ('BONDS', 'COMMODITIES') else 'NONE'
    suffix = '.2' if is_cost else '.1'
    return f"ONTARIOTEACHERS.{asset_code}.LEVEL.{level}.H{suffix}@ONTARIOTEACHERS"


def map_to_output(raw_df: pd.DataFrame, report_info: dict) -> pd.DataFrame:
    """Transform raw fitz table into AfricaAI 2-header-row format.

    Output DataFrame:
        Row 0 : [None, CODE1, CODE2, ...]
        Row 1 : [None, DESC1, DESC2, ...]
        Row 2+: [period, val1, val2, ...]   — sorted descending
    """
    current_period, prior_period = parse_period(report_info['type'], report_info['year'])
    print(f"[INFO] Periods — current: {current_period}, prior: {prior_period}")

    data = {current_period: {}, prior_period: {}}

    for idx, raw_row in raw_df.iterrows():
        excel_row = idx + 5  # raw_df index 0 → Excel row 5
        mapping = _ROW_MAPPINGS.get(excel_row)
        if mapping is None or mapping['asset_code'] == 'REALRATEHEADER_SKIP':
            continue

        ac = mapping['asset_code']
        fv_cur   = parse_value(raw_row['fv_current'])
        cost_cur = parse_value(raw_row['cost_current'])
        fv_pri   = parse_value(raw_row['fv_prior'])
        cost_pri = parse_value(raw_row['cost_prior'])

        if fv_cur is not None:
            data[current_period][_series_code(ac, False)] = fv_cur
        if cost_cur is not None:
            data[current_period][_series_code(ac, True)] = cost_cur
        if fv_pri is not None:
            data[prior_period][_series_code(ac, False)] = fv_pri
        if cost_pri is not None:
            data[prior_period][_series_code(ac, True)] = cost_pri

    codes        = COLUMN_HEADERS['codes']         # [None, CODE1, ...]
    descriptions = COLUMN_HEADERS['descriptions']  # [None, DESC1, ...]
    series_codes = codes[1:]

    rows = [codes, descriptions]
    for period in sorted(data.keys(), reverse=True):
        rows.append([period] + [data[period].get(c) for c in series_codes])

    return pd.DataFrame(rows)


def build_metadata_rows() -> list:
    """Return list of dicts suitable for the META Excel sheet."""
    codes        = COLUMN_HEADERS['codes'][1:]
    descriptions = COLUMN_HEADERS['descriptions'][1:]
    return [
        {
            'CODE':              code,
            'DESCRIPTION':       desc,
            'FREQUENCY':         'Semi-Annual',
            'UNIT':              'CAD millions',
            'SOURCE_NAME':       "Ontario Teachers' Pension Plan",
            'SOURCE_URL':        PAGE_URL,
            'DATASET':           'CANF_OTPP',
            'NEXT_RELEASE_DATE': '',
        }
        for code, desc in zip(codes, descriptions)
    ]


PAGE_URL = "https://www.otpp.com/en-ca/about-us/our-results/report-archive/"
