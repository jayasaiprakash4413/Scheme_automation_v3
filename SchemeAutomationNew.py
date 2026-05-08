import streamlit as st
import pandas as pd
import json
import re
import requests
import os
import time
from copy import deepcopy
from pathlib import Path
from datetime import datetime
from decimal import Decimal, getcontext, ROUND_HALF_UP

# Try to import DB library gracefully
try:
    import psycopg2
    DB_DRIVER_AVAILABLE = True
except ImportError:
    DB_DRIVER_AVAILABLE = False

getcontext().prec = 50

st.set_page_config(layout="wide")
st.title("Final Scheme Configuration Engine")

tab1, tab2, tab3, tab4 = st.tabs([
    "Phase 1 — Generate", 
    "Phase 2 — Check & Rectify", 
    "Phase 3 — Push to Production", 
    "Phase 4 — Inject Schemes"
])

# ============================================================
# SECRETS, CONFIGS & CONSTANTS
# ============================================================

# Safely load secrets with a friendly error if the file is missing
try:
    DB_HOST = st.secrets["DB_HOST"]
    DB_PORT = st.secrets["DB_PORT"]
    DB_NAME = st.secrets["DB_NAME"]
    DB_USER = st.secrets["DB_USER"]
    DB_PASS = st.secrets["DB_PASS"]
except FileNotFoundError:
    st.error("🚨 **Error:** The `.streamlit/secrets.toml` file is missing. Please create it to connect to the database.")
    st.stop()
except KeyError as e:
    st.error(f"🚨 **Error:** Missing key {e} in your `secrets.toml` file.")
    st.stop()

SECURE_S1_DELIGHT = Decimal("9.95")
SECURE_S2_DELIGHT = Decimal("17.00")

SECURE_S1_ROYAL = Decimal("13.20")
SECURE_S2_ROYAL = Decimal("18.50")

UNSECURE_JSON_6_7 = (Decimal("48.00"), Decimal("48.00"), Decimal("48.00"))
UNSECURE_JSON_12 = (Decimal("37.65"), Decimal("37.65"), Decimal("37.65"))

# Internal calc values
UNSECURE_CALC_6_7 = (Decimal("48.00"), Decimal("46.00"), Decimal("48.00"))
UNSECURE_CALC_12 = (Decimal("37.65"), Decimal("32.00"), Decimal("37.65"))

LTV_CODE_MAP = {
    "e0": Decimal("80"),
    "s5": Decimal("75"),
    "s7": Decimal("77"),
    "s6": Decimal("76"),
    "si5": Decimal("65")
}

REQUIRED_SCHEME_COLUMNS = [
    "SchemeName", "schemeFlags", "refName", "SchemeMin", "SchemeMax", "CityIds",
    "description", "displayText", "chargeText", "refno", "tenure", "GroupTags",
    "applicableProcesses", "fulfillmentChannels", "customerLtv", "goldBenchmark",
    "productCategory", "OverallInterestCalculation", "NoOfBaseScheme", "bs1-legalName",
    "bs1-Lender", "bs1-type", "bs1-benchmark", "bs1-ltv", "bs1-tenure", "bs1-calculation",
    "bs1-NoOfCharges", "bs1-charge-1", "bs1-charge-2", "bs1-charge-3", "bs1-charge-4",
    "bs1-NoOfAddOns", "bs1-addon-1", "bs1-addon-2", "bs1-addon-3", "bs2-legalName",
    "bs2-Lender", "bs2-tenure", "bs2-type", "bs2-benchmark", "bs2-minThreshold", "bs2-MaxThreshold",
    "bs2-calculation", "bs2-NoOfCharges", "bs2-charge-1", "bs2-charge-2", "bs2-charge-3",
    "bs2-NoOfAddOns", "bs2-addon-1", "bs2-addon-2", "bs2-addon-3"
]

SUMMARY_INPUT_COLUMNS = [
    "customerLtv", "TS", "slab1 ROI", "PF Tag", "Flow", "PF val", "Tenure", 
    "Scheme End Tag", "SchemeMin Input", "SchemeMax Input", "FC Value", "Include FC",
    "Custom Sec LTV", "Custom Legal Name", "Is Balance Scheme"
]

CHECKER_COLUMNS = [
    "CHK PF Config Min", "CHK PF Config Max", "CHK PF Calc Min", "CHK PF Calc Max",
    "CHK FC Config", "CHK FC Calc",
    "CHK Overall IR Config", "CHK Overall IR Calc",
    "CHK Secure IR Config", "CHK Secure IR Calc",
    "CHK Unsecure IR Config", "CHK Unsecure IR Calc",
    "CHK Tenure Config", "CHK Tenure Calc",
    "CHK Slab3 ToDay Config", "CHK Slab3 ToDay Calc",
    "CHK Secure LTV Config", "CHK Secure LTV Calc",
    "CHK BS1 LegalName Config", "CHK BS1 LegalName Calc",
    "CHK BS2 LegalName Config", "CHK BS2 LegalName Calc",
    "CHK ApplicableProcesses Config", "CHK ApplicableProcesses Calc",
    "CHK Balance Config", "CHK Balance Calc",
    "FED CLM Flag"
]

# ============================================================
# DATABASE LOGGING UTILITIES
# ============================================================

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def fetch_previously_validated_schemes():
    """Returns a set of masterschemeid strings that have already been validated successfully."""
    if not DB_DRIVER_AVAILABLE: return set()
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "SELECT masterschemeid FROM temp.scheme_validation_logs WHERE overall_status = '✅ ALL OK'"
        cursor.execute(query)
        rows = cursor.fetchall()
        conn.close()
        return {str(r[0]) for r in rows if r[0]}
    except Exception as e:
        st.warning(f"Could not fetch validation history from DB. Assuming all are new. Error: {e}")
        return set()

def log_validation_results_to_db(status_df, raw_api_df=pd.DataFrame()):
    """Writes the results of a validation check batch directly to the Redshift table."""
    if not DB_DRIVER_AVAILABLE or status_df.empty: return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO temp.scheme_validation_logs 
            (masterschemeid, refname, lm_appname, product_type, overall_status, failed_checks)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        appname_map = {}
        ptype_map = {}
        if not raw_api_df.empty and 'masterschemeid' in raw_api_df.columns:
            for _, row in raw_api_df.iterrows():
                mid = str(row.get('masterschemeid', ''))
                appname_map[mid] = str(row.get('lm_appname', ''))
                ptype_map[mid] = str(row.get('Product Type', ''))

        for idx, row in status_df.iterrows():
            m_id = str(row.get("masterschemeid", ""))
            ref = str(row.get("refName", ""))
            status = str(row.get("Overall", ""))
            
            failed_checks = []
            for col in status_df.columns:
                if col not in ["masterschemeid", "refName", "Overall"] and "❌" in str(row.get(col, "")):
                    failed_checks.append(col)
            failed_str = ", ".join(failed_checks)

            lm_appname = appname_map.get(m_id, "")
            prod_type = ptype_map.get(m_id, "")

            cursor.execute(insert_query, (m_id, ref, lm_appname, prod_type, status, failed_str))
            
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Failed to write validation logs to Database: {e}")

def log_creation_results_to_db(results_list):
    """Writes the results of Phase 3 API creation directly to the Redshift table."""
    if not DB_DRIVER_AVAILABLE or not results_list: return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO temp.scheme_creation_logs 
            (refname, status, api_response)
            VALUES (%s, %s, %s)
        """
        for res in results_list:
            ref = str(res.get("Scheme Name (refName)", ""))
            status = str(res.get("Status", ""))
            resp = str(res.get("API Response", ""))
            cursor.execute(insert_query, (ref, status, resp))
            
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Failed to write creation logs to Database: {e}")

def log_disable_results_to_db(results_list):
    """Writes the results of Phase 3 Scheme Disable/Enable to the Redshift table."""
    if not DB_DRIVER_AVAILABLE or not results_list: return
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
            INSERT INTO temp.scheme_disable_logs 
            (masterschemeid, disable_flag, action_comment, api_status, api_response)
            VALUES (%s, %s, %s, %s, %s)
        """
        for res in results_list:
            m_id = str(res.get("MasterSchemeId", ""))
            dis_flag = str(res.get("Disable Flag", ""))
            action = str(res.get("Action", ""))
            status = str(res.get("Status", ""))
            resp = str(res.get("API Response", ""))
            
            cursor.execute(insert_query, (m_id, dis_flag, action, status, resp))
            
        conn.commit()
        conn.close()
    except Exception as e:
        st.error(f"Failed to write disable logs to Database: {e}")

# ============================================================
# ENGINE UTILITIES
# ============================================================

def _normalize_construct_keys(construct):
    normalized = dict(construct)
    if "Description" in normalized and "description" not in normalized:
        normalized["description"] = normalized.pop("Description")
    return normalized

def _load_dummy_construct(filename):
    path = Path(__file__).resolve().parent / filename
    try:
        with path.open("r", encoding="utf-8") as file:
            loaded = json.load(file)
        return _normalize_construct_keys(loaded)
    except Exception:
        return {}

DUMMY_FLEXI_CONSTRUCT = _load_dummy_construct("Flexi_pf.json")
DUMMY_FIXED_CONSTRUCT = _load_dummy_construct("fixed_pf.json")
DUMMY_HIP_CONSTRUCT  = _load_dummy_construct("HIP_Test_Scheme.json")

def _is_restructure_input(df):
    return not all(column in df.columns for column in REQUIRED_SCHEME_COLUMNS)

def _normalize_column_name(column_name):
    return re.sub(r'[^a-z0-9]+', '', str(column_name).lower())

def _get_row_value(row, aliases):
    normalized_to_actual = {
        _normalize_column_name(column): column
        for column in row.index
    }
    for alias in aliases:
        resolved_column = normalized_to_actual.get(_normalize_column_name(alias))
        if resolved_column is not None:
            return row.get(resolved_column)
    return None

def _parse_decimal(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace('%', '').replace(',', '')
    try:
        return Decimal(text)
    except Exception:
        return None

def _parse_int(value):
    parsed_decimal = _parse_decimal(value)
    if parsed_decimal is None:
        return None
    return int(parsed_decimal)

def _parse_pf_value(pf_value):
    if pf_value is None or (isinstance(pf_value, float) and pd.isna(pf_value)):
        return None, None
    numbers = re.findall(r'([0-9]+(?:\.[0-9]+)?)', str(pf_value))
    if len(numbers) >= 2:
        return Decimal(numbers[0]), Decimal(numbers[1])
    if len(numbers) == 1:
        value = Decimal(numbers[0])
        return value, value
    return None, None

def _ts_label_to_min_max(ts_label):
    if ts_label is None:
        return None, None
    tag = str(ts_label).strip().upper().replace(" ", "")
    mapping = {
        "<3L":   (30000,   299999),
        "3-6L":  (300000,  599999),
        "6-12L": (600000,  1199999),
        ">12L":  (1200000, 10000000),
        "ALLTS": (30000,   10000000),
    }
    return mapping.get(tag, (None, None))

def _ltv_code_from_input(ltv_value):
    parsed_ltv = _parse_decimal(ltv_value)
    if parsed_ltv is None:
        return None
    for code, ltv in LTV_CODE_MAP.items():
        if parsed_ltv == ltv:
            return code
    return None

def _update_refname_ltv_code(refname, ltv_value):
    target_code = _ltv_code_from_input(ltv_value)
    if not target_code:
        return str(refname)
    updated = str(refname)
    code_pattern = r'\b(?:' + '|'.join(sorted(LTV_CODE_MAP.keys(), key=len, reverse=True)) + r')\b'
    bracket_match = re.search(r'\((.*?)\)', updated)
    if bracket_match:
        segment = bracket_match.group(1)
        replaced_segment = re.sub(code_pattern, target_code, segment, count=1, flags=re.IGNORECASE)
        return updated[:bracket_match.start(1)] + replaced_segment + updated[bracket_match.end(1):]
    return re.sub(code_pattern, target_code, updated, count=1, flags=re.IGNORECASE)

def _update_refname_opp(refname, slab1_roi):
    opp_value = _parse_decimal(slab1_roi)
    if opp_value is None:
        return str(refname)
    updated = str(refname)
    opp_text = f"{opp_value.quantize(Decimal('0.00'), ROUND_HALF_UP)}%"
    split_on_pf = re.split(r'(\bPF\b)', updated, maxsplit=1, flags=re.IGNORECASE)
    if len(split_on_pf) >= 3:
        left_part = re.sub(r'([0-9]+(?:\.[0-9]+)?)\s*%', opp_text, split_on_pf[0], count=1)
        return ''.join([left_part, split_on_pf[1], split_on_pf[2]])
    return re.sub(r'([0-9]+(?:\.[0-9]+)?)\s*%', opp_text, updated, count=1)

def _update_refname_pf(refname, pf_tag, pf_value):
    tag_text = str(pf_tag).strip().lower() if pf_tag is not None else ""
    updated = str(refname)
    if tag_text == "nopf":
        updated = re.sub(r'\|\|\s*PF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%\s*(?:[-–]\s*[0-9]+(?:\.[0-9]+)?\s*%)?', '', updated, flags=re.IGNORECASE)
        updated = re.sub(r'PF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%\s*(?:[-–]\s*[0-9]+(?:\.[0-9]+)?\s*%)?', '', updated, count=1, flags=re.IGNORECASE)
        updated = re.sub(r'\s*flexi\s*pf\b|\s*flexi-pf\b|\s*flexipf\b', '', updated, flags=re.IGNORECASE)
        updated = re.sub(r'\|\|\s*\|\|', '||', updated)
        return re.sub(r'\s+', ' ', updated).strip()
    min_pf, max_pf = _parse_pf_value(pf_value)
    if min_pf is None:
        updated = re.sub(r'(%)(\d{1,2}\s*M\b)', r'\1 \2', updated)
        return re.sub(r'\s+', ' ', updated).strip()
    is_flexi = tag_text == "flexi" if pf_tag is not None else (min_pf != max_pf)
    if is_flexi:
        pf_text = f"PF- {min_pf.quantize(Decimal('0.00'), ROUND_HALF_UP)}%-{max_pf.quantize(Decimal('0.00'), ROUND_HALF_UP)}%"
    else:
        pf_text = f"PF- {min_pf.quantize(Decimal('0.00'), ROUND_HALF_UP)}%"
    pf_pattern = r'PF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%\s*(?:[-–]\s*[0-9]+(?:\.[0-9]+)?\s*%)?'
    pf_already_present = bool(re.search(pf_pattern, updated, re.IGNORECASE))
    updated = re.sub(pf_pattern, pf_text, updated, count=1, flags=re.IGNORECASE)
    if not pf_already_present:
        updated = f"{str(refname).strip()} || {pf_text}".strip()
    if is_flexi:
        if not re.search(r'flexi\s*pf|flexi-pf|flexipf', updated, re.IGNORECASE):
            updated = f"{updated} flexipf".strip()
    else:
        updated = re.sub(r'\s*flexi\s*pf\b|\s*flexi-pf\b|\s*flexipf\b', '', updated, flags=re.IGNORECASE)
    updated = re.sub(r'(%)(\d{1,2}\s*M\b)', r'\1 \2', updated)
    return re.sub(r'\s+', ' ', updated).strip()

def _update_refname_flow(refname, flow_tag):
    if flow_tag is None:
        return str(refname)
    flow = str(flow_tag).strip().upper()
    updated = str(refname)
    if flow == "RWL":
        updated = re.sub(r'\bFL\s*TO\b', 'Renewal', updated, flags=re.IGNORECASE)
        updated = re.sub(r'\bRetention\b', 'Renewal', updated, flags=re.IGNORECASE)
    elif flow == "FWD":
        updated = re.sub(r'\bRenewal\b', 'FL TO', updated, flags=re.IGNORECASE)
        updated = re.sub(r'\bRetention\b', 'FL TO', updated, flags=re.IGNORECASE)
    elif flow == "RTN":
        updated = re.sub(r'\bRenewal\b', 'Retention', updated, flags=re.IGNORECASE)
        updated = re.sub(r'\bFL\s*TO\b', 'Retention', updated, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', updated).strip()

def _update_refname_ts(refname, ts_tag):
    if ts_tag is None:
        return str(refname)
    tag = str(ts_tag).strip().upper().replace(" ", "")
    label_map = {"<3L": "<3L", "3-6L": "3-6L", "6-12L": "6-12L", ">12L": ">12L", "ALLTS": "ALL TS"}
    ts_label = label_map.get(tag)
    if ts_label is None:
        return str(refname)
    updated = re.sub(r'(<\s*3L|3\s*-\s*6L|6\s*-\s*12L|>\s*12L|ALL\s*TS|<\s*6L|>\s*6L)', ts_label, str(refname), count=1, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', updated).strip()

def _pick_dummy_construct(refname, pf_tag=None, product_type=None):
    if product_type is not None and "hip" in str(product_type).strip().lower():
        return deepcopy(DUMMY_HIP_CONSTRUCT)
    if pf_tag is not None:
        tag_text = str(pf_tag).strip().lower()
        if tag_text == "flexi":
            return deepcopy(DUMMY_FLEXI_CONSTRUCT)
        if tag_text == "fixed":
            return deepcopy(DUMMY_FIXED_CONSTRUCT)
    refname_text = str(refname)
    refname_lower = refname_text.lower()
    if any(token in refname_lower for token in ["flexipf", "flexi pf", "flexi-pf"]):
        return deepcopy(DUMMY_FLEXI_CONSTRUCT)
    min_pf, max_pf = extract_pf_range(refname_text)
    if min_pf is not None and max_pf is not None and min_pf != max_pf:
        return deepcopy(DUMMY_FLEXI_CONSTRUCT)
    return deepcopy(DUMMY_FIXED_CONSTRUCT)

def build_working_dataframe(input_df):
    if not _is_restructure_input(input_df):
        return input_df.copy()
    rebuilt_rows = []
    for _, input_row in input_df.iterrows():
        refname_value = input_row.get("refName", "")
        pf_tag_value = _get_row_value(input_row, ["PF Tag", "pf_tag", "pfTag"])
        product_type_value = _get_row_value(input_row, ["Product Type", "product_type", "productType"])
        template_row = _pick_dummy_construct(refname_value, pf_tag_value, product_type_value)
        for column, value in input_row.items():
            if pd.notna(value):
                template_row[column] = value
        if "refName" in template_row:
            template_row["refName"] = str(template_row["refName"]).strip()
        if "refno" in template_row and "refName" in template_row:
            template_row["refno"] = template_row["refName"]
        rebuilt_rows.append(template_row)

    rebuilt_df = pd.DataFrame(rebuilt_rows)
    for required_column in REQUIRED_SCHEME_COLUMNS:
        if required_column not in rebuilt_df.columns:
            rebuilt_df[required_column] = ""
    return rebuilt_df

def _extract_charge_value(json_str):
    try:
        data = json.loads(json_str) if str(json_str).strip() else {}
    except Exception:
        return None
    if isinstance(data, dict) and "chargeValue" in data:
        try:
            return Decimal(str(data.get("chargeValue")))
        except Exception:
            return None
    return None

def _extract_interest_rates(json_str):
    try:
        data = json.loads(json_str) if str(json_str).strip() else {}
    except Exception:
        return []
    slabs = _find_slab_list(data)
    if slabs:
        values = []
        for slab in slabs[:3]:
            if isinstance(slab, dict) and "interestRate" in slab:
                values.append(str(slab.get("interestRate")))
        return values
    if isinstance(data, dict) and "interestRate" in data:
        return [str(data.get("interestRate"))]
    return []

def _extract_slab3_today(json_str):
    try:
        data = json.loads(json_str) if str(json_str).strip() else {}
    except Exception:
        return None
    slabs = _find_slab_list(data)
    if slabs and isinstance(slabs[-1], dict):
        return slabs[-1].get("toDay")
    if isinstance(data, dict):
        return data.get("toDay")
    return None

def _extract_pf_config_values(json_str):
    try:
        data = json.loads(json_str) if str(json_str).strip() else {}
    except Exception:
        return None, None
    if not isinstance(data, dict):
        return None, None
    meta = data.get("chargesMetaData")
    if isinstance(meta, dict):
        min_val = meta.get("minPercentUnsecure")
        max_val = meta.get("maxPercentUnsecure")
        try: min_dec = Decimal(str(min_val)) if min_val is not None else None
        except: min_dec = None
        try: max_dec = Decimal(str(max_val)) if max_val is not None else None
        except: max_dec = None
        if min_dec is not None or max_dec is not None:
            return min_dec, max_dec
    charge = data.get("chargeValue")
    try: charge_dec = Decimal(str(charge)) if charge is not None else None
    except: charge_dec = None
    return charge_dec, charge_dec

def _drop_empty_rows(df):
    if df.empty:
        return df
    def has_value(row):
        for value in row:
            if pd.isna(value): continue
            if str(value).strip() != "": return True
        return False
    return df[df.apply(has_value, axis=1)].reset_index(drop=True)

def finalize_output_columns(df):
    base_columns = [column for column in REQUIRED_SCHEME_COLUMNS if column in df.columns]
    summary_columns = [column for column in SUMMARY_INPUT_COLUMNS if column in df.columns]
    checker_columns = [column for column in CHECKER_COLUMNS if column in df.columns]
    ordered_columns = []
    seen = set()
    for column in base_columns + summary_columns + checker_columns:
        if column not in seen:
            seen.add(column)
            ordered_columns.append(column)
    return df.loc[:, ordered_columns].copy()

def extract_ltv_from_code(refname):
    refname_str = str(refname).lower()
    bracket_matches = re.findall(r'\((.*?)\)', refname_str)
    for segment in bracket_matches:
        tokens = [token for token in re.split(r'[^a-z0-9]+', segment) if token]
        for token in tokens:
            if token in LTV_CODE_MAP:
                return LTV_CODE_MAP[token]
    tokens = [token for token in re.split(r'[^a-z0-9]+', refname_str) if token]
    for token in tokens:
        if token in LTV_CODE_MAP:
            return LTV_CODE_MAP[token]
    return None

def extract_tenure(refname):
    match = re.search(r'\b(\d{1,2})\s*M\b', str(refname), re.IGNORECASE)
    return int(match.group(1)) if match else None

def extract_pf(refname):
    match = re.search(r'PF\s*[-:]?\s*([0-9]+(?:\.[0-9]+)?)%', str(refname), re.IGNORECASE)
    return Decimal(match.group(1)) if match else None

def extract_pf_range(refname):
    match = re.search(
        r'PF\s*[-:]?\s*([0-9]+(?:\.[0-9]+)?)%\s*[-–]\s*([0-9]+(?:\.[0-9]+)?)%',
        str(refname),
        re.IGNORECASE
    )
    if match: return Decimal(match.group(1)), Decimal(match.group(2))
    return None, None

def extract_opp(refname):
    parts = re.split(r'PF', str(refname), flags=re.IGNORECASE)[0]
    match = re.search(r'([0-9]+(?:\.[0-9]+)?)%', parts)
    return Decimal(match.group(1)) if match else None

def update_refname_tenure(refname, tenure):
    return re.sub(r'\b(\d{1,2})\s*M\b', f'{tenure}M', str(refname), count=1, flags=re.IGNORECASE)

def get_tenure_days(tenure):
    mapping = {6: 180, 7: 210, 12: 360}
    return mapping.get(int(tenure), int(tenure) * 30)

def decision_engine(overall_ltv, monthly_opp, requested_tenure):
    secure_s1 = Decimal("9.95")
    if requested_tenure == 12:
        secure_ltv = Decimal("60")
        unsecure_s1 = Decimal("37.65")
    else:
        secure_ltv = Decimal("67")
        unsecure_s1 = Decimal("48.00")

    if overall_ltv <= secure_ltv:
        if requested_tenure in (6, 7): return ("Royal", 7)
        return ("Royal", requested_tenure)

    secure_weight = secure_ltv / overall_ltv
    unsecure_weight = (overall_ltv - secure_ltv) / overall_ltv

    min_opp = (secure_weight * secure_s1) / Decimal("12")
    max_opp = (secure_weight * secure_s1 + unsecure_weight * unsecure_s1) / Decimal("12")

    min_opp = min_opp.quantize(Decimal("0.01"), ROUND_HALF_UP)
    max_opp = max_opp.quantize(Decimal("0.01"), ROUND_HALF_UP)

    if min_opp <= monthly_opp <= max_opp:
        if requested_tenure in (6, 7): return ("Delight", 6)
        return ("Delight", requested_tenure)

    if requested_tenure in (6, 7): return ("Royal", 7)
    return ("Royal", requested_tenure)

def secure_slab3(tenure):
    r = Decimal("0.229")
    m = Decimal("12")
    t = Decimal(str(tenure))
    compound = (Decimal("1") + r/m) ** t
    result = (compound - Decimal("1")) * m / t
    return (result * 100).quantize(Decimal("0.00"), ROUND_HALF_UP)

def interest_engine(scheme, tenure, overall_ltv, monthly_opp, secure_ltv_override=None):
    if scheme == "Delight":
        secure_s1 = SECURE_S1_DELIGHT
        secure_s2 = SECURE_S2_DELIGHT
    else:
        secure_s1 = SECURE_S1_ROYAL
        secure_s2 = SECURE_S2_ROYAL

    if secure_ltv_override is not None:
        secure_ltv = secure_ltv_override
    elif tenure == 6: secure_ltv = Decimal("67")
    elif tenure == 7: secure_ltv = Decimal("66")
    else: secure_ltv=Decimal("60")
    
    secure_s3 = secure_slab3(tenure)

    if tenure == 12:
        calc_unsecure = UNSECURE_CALC_12
        json_unsecure = UNSECURE_JSON_12
    else:
        calc_unsecure = UNSECURE_CALC_6_7
        json_unsecure = UNSECURE_JSON_6_7

    secure_weight = secure_ltv / overall_ltv
    unsecure_weight = (overall_ltv - secure_ltv) / overall_ltv

    s1 = (monthly_opp * Decimal("12")).quantize(Decimal("0.00"), ROUND_HALF_UP)
    s2 = (secure_weight * secure_s2 + unsecure_weight * calc_unsecure[1]).quantize(Decimal("0.00"), ROUND_HALF_UP)
    s3 = (secure_weight * secure_s3 + unsecure_weight * calc_unsecure[2]).quantize(Decimal("0.00"), ROUND_HALF_UP)

    return {
        "secure_slabs": (secure_s1, secure_s2, secure_s3),
        "unsecure_slabs": json_unsecure,
        "overall_slabs": (s1, s2, s3),
        "secure_ltv": secure_ltv,
        "calc_unsecure_slabs": calc_unsecure
    }

def update_charge_text(json_str, unsecure_pf, overall_pf):
    data = json.loads(json_str)
    data["secureProcessingFee"] = "0%"
    data["unsecureProcessingFee"] = f"{unsecure_pf.quantize(Decimal('0.00'), ROUND_HALF_UP)}%+GST"
    data["processingFee"] = f"{overall_pf.quantize(Decimal('0.00'), ROUND_HALF_UP)}%+GST"
    return json.dumps(data)

def update_bs2_charge_2(json_str, charge_value, backcalc_min, backcalc_max, is_flexi, applicable_processes=None):
    data = json.loads(json_str)
    data["chargeValue"] = float(charge_value.quantize(Decimal("0.00"), ROUND_HALF_UP))
    if applicable_processes is not None:
        data["applicableProcesses"] = applicable_processes
    if is_flexi:
        if "chargesMetaData" not in data or not isinstance(data["chargesMetaData"], dict):
            data["chargesMetaData"] = {}
        data["chargesMetaData"]["minPercentUnsecure"] = float(backcalc_min.quantize(Decimal("0.00"), ROUND_HALF_UP))
        data["chargesMetaData"]["maxPercentUnsecure"] = float(backcalc_max.quantize(Decimal("0.00"), ROUND_HALF_UP))
    else:
        data["chargeCalculationType"] = "fixed-percentage"
        data["chargeType"] = "processing-fee"
        data["percentageOn"] = "loanamount"
        if "chargesMetaData" in data:
            data.pop("chargesMetaData")
    return json.dumps(data)

def update_bs2_legal_name(text, tenure, encoding):
    updated = re.sub(r'\b(6M|7M|12M)\b', f'{tenure}M', str(text), count=1)
    if re.search(r'(th7\.si5|f8)', updated):
        updated = re.sub(r'(th7\.si5|f8)', encoding, updated, count=1)
    else:
        updated = re.sub(r'(48(?:\.00)?%|37\.65%)', encoding, updated, count=1)
    return updated

def update_foreclosure_charge(json_str, charge_value, duration_months, applicable_processes):
    fallback = {
        "name": "Foreclosure", "chargeType": "foreclosure", "chargeCalculationType": "fixed-percentage",
        "applicableProcesses": ["fresh-loan", "renewal", "release"], "chargeValue": 0, "maxValue": 100000,
        "cityId": None, "percentageOn": "loanamount", "chargesMetaData": {"duration": 2}, "minValue": 999
    }
    try:
        data = json.loads(json_str) if str(json_str).strip() else fallback
        if not isinstance(data, dict): data = dict(fallback)
    except: data = dict(fallback)

    data["name"] = "Foreclosure"
    data["chargeType"] = "foreclosure"
    data["chargeCalculationType"] = "fixed-percentage"
    data["percentageOn"] = "loanamount"
    data["maxValue"] = 100000
    data["minValue"] = 999
    data["cityId"] = None
    data["applicableProcesses"] = applicable_processes
    data["chargeValue"] = float(charge_value.quantize(Decimal("0.00"), ROUND_HALF_UP))

    if "chargesMetaData" not in data or not isinstance(data["chargesMetaData"], dict):
        data["chargesMetaData"] = {}
    data["chargesMetaData"]["duration"] = int(duration_months)
    return json.dumps(data)

def update_bs2_legal_name_pf_fc(text, pf_value, fc_duration_months, has_pf_in_refname, include_fc=True):
    updated = str(text).strip()
    updated = re.sub(r'\bFC(?:\s*[-:]?\s*\d+D)?\b', 'FC', updated, flags=re.IGNORECASE)

    if not include_fc:
        updated = re.sub(r'\bFC\b', '', updated, flags=re.IGNORECASE)
        if not has_pf_in_refname:
            updated = re.sub(r'\bPF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%\s*', '', updated, flags=re.IGNORECASE)
        return re.sub(r'\s+', ' ', updated).strip()

    rounded_fc_value = None
    if pf_value is not None:
        rounded_fc_value = pf_value.quantize(Decimal("0.1"), ROUND_HALF_UP).quantize(Decimal("0.00"))

    if not has_pf_in_refname:
        updated = re.sub(r'\bPF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%\s*', '', updated, flags=re.IGNORECASE)
        updated = re.sub(r'\s+', ' ', updated).strip()
        if rounded_fc_value is not None:
            fc_str = f"{rounded_fc_value}%"
            if re.search(r'\bFC\b', updated, re.IGNORECASE):
                updated = re.sub(r'\bFC\b', f'{fc_str} FC', updated, count=1, flags=re.IGNORECASE)
            else:
                updated = f"{updated} {fc_str}".strip()
    elif rounded_fc_value is not None:
        pf_str = f"{rounded_fc_value}%"
        if re.search(r'PF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%', updated, re.IGNORECASE):
            updated = re.sub(r'PF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%', f'PF {pf_str}', updated, count=1, flags=re.IGNORECASE)
        elif re.search(r'\bFC\b', updated, re.IGNORECASE):
            updated = re.sub(r'\bFC\b', f'PF {pf_str} FC', updated, count=1, flags=re.IGNORECASE)
        else:
            updated = f"{updated} PF {pf_str}".strip()

    if not re.search(r'\bFC\b', updated, re.IGNORECASE):
        updated = f"{updated} FC".strip()

    if int(fc_duration_months) == 3:
        updated = re.sub(r'\bFC\b', 'FC 90D', updated, count=1, flags=re.IGNORECASE)
    elif int(fc_duration_months) == 4:
        updated = re.sub(r'\bFC\b', 'FC 120D', updated, count=1, flags=re.IGNORECASE)

    return re.sub(r'\s+', ' ', updated).strip()

def _find_slab_list(node):
    if isinstance(node, dict):
        if "interestSlabs" in node and isinstance(node["interestSlabs"], list): return node["interestSlabs"]
        for value in node.values():
            found = _find_slab_list(value)
            if found is not None: return found
    elif isinstance(node, list):
        if len(node) >= 3 and all(isinstance(item, dict) for item in node):
            if any("interestRate" in item for item in node): return node
        for item in node:
            found = _find_slab_list(item)
            if found is not None: return found
    return None

def update_interest_json(json_str, slabs, tenure_days, slab_days=None):
    try: data = json.loads(json_str)
    except: return json_str

    slab_list = _find_slab_list(data)
    if slab_list:
        max_count = min(3, len(slab_list), len(slabs))
        for i in range(max_count):
            value = Decimal(slabs[i]).quantize(Decimal("0.00"), ROUND_HALF_UP)
            slab_list[i]["interestRate"] = float(value)
            if slab_days is not None and i < len(slab_days):
                slab_list[i]["fromDay"] = slab_days[i][0]
                slab_list[i]["toDay"] = slab_days[i][1]

        if slab_days is None:
            if slab_list and isinstance(slab_list[-1], dict):
                slab_list[-1]["toDay"] = tenure_days
        return json.dumps(data)

    if isinstance(data, dict) and "interestRate" in data and len(slabs) > 0:
        data["interestRate"] = float(Decimal(slabs[0]).quantize(Decimal("0.00"), ROUND_HALF_UP))
        if "toDay" in data: data["toDay"] = tenure_days
        return json.dumps(data)
    return json.dumps(data)

def get_applicable_processes_list(flow_text, refname, lm_appname=""):
    f = str(flow_text).strip().upper() if flow_text else ""
    r = str(refname).strip().lower() if refname else ""
    l = str(lm_appname).strip().lower() if lm_appname else ""
    combined = f"{r} {l}"
    
    if f == "RTN" or "retention" in combined:
        return ["renewal-retention"]
    elif f == "RWL":
        return ["renewal"]
    elif f == "FWD":
        return ["fresh-loan", "takeover-loan"]
    else:
        return ["fresh-loan", "release"]

def get_applicable_processes_string(flow_text, refname, lm_appname=""):
    return ",".join(get_applicable_processes_list(flow_text, refname, lm_appname))

def _normalize_process_list(val):
    parts = sorted([p.strip().lower() for p in str(val).split(",") if p.strip()])
    return ",".join(parts)


# ============================================================
# API FLATTENER (New Phase 2 Functionality)
# ============================================================

def parse_api_scheme_to_row(master_id, api_payload):
    """Takes the raw API JSON for a master scheme and builds a flat dict mapping to our CSV schema."""
    try:
        base = api_payload.get('data', {})
        if not base:
            return {"masterschemeid": master_id, "Error": "No data found"}

        display_configs = base.get('displayConfigs', [{}])
        display = display_configs[0].get('displayProperties', {}) if display_configs else {}
        refName = display.get('referenceName', '')

        ref_lower = refName.lower()
        lm_appname = base.get('schemeName', '').lower()
        combined_text = ref_lower + " " + lm_appname
        
        if "hip" in combined_text and "90d" in combined_text:
            prod_type = "HIP 90D Jumping"
        elif "hip" in combined_text:
            prod_type = "HIP 30D Jumping"
        elif "90d" in combined_text:
            prod_type = "90D Jumping"
        else:
            prod_type = "30D Jumping"

        row = {
            "masterschemeid": master_id,
            "refName": refName,
            "SchemeMin": base.get('eligibility', {}).get('ticketSizeMinAmount', ''),
            "SchemeMax": base.get('eligibility', {}).get('ticketSizeMaxAmount', ''),
            "tenure": base.get('tenure'),
            "customerLtv": base.get('customerLtv'),
            "applicableProcesses": ",".join(base.get('applicableProcesses', [])),
            "fulfillmentChannels": ",".join(base.get('fulfillmentChannels', [])),
            "OverallInterestCalculation": json.dumps(base.get('interestCalculation', {})),
            "schemeFlags": json.dumps(base.get('schemeFlags', {})),
            "Product Type": prod_type,
            "lm_appname": lm_appname
        }

        bs1, bs2 = None, None
        for bs in base.get('baseSchemes', []):
            if bs.get('type') == 'secure':
                bs1 = bs
            elif bs.get('type') == 'unsecure':
                bs2 = bs

        if bs1:
            row["bs1-legalName"] = bs1.get('legalName', '')
            row["bs1-ltv"] = bs1.get('ltv', '')
            if bs1.get('addons'):
                row["bs1-addon-1"] = json.dumps(bs1['addons'][0])
                
        if bs2:
            row["bs2-legalName"] = bs2.get('legalName', '')
            if bs2.get('addons'):
                row["bs2-addon-1"] = json.dumps(bs2['addons'][0])
            for c in bs2.get('charges', []):
                if c.get('chargeType') == 'processing-fee':
                    row["bs2-charge-2"] = json.dumps(c)
                elif c.get('chargeType') == 'foreclosure':
                    row["bs2-charge-3"] = json.dumps(c)

        return row
    except Exception as e:
        return {"masterschemeid": master_id, "Error": str(e)}

# ============================================================
# API PAYLOAD BUILDER (New Phase 3 Functionality)
# ============================================================
def build_creation_payload(row, api_template):
    if api_template == "Disable/Delete Scheme":
        m_id = str(row.get("masterschemeid", row.get("masterSchemeId", "")))
        dis_val = str(row.get("disable", "true")).strip().lower()
        if dis_val in ["nan", "none", ""]: dis_val = "true"
        return {
            "masterSchemeId": m_id,
            "disable": dis_val
        }

    payload = {}
    
    # Core string columns shared across all templates
    str_cols = {
        "SchemeName": "SchemeName", "refName": "refName", "refno": "refno",
        "SchemeMin": "SchemeMin", "SchemeMax": "SchemeMax", "CityIds": "CityIds",
        "applicableProcesses": "applicableProcesses", "fulfillmentChannels": "fulfillmentChannels",
        "goldBenchmark": "goldBenchmark", "description": "Description", "tenure": "tenure",
        "GroupTags": "GroupTags", "customerLtv": "customerLtv", "productCategory": "productCategory",
        "enabled": "enabled", "NoOfBaseScheme": "NoOfBaseScheme",
        "bs1-legalName": "bs1-legalName", "bs1-Lender": "bs1-Lender", "bs1-type": "bs1-type",
        "bs1-benchmark": "bs1-benchmark", "bs1-ltv": "bs1-ltv",
        "bs1-NoOfCharges": "bs1-NoOfCharges", "bs1-NoOfAddOns": "bs1-NoOfAddOns",
        "bs2-legalName": "bs2-legalName", "bs2-Lender": "bs2-Lender", "bs2-type": "bs2-type",
        "bs2-benchmark": "bs2-benchmark", "bs2-minThreshold": "bs2-minThreshold",
        "bs2-MaxThreshold": "bs2-maxThreshold", "bs2-LTV": "bs2-LTV",
        "bs2-NoOfCharges": "bs2-NoOfCharges", "bs2-NoOfAddOns": "bs2-NoOfAddOns"
    }
    
    # Core json columns shared across all templates
    json_cols = [
        "schemeFlags", "chargeText", "displayText", "OverallInterestCalculation",
        "bs1-calculation", "bs1-charge-1", "bs1-charge-2", "bs1-charge-3",
        "bs1-addon-1", "bs1-addon-2", "bs1-addon-3",
        "bs2-calculation", "bs2-charge-1", "bs2-charge-2", "bs2-charge-3",
        "bs2-addon-1", "bs2-addon-2", "bs2-addon-3"
    ]

    # Modify mappings based on user selection
    if api_template == "HIP (Default)":
        str_cols["bs1-tenure"] = "bs1-tenure"
        str_cols["bs2-tenure"] = "bs2-tenure"
    elif api_template == "Non-HIP (Fed)":
        # Missing bs1-tenure per API rules
        str_cols["bs2-tenure"] = "bs2-tenure"
    elif api_template == "CLM Models":
        # Missing bs1-tenure and bs2-tenure per API rules, but adding massive CLM blocks
        clm_str = {
            "bs1-securedLoanType": "bs1-securedLoanType",
            "bs1-noOfLenders": "bs1-noOfLenders",
            "bs1-orderOfPayment": "bs1-orderOfPayment",
            "bs2-orderOfPayment": "bs2-orderOfPayment",
            "cl1-legalName": "cl1-legalName",
            "cl1-lender": "cl1-lender",
            "cl1-lenderId": "cl1-lenderId",
            "cl1-noOfCharges": "cl1-noOfCharges",
            "cl1-noOfAddons": "cl1-noOfAddons",
            "cl1-principalShare": "cl1-principalShare",
            "cl2-legalName": "cl2-legalName",
            "cl2-lender": "cl2-lender",
            "cl2-lenderId": "cl2-lenderId",
            "cl2-noOfCharges": "cl2-noOfCharges",
            "cl2-noOfAddons": "cl2-noOfAddons",
            "cl2-principalShare": "cl2-principalShare"
        }
        str_cols.update(clm_str)

        clm_json = [
            "cl1-calculation", "cl1-charge-1", "cl1-charge-2",
            "cl1-addon-1", "cl1-addon-2",
            "cl2-calculation", "cl2-charge-1", "cl2-charge-2", "cl2-charge-3",
            "cl2-addon-1", "cl2-addon-2", "cl2-addon-3"
        ]
        json_cols.extend(clm_json)

    # Process String columns
    for csv_col, api_key in str_cols.items():
        val = row.get(csv_col, "")
        if pd.isna(val):
            val = ""
        if api_key == "enabled" and str(val).strip() == "":
            val = "true"
        if isinstance(val, float) and val.is_integer():
            val = int(val)
        payload[api_key] = str(val) if val != "" else ""

    # Process JSON columns
    for j_col in json_cols:
        val = row.get(j_col, "")
        if pd.isna(val) or str(val).strip() == "":
            continue 
        try:
            payload[j_col] = json.loads(str(val))
        except:
            payload[j_col] = str(val)
            
    # Process Array of JSON (Only for CLM Models)
    if api_template == "CLM Models":
        opf = row.get("OverallPF", "")
        if pd.notna(opf) and str(opf).strip() != "":
            try:
                parsed_opf = json.loads(str(opf))
                if isinstance(parsed_opf, list):
                    payload["OverallPF"] = parsed_opf
                else:
                    payload["OverallPF"] = [parsed_opf]
            except:
                payload["OverallPF"] = [str(opf)]

    return payload


# ============================================================
# PHASE 1: GENERATE
# ============================================================

with tab1:
    st.subheader("Input Parameters")

    with st.expander("Demo constructs available", expanded=False):
        c1, c2 = st.columns([1, 1])
        c1.download_button("Download Flexi demo JSON", json.dumps(DUMMY_FLEXI_CONSTRUCT, indent=2), "Flexi_pf_demo.json", mime="application/json")
        c2.download_button("Download Fixed demo JSON", json.dumps(DUMMY_FIXED_CONSTRUCT, indent=2), "fixed_pf_demo.json", mime="application/json")

    FLOW_OPTIONS = ["Non HIP", "HIP"]
    if "selected_flow" not in st.session_state:
        st.session_state.selected_flow = FLOW_OPTIONS[0]

    c_flow, c_mode = st.columns([1, 2])
    selected_flow = c_flow.selectbox("Flow", options=FLOW_OPTIONS, index=FLOW_OPTIONS.index(st.session_state.selected_flow), key="flow_selector")
    st.session_state.selected_flow = selected_flow
    is_hip_flow = selected_flow == "HIP"
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    MODE_OPTIONS = ["Standard", "SPS Mode", "GPA Mode"]
    selected_mode = st.radio("Configuration Mode", MODE_OPTIONS, horizontal=True)
    
    is_sps = selected_mode in ["SPS Mode", "GPA Mode"]
    is_gpa = selected_mode == "GPA Mode"
    
    if is_gpa:
        st.info("""
        **GPA Allowed SEC LTVs & Legal Names:**
        * **6M:** 60, 64, 65, 66, 67 `->` Rupeek Delight
        * **7M:** 60, 64, 65, 66 `->` Rupeek Royal
        * **12M:** 60 `->` Rupeek Delight OR Rupeek Royal
        """)

    JUMPING_COLUMNS = [
        "customerLtv", "TS", "slab1 ROI", "PF Tag", "Flow", "PF val", "Tenure", "Product Type", "Scheme End Tag", 
        "SchemeMin Input", "SchemeMax Input", "FC Value", "Custom Sec LTV", "Custom Legal Name", "Is Balance Scheme", "Include FC"
    ]
    HIP_COLUMNS = [
        "customerLtv", "TS", "slab1 ROI", "PF Tag", "Flow", "PF val", "Tenure", "Product Type", "Scheme End Tag", 
        "SchemeMin Input", "SchemeMax Input", "FC Value", "Custom Sec LTV", "Custom Legal Name", "Is Balance Scheme", "Include FC"
    ]

    JUMPING_DEFAULTS = pd.DataFrame([
        {"customerLtv": 77, "TS": "6-12L", "slab1 ROI": 1.09, "PF Tag": "Nopf",  "Flow": "RWL", "PF val": "0.50%",      "Tenure": 6,  "Product Type": "30D Jumping", "Scheme End Tag": "", "SchemeMin Input": "", "SchemeMax Input": "", "FC Value": "1.00", "Custom Sec LTV": "", "Custom Legal Name": "", "Is Balance Scheme": False, "Include FC": True},
        {"customerLtv": 77, "TS": "3-6L",  "slab1 ROI": 1.09, "PF Tag": "Flexi", "Flow": "FWD", "PF val": "0.1%-0.70%", "Tenure": 12, "Product Type": "90D Jumping", "Scheme End Tag": "", "SchemeMin Input": "", "SchemeMax Input": "", "FC Value": "1.00", "Custom Sec LTV": "", "Custom Legal Name": "", "Is Balance Scheme": False, "Include FC": True},
    ], columns=JUMPING_COLUMNS)

    HIP_DEFAULTS = pd.DataFrame([
        {"customerLtv": 75, "TS": "<3L", "slab1 ROI": 1.19, "PF Tag": "Flexi", "Flow": "FWD", "PF val": "0.70%-1.00%", "Tenure": 7, "Product Type": "30D Jumping", "Scheme End Tag": "", "SchemeMin Input": "", "SchemeMax Input": "", "FC Value": "1.00", "Custom Sec LTV": "", "Custom Legal Name": "", "Is Balance Scheme": False, "Include FC": True},
    ], columns=HIP_COLUMNS)

    state_key = "input_df_HIP" if is_hip_flow else "input_df_Jumping"
    if state_key not in st.session_state:
        st.session_state[state_key] = (HIP_DEFAULTS if is_hip_flow else JUMPING_DEFAULTS).copy()

    col_config = {
        "Product Type": st.column_config.SelectboxColumn(options=["30D Jumping", "90D Jumping"]),
        "PF Tag": st.column_config.SelectboxColumn(options=["Flexi", "Fixed", "Nopf"]),
        "Flow":   st.column_config.SelectboxColumn(options=["FWD", "RWL", "RTN"]),
        "Custom Sec LTV": st.column_config.SelectboxColumn(options=["", "60", "64", "65", "66", "67"]),
        "Custom Legal Name": st.column_config.SelectboxColumn(options=["", "Rupeek Delight", "Rupeek Royal"]),
        "Is Balance Scheme": st.column_config.CheckboxColumn("Balance Scheme", default=False)
    }
    
    if not is_gpa:
        col_config["Custom Sec LTV"] = None
        col_config["Custom Legal Name"] = None
    if not is_sps:
        col_config["SchemeMin Input"] = None
        col_config["SchemeMax Input"] = None
        col_config["FC Value"] = None

    if is_hip_flow: st.write("Enter HIP rows below:")
    else: st.write("Enter Jumping rows below:")
        
    edited_input_df = st.data_editor(
        st.session_state[state_key],
        use_container_width=True,
        num_rows="dynamic",
        column_config=col_config,
        key=f"editor_{state_key}"
    )

    if st.button("Compute"):
        st.session_state[state_key] = edited_input_df.copy()
        cleaned_input_df = _drop_empty_rows(edited_input_df)
        if cleaned_input_df.empty:
            st.warning("Please enter at least one input row before computing.")
            st.stop()

        cleaned_input_df = cleaned_input_df.copy()
        if is_hip_flow:
            cleaned_input_df["Product Type"] = cleaned_input_df["Product Type"].apply(
                lambda x: f"HIP {x}" if pd.notna(x) and str(x).strip() else "HIP"
            )

        df = build_working_dataframe(cleaned_input_df)
        for checker_column in CHECKER_COLUMNS:
            if checker_column not in df.columns:
                df[checker_column] = ""

        for idx in df.index:
            row = df.loc[idx]
            refname = str(df.at[idx, "refName"]).strip()

            configured_overall_ir = _extract_interest_rates(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else []
            configured_secure_ir = _extract_interest_rates(df.at[idx, "bs1-addon-1"]) if "bs1-addon-1" in df.columns else []
            configured_unsecure_ir = _extract_interest_rates(df.at[idx, "bs2-addon-1"]) if "bs2-addon-1" in df.columns else []
            configured_tenure = df.at[idx, "tenure"] if "tenure" in df.columns else ""
            configured_todayslab3 = _extract_slab3_today(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else None
            configured_secure_ltv = df.at[idx, "bs1-ltv"] if "bs1-ltv" in df.columns else ""
            configured_bs1_legalname = df.at[idx, "bs1-legalName"] if "bs1-legalName" in df.columns else ""
            configured_pf_min, configured_pf_max = _extract_pf_config_values(df.at[idx, "bs2-charge-2"]) if "bs2-charge-2" in df.columns else (None, None)
            configured_fc = _extract_charge_value(df.at[idx, "bs2-charge-3"]) if "bs2-charge-3" in df.columns else None

            input_ltv = _get_row_value(row, ["customerLtv", "customer_ltv", "ltv"])
            input_ts_tag = _get_row_value(row, ["TS", "ts", "ticket_size", "ticketSize"])
            input_scheme_min, input_scheme_max = _ts_label_to_min_max(input_ts_tag)
            
            input_scheme_min_custom = _parse_int(_get_row_value(row, ["SchemeMin Input", "custom_schememin"]))
            if input_scheme_min_custom is not None: input_scheme_min = input_scheme_min_custom
            
            input_scheme_max_custom = _parse_int(_get_row_value(row, ["SchemeMax Input", "custom_schememax"]))
            if input_scheme_max_custom is not None: input_scheme_max = input_scheme_max_custom
            
            custom_sec_ltv_input = _parse_decimal(_get_row_value(row, ["Custom Sec LTV"]))
            cln_raw = _get_row_value(row, ["Custom Legal Name"])
            custom_legal_name_input = str(cln_raw).strip() if cln_raw is not None and not pd.isna(cln_raw) else ""
            input_balance = _get_row_value(row, ["Is Balance Scheme", "is_balance_scheme"])
            
            # --- BLOCK INVALID LTV + ROYAL CONFIGURATION ---
            if is_gpa and "royal" in custom_legal_name_input.lower() and custom_sec_ltv_input == Decimal("67"):
                st.error(f"🛑 Error on Row {idx + 1}: 'Rupeek Royal' cannot have a Secure LTV of 67. Please change the Custom Sec LTV to 66 or lower, or change the Legal Name to 'Rupeek Delight'.")
                st.stop()
            # -----------------------------------------------
            
            input_roi = _get_row_value(row, ["slab1 ROI", "slab1ROI", "roi", "slab1_opp"])
            input_pf_tag = _get_row_value(row, ["PF Tag", "pf_tag", "pfTag"])
            input_flow = _get_row_value(row, ["Flow", "flow"])
            input_pf_val = _get_row_value(row, ["PF val", "PF Value", "pf_val", "pfValue"])
            input_tenure = _get_row_value(row, ["Tenure", "tenure"])
            input_product_type = _get_row_value(row, ["Product Type", "product_type", "productType"])
            
            pt_lower = str(input_product_type).strip().lower() if input_product_type is not None else ""
            is_90d_jumping = "90d" in pt_lower
            is_hip = "hip" in pt_lower

            # Inject schemeFlags dynamically based on selections
            flags_dict = {"isLoanCalcSplitv2Enabled": True}
            if is_hip: flags_dict["isHIP"] = True
            if input_balance: flags_dict["renewalAtOutstanding"] = True
            df.at[idx, "schemeFlags"] = json.dumps(flags_dict)

            refname = _update_refname_ltv_code(refname, input_ltv)
            refname = _update_refname_opp(refname, input_roi)
            refname = _update_refname_pf(refname, input_pf_tag, input_pf_val)
            refname = _update_refname_flow(refname, input_flow)
            refname = _update_refname_ts(refname, input_ts_tag)

            input_scheme_end_tag = _get_row_value(row, ["Scheme End Tag", "scheme_end_tag", "schemeEndTag"])
            if input_scheme_end_tag is not None and str(input_scheme_end_tag).strip():
                end_tag = str(input_scheme_end_tag).strip()
                refname = re.sub(
                    r'(<\s*3L|3\s*-\s*6L|6\s*-\s*12L|>\s*12L|ALL\s*TS)(\s+\S.*)?$',
                    lambda m: m.group(1) + ' ' + end_tag, refname, count=1, flags=re.IGNORECASE)
                refname = re.sub(r'\s+', ' ', refname).strip()

            overall_ltv = _parse_decimal(input_ltv) or extract_ltv_from_code(refname)
            requested_tenure = _parse_int(input_tenure) or extract_tenure(refname)
            monthly_opp = _parse_decimal(input_roi) or extract_opp(refname)

            pf_min, pf_max = _parse_pf_value(input_pf_val)
            if pf_min is None:
                pf_min, pf_max = extract_pf_range(refname)
                if pf_min is None:
                    extracted_pf = extract_pf(refname)
                    pf_min, pf_max = extracted_pf, extracted_pf

            pf_tag_text = str(input_pf_tag).strip().lower() if input_pf_tag is not None else ""
            is_nopf = pf_tag_text == "nopf"
            if is_nopf and "bs2-charge-2" in df.columns:
                configured_fc = _extract_charge_value(df.at[idx, "bs2-charge-2"])
            overall_pf = None if is_nopf else (pf_max if pf_max is not None else pf_min)

            if not all([overall_ltv, requested_tenure, monthly_opp]) or (not is_nopf and overall_pf is None):
                continue

            df.at[idx, "SchemeMin"] = _parse_int(input_scheme_min) or df.at[idx, "SchemeMin"]
            df.at[idx, "SchemeMax"] = _parse_int(input_scheme_max) or df.at[idx, "SchemeMax"]
            df.at[idx, "customerLtv"] = float(overall_ltv)

            scheme, final_tenure = decision_engine(overall_ltv, monthly_opp, requested_tenure)
            if is_90d_jumping: scheme = "Royal"

            secure_ltv_override_val = None
            if is_gpa and custom_sec_ltv_input is not None: secure_ltv_override_val = custom_sec_ltv_input
                
            if is_gpa and custom_legal_name_input:
                if "delight" in custom_legal_name_input.lower(): scheme = "Delight"
                elif "royal" in custom_legal_name_input.lower(): scheme = "Royal"

            df.at[idx, "tenure"] = final_tenure
            if "Tenure" in df.columns: df.at[idx, "Tenure"] = final_tenure
            if "bs1-tenure" in df.columns: df.at[idx, "bs1-tenure"] = final_tenure
            if is_hip:
                hip_unsecure_tenure = 48 if final_tenure in (12,6,7) else 24
                if "bs2-tenure" in df.columns: df.at[idx, "bs2-tenure"] = hip_unsecure_tenure
            else:
                if "bs2-tenure" in df.columns: df.at[idx, "bs2-tenure"] = final_tenure
            
            refname = update_refname_tenure(refname, final_tenure)
            refname = re.sub(r'(%)(\d{1,2}M\b)', r'\1 \2', refname)
            df.at[idx, "refName"] = refname
            if "refno" in df.columns: df.at[idx, "refno"] = refname

            if "description" in df.columns:
                flow_text = str(input_flow).strip().upper() if input_flow is not None else ""
                if flow_text == "RWL": df.at[idx, "description"] = "RWL"
                elif flow_text == "FWD": df.at[idx, "description"] = "FL TO"
                elif flow_text == "RTN": df.at[idx, "description"] = "RTN"

            app_proc_list = get_applicable_processes_list(input_flow, refname)
            if "applicableProcesses" in df.columns:
                df.at[idx, "applicableProcesses"] = ",".join(app_proc_list)

            if is_gpa and custom_legal_name_input:
                df.at[idx, "bs1-legalName"] = "Rupeek Delight" if "delight" in custom_legal_name_input.lower() else "Rupeek Royal"
            elif is_90d_jumping: df.at[idx, "bs1-legalName"] = "Rupeek Ultra"
            else: df.at[idx, "bs1-legalName"] = f"Rupeek {scheme}"

            result = interest_engine(scheme, final_tenure, overall_ltv, monthly_opp, secure_ltv_override=secure_ltv_override_val)

            if "bs1-ltv" in df.columns: df.at[idx, "bs1-ltv"] = float(result["secure_ltv"])

            tenure_days = get_tenure_days(final_tenure)
            slab_days_override = [(0, 90), (91, 120), (121, int(final_tenure) * 30)] if is_90d_jumping else None

            df.at[idx, "OverallInterestCalculation"] = update_interest_json(df.at[idx, "OverallInterestCalculation"], result["overall_slabs"], tenure_days, slab_days=slab_days_override)
            df.at[idx, "bs1-addon-1"] = update_interest_json(df.at[idx, "bs1-addon-1"], result["secure_slabs"], tenure_days, slab_days=slab_days_override)

            bs2_addon1_tenure_days = (48 if final_tenure in (12,6,7) else 24)*30 if is_hip else tenure_days
            bs2_slab_days_override = [(0, 90), (91, 120), (121, bs2_addon1_tenure_days)] if is_90d_jumping else None

            df.at[idx, "bs2-addon-1"] = update_interest_json(df.at[idx, "bs2-addon-1"], result["unsecure_slabs"], bs2_addon1_tenure_days, slab_days=bs2_slab_days_override)
            if "bs2-calculation" in df.columns:
                df.at[idx, "bs2-calculation"] = update_interest_json(df.at[idx, "bs2-calculation"], result["unsecure_slabs"], bs2_addon1_tenure_days, slab_days=bs2_slab_days_override)

            secure_ltv = result["secure_ltv"]
            denominator = Decimal("1") - (secure_ltv / overall_ltv)
            if denominator == 0: continue

            min_pf_input = pf_min if pf_min is not None else overall_pf
            max_pf_input = pf_max if pf_max is not None else overall_pf
            min_unsecure_pf = (min_pf_input / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP) if min_pf_input is not None else None
            max_unsecure_pf = (max_pf_input / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP) if max_pf_input is not None else None

            flow_pf_tag = str(input_pf_tag).strip().lower() if input_pf_tag is not None else ""
            refname_lower = str(df.at[idx, "refName"]).lower()
            is_flexi = flow_pf_tag == "flexi" or any(token in refname_lower for token in ["flexipf", "flexi pf", "flexi-pf"])
            charge_pf_value = (max_unsecure_pf if is_flexi else min_unsecure_pf) if not is_nopf else None

            if is_nopf:
                if "chargeText" in df.columns: df.at[idx, "chargeText"] = "{}"
            else:
                if "chargeText" in df.columns and charge_pf_value is not None:
                    charge_text_overall_pf = max_pf_input if is_flexi else min_pf_input
                    df.at[idx, "chargeText"] = update_charge_text(df.at[idx, "chargeText"], charge_pf_value, charge_text_overall_pf)
                if "bs2-charge-2" in df.columns and charge_pf_value is not None and min_unsecure_pf is not None and max_unsecure_pf is not None:
                    df.at[idx, "bs2-charge-2"] = update_bs2_charge_2(df.at[idx, "bs2-charge-2"], charge_pf_value, min_unsecure_pf, max_unsecure_pf, is_flexi, applicable_processes=app_proc_list)

            include_fc_input = _get_row_value(row, ["Include FC"])
            if pd.isna(include_fc_input) or str(include_fc_input).strip() == "": include_fc = True
            elif str(include_fc_input).strip().lower() in ["false", "0", "no"]: include_fc = False
            else: include_fc = bool(include_fc_input)
            
            custom_fc_val_input = _get_row_value(row, ["FC Value", "FC Val"])
            if pd.notna(custom_fc_val_input) and str(custom_fc_val_input).strip(): foreclosure_overall = _parse_decimal(custom_fc_val_input) or Decimal("1.00")
            else: foreclosure_overall = Decimal("1.00")
            
            foreclosure_duration = 3 if final_tenure in (6, 7, 12) else 2

            if include_fc:
                foreclosure_unsecure = (foreclosure_overall / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP)
                if is_nopf:
                    if "bs2-charge-2" in df.columns: df.at[idx, "bs2-charge-2"] = update_foreclosure_charge(df.at[idx, "bs2-charge-2"], foreclosure_unsecure, foreclosure_duration, app_proc_list)
                    if "bs2-charge-3" in df.columns: df.at[idx, "bs2-charge-3"] = "{}"
                    if "bs2-NoOfCharges" in df.columns: df.at[idx, "bs2-NoOfCharges"] = 2
                else:
                    if "bs2-charge-3" in df.columns: df.at[idx, "bs2-charge-3"] = update_foreclosure_charge(df.at[idx, "bs2-charge-3"], foreclosure_unsecure, foreclosure_duration, app_proc_list)
                    if "bs2-NoOfCharges" in df.columns: df.at[idx, "bs2-NoOfCharges"] = 3
            else:
                foreclosure_unsecure = None
                if is_nopf:
                    if "bs2-charge-2" in df.columns: df.at[idx, "bs2-charge-2"] = "{}"
                    if "bs2-charge-3" in df.columns: df.at[idx, "bs2-charge-3"] = "{}"
                    if "bs2-NoOfCharges" in df.columns: df.at[idx, "bs2-NoOfCharges"] = 1
                else:
                    if "bs2-charge-3" in df.columns: df.at[idx, "bs2-charge-3"] = "{}"
                    if "bs2-NoOfCharges" in df.columns: df.at[idx, "bs2-NoOfCharges"] = 2
                                
            if "bs2-legalName" in df.columns:
                if is_hip:
                    hip_unsecure_tenure = 48 if final_tenure in (12,6,7) else 24
                    hip_encoding = "th7.si5" if final_tenure == 12 else "f8"
                    hip_ln = str(df.at[idx, "bs2-legalName"])
                    if re.search(r'th7\.si5|f8', hip_ln, re.IGNORECASE): hip_ln = re.sub(r'th7\.si5|f8', hip_encoding, hip_ln, count=1, flags=re.IGNORECASE)
                    else: hip_ln = re.sub(r'48(?:\.00)?%|37\.65%', hip_encoding, hip_ln, count=1)
                    hip_ln = re.sub(r'\b(?:24|48)M\b', f'{hip_unsecure_tenure}M', hip_ln, count=1, flags=re.IGNORECASE)
                    
                    if include_fc:
                        fc_str_hip = str(((foreclosure_unsecure * 2).quantize(Decimal("1"), ROUND_HALF_UP) / 2).quantize(Decimal("0.00")))
                        hip_ln = re.sub(r'(PF\s+HIP\s+)[0-9]+(?:\.[0-9]+)?(%)', rf'\g<1>{fc_str_hip}\g<2>', hip_ln, count=1, flags=re.IGNORECASE)
                        hip_ln = re.sub(r'\bFC(?:\s*[-:]?\s*\d+D)?\b', 'FC', hip_ln, flags=re.IGNORECASE)
                        if int(foreclosure_duration) == 3: hip_ln = re.sub(r'\bFC\b', 'FC 90D', hip_ln, count=1, flags=re.IGNORECASE)
                        elif int(foreclosure_duration) == 4: hip_ln = re.sub(r'\bFC\b', 'FC 120D', hip_ln, count=1, flags=re.IGNORECASE)
                    else:
                        hip_ln = re.sub(r'(PF\s+HIP\s+)[0-9]+(?:\.[0-9]+)?(%)', '', hip_ln, flags=re.IGNORECASE)
                        hip_ln = re.sub(r'\bFC(?:\s*[-:]?\s*\d+D)?\b', '', hip_ln, flags=re.IGNORECASE)
                        
                    df.at[idx, "bs2-legalName"] = re.sub(r'\s+', ' ', hip_ln).strip()
                else:
                    encoding = "th7.si5" if final_tenure == 12 else "f8"
                    updated_legal_name = update_bs2_legal_name(df.at[idx, "bs2-legalName"], final_tenure, encoding)
                    has_pf_in_name = not is_nopf
                    df.at[idx, "bs2-legalName"] = update_bs2_legal_name_pf_fc(updated_legal_name, foreclosure_unsecure, foreclosure_duration, has_pf_in_name, include_fc=include_fc)

            configured_overall_ir_now = _extract_interest_rates(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else []
            configured_secure_ir_now = _extract_interest_rates(df.at[idx, "bs1-addon-1"]) if "bs1-addon-1" in df.columns else []
            configured_unsecure_ir_now = _extract_interest_rates(df.at[idx, "bs2-addon-1"]) if "bs2-addon-1" in df.columns else []
            configured_tenure_now = df.at[idx, "tenure"] if "tenure" in df.columns else ""
            configured_todayslab3_now = _extract_slab3_today(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else None
            configured_secure_ltv_now = df.at[idx, "bs1-ltv"] if "bs1-ltv" in df.columns else ""
            configured_bs1_legalname_now = df.at[idx, "bs1-legalName"] if "bs1-legalName" in df.columns else ""

            configured_pf_min_now, configured_pf_max_now = (None, None)
            if "bs2-charge-2" in df.columns: configured_pf_min_now, configured_pf_max_now = _extract_pf_config_values(df.at[idx, "bs2-charge-2"])

            if is_nopf and "bs2-charge-2" in df.columns: configured_fc_now = _extract_charge_value(df.at[idx, "bs2-charge-2"])
            elif "bs2-charge-3" in df.columns: configured_fc_now = _extract_charge_value(df.at[idx, "bs2-charge-3"])
            else: configured_fc_now = None

            df.at[idx, "CHK PF Config Min"] = str(configured_pf_min_now) if configured_pf_min_now is not None else ""
            df.at[idx, "CHK PF Config Max"] = str(configured_pf_max_now) if configured_pf_max_now is not None else ""
            df.at[idx, "CHK PF Calc Min"] = str(min_unsecure_pf) if min_unsecure_pf is not None else ""
            df.at[idx, "CHK PF Calc Max"] = str(max_unsecure_pf) if max_unsecure_pf is not None else ""
            df.at[idx, "CHK FC Config"] = str(configured_fc_now) if configured_fc_now is not None else "None"
            df.at[idx, "CHK FC Calc"] = str(foreclosure_unsecure) if foreclosure_unsecure is not None else "None"
            df.at[idx, "CHK Overall IR Config"] = ",".join(configured_overall_ir_now)
            df.at[idx, "CHK Overall IR Calc"] = ",".join([str(x) for x in result["overall_slabs"]])
            df.at[idx, "CHK Secure IR Config"] = ",".join(configured_secure_ir_now)
            df.at[idx, "CHK Secure IR Calc"] = ",".join([str(x) for x in result["secure_slabs"]])
            df.at[idx, "CHK Unsecure IR Config"] = ",".join(configured_unsecure_ir_now)
            df.at[idx, "CHK Unsecure IR Calc"] = ",".join([str(x) for x in result["unsecure_slabs"]])
            df.at[idx, "CHK Tenure Config"] = str(configured_tenure_now)
            df.at[idx, "CHK Tenure Calc"] = str(final_tenure)
            df.at[idx, "CHK Slab3 ToDay Config"] = str(configured_todayslab3_now) if configured_todayslab3_now is not None else ""
            df.at[idx, "CHK Slab3 ToDay Calc"] = str(tenure_days)
            df.at[idx, "CHK Secure LTV Config"] = str(configured_secure_ltv_now)
            df.at[idx, "CHK Secure LTV Calc"] = str(result["secure_ltv"])
            df.at[idx, "CHK BS1 LegalName Config"] = str(configured_bs1_legalname_now)
            df.at[idx, "CHK BS1 LegalName Calc"] = str(df.at[idx, "bs1-legalName"]) if "bs1-legalName" in df.columns else ""

            df.at[idx, "CHK ApplicableProcesses Config"] = str(df.at[idx, "applicableProcesses"]) if "applicableProcesses" in df.columns else ""
            df.at[idx, "CHK ApplicableProcesses Calc"] = ",".join(app_proc_list)
            
            # --- BALANCE SCHEME FLAG CHECK ---
            flags_str = str(df.at[idx, "schemeFlags"]) if "schemeFlags" in df.columns else "{}"
            try: flags_dict = json.loads(flags_str)
            except: flags_dict = {}
            cfg_balance = "True" if flags_dict.get("renewalAtOutstanding") in [True, "true", "True"] else "False"
            
            name_for_bal = refname.lower() + " " + str(df.at[idx, "lm_appname"]).lower() if "lm_appname" in df.columns else refname.lower()
            calc_balance = "True" if "balance" in name_for_bal else "False"
            
            df.at[idx, "CHK Balance Config"] = cfg_balance
            df.at[idx, "CHK Balance Calc"] = calc_balance

        df = finalize_output_columns(df)
        st.session_state.df = df
        
        # Clear out any old checked/rectified data to prevent ghost data passing to Phase 3
        if "checked_df" in st.session_state: del st.session_state["checked_df"]
        if "rectified_df" in st.session_state: del st.session_state["rectified_df"]
            
        st.success("Computation Complete")
        st.subheader("Updated Schemes")
        st.dataframe(df, use_container_width=True)

    if "df" in st.session_state:
        st.download_button("Download Updated CSV", st.session_state.df.to_csv(index=False), "updated_scheme.csv")


# ============================================================
# PHASE 2: CHECK & RECTIFY (With API Flow)
# ============================================================

with tab2:
    st.subheader("Phase 2 — Checker & Rectifier")
    st.info("📊 **Database Logs:** Validation results are automatically stored in `temp.scheme_validation_logs`.")
    
    phase_2_mode = st.radio("Select Data Source", ["Use Phase 1 Output", "Upload CSV", "Live DB / API Fetch"], horizontal=True)

    st.markdown("#### Validation Rules & View Options")
    col_opts1, col_opts2 = st.columns(2)
    with col_opts1:
        advanced_mode_chk = st.radio("Advanced Options", ["Standard", "SPS Mode", "GPA Mode"], horizontal=True, key="adv_mode_chk")
    with col_opts2:
        exclude_fc_check = st.checkbox("Exclude FC from Validation Status", value=False, key="excl_fc_chk")
        show_errors_only = st.checkbox("Show Only Schemes with Errors", value=False, key="show_err_chk")

    CHECK_FIELDS = [
        ("Overall IR",    "CHK Overall IR Config",    "CHK Overall IR Calc"),
        ("Secure IR",     "CHK Secure IR Config",     "CHK Secure IR Calc"),
        ("Unsecure IR",   "CHK Unsecure IR Config",   "CHK Unsecure IR Calc"),
        ("Tenure",        "CHK Tenure Config",        "CHK Tenure Calc"),
        ("Slab3 ToDay",   "CHK Slab3 ToDay Config",   "CHK Slab3 ToDay Calc"),
        ("Secure LTV",    "CHK Secure LTV Config",    "CHK Secure LTV Calc"),
        ("PF Min",        "CHK PF Config Min",        "CHK PF Calc Min"),
        ("PF Max",        "CHK PF Config Max",        "CHK PF Calc Max"),
        ("FC",            "CHK FC Config",            "CHK FC Calc"),
        ("BS1 LegalName", "CHK BS1 LegalName Config", "CHK BS1 LegalName Calc"),
        ("BS2 LegalName", "CHK BS2 LegalName Config", "CHK BS2 LegalName Calc"),
        ("ApplicableProcesses", "CHK ApplicableProcesses Config", "CHK ApplicableProcesses Calc"),
        ("Balance Flag",  "CHK Balance Config",       "CHK Balance Calc")
    ]

    def _run_checker_on_df(raw_df, product_type_fallback="30D Jumping", advanced_mode_chk="Standard"):
        df = raw_df.copy()
        for checker_column in CHECKER_COLUMNS:
            if checker_column not in df.columns:
                df[checker_column] = ""

        for idx in df.index:
            row = df.loc[idx]
            refname = str(df.at[idx, "refName"]).strip() if "refName" in df.columns else ""

            overall_ltv    = extract_ltv_from_code(refname)
            requested_tenure = extract_tenure(refname)
            monthly_opp    = extract_opp(refname)
            pf_min, pf_max = extract_pf_range(refname)
            if pf_min is None:
                extracted_pf = extract_pf(refname)
                pf_min, pf_max = extracted_pf, extracted_pf

            if "customerLtv" in df.columns:
                ltv_col = _parse_decimal(df.at[idx, "customerLtv"])
                if ltv_col is not None: overall_ltv = ltv_col

            if "tenure" in df.columns:
                ten_col = _parse_int(df.at[idx, "tenure"])
                if ten_col is not None: requested_tenure = ten_col

            if not all([overall_ltv, requested_tenure, monthly_opp]):
                continue

            row_prod_type = str(df.at[idx, "Product Type"]).strip() if "Product Type" in df.columns else ""
            if not row_prod_type or str(row_prod_type).lower() == "nan":
                row_prod_type = product_type_fallback

            is_hip_chk = "hip" in row_prod_type.lower()
            is_90d_chk = "90d" in row_prod_type.lower()
            is_nopf_chk  = not bool(re.search(r"PF\s*[-:]?\s*[0-9]", refname, re.IGNORECASE))

            overall_pf = None if is_nopf_chk else (pf_max if pf_max is not None else pf_min)

            if is_90d_chk:
                scheme_chk = "Royal"
                final_ten_chk = requested_tenure
            else:
                scheme_chk, final_ten_chk = decision_engine(overall_ltv, monthly_opp, requested_tenure)

            secure_ltv_ov = None
            custom_ln_chk = ""
            
            if advanced_mode_chk == "GPA Mode":
                custom_sec_ltv_chk = _parse_decimal(df.at[idx, "Custom Sec LTV"]) if "Custom Sec LTV" in df.columns else None
                cln_raw_chk = df.at[idx, "Custom Legal Name"] if "Custom Legal Name" in df.columns else None
                custom_ln_chk = str(cln_raw_chk).strip() if pd.notna(cln_raw_chk) else ""
                
                if custom_sec_ltv_chk is None and "bs1-ltv" in df.columns:
                    custom_sec_ltv_chk = _parse_decimal(df.at[idx, "bs1-ltv"])
                if not custom_ln_chk and "bs1-legalName" in df.columns:
                    cln_raw_chk2 = df.at[idx, "bs1-legalName"]
                    custom_ln_chk = str(cln_raw_chk2).strip() if pd.notna(cln_raw_chk2) else ""

                if custom_sec_ltv_chk is not None: secure_ltv_ov = custom_sec_ltv_chk
                if custom_ln_chk:
                    if "delight" in custom_ln_chk.lower(): scheme_chk = "Delight"
                    elif "royal" in custom_ln_chk.lower(): scheme_chk = "Royal"

            result = interest_engine(scheme_chk, final_ten_chk, overall_ltv, monthly_opp, secure_ltv_override=secure_ltv_ov)
            tenure_days_chk = get_tenure_days(final_ten_chk)
            secure_ltv    = result["secure_ltv"]
            denominator   = Decimal("1") - (secure_ltv / overall_ltv)
            if denominator == 0: continue

            min_pf_i = pf_min if pf_min is not None else overall_pf
            max_pf_i = pf_max if pf_max is not None else overall_pf
            min_unsecure_pf_chk = (min_pf_i / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP) if min_pf_i is not None else None
            max_unsecure_pf_chk = (max_pf_i / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP) if max_pf_i is not None else None

            include_fc_chk_val = df.at[idx, "Include FC"] if "Include FC" in df.columns else True
            if pd.isna(include_fc_chk_val) or str(include_fc_chk_val).strip() == "": include_fc_chk = True
            elif str(include_fc_chk_val).strip().lower() in ["false", "0", "no"]: include_fc_chk = False
            else: include_fc_chk = bool(include_fc_chk_val)
            
            fc_val_chk_val = df.at[idx, "FC Value"] if "FC Value" in df.columns else "1.00"
            if pd.notna(fc_val_chk_val) and str(fc_val_chk_val).strip(): fc_overall_chk = _parse_decimal(fc_val_chk_val) or Decimal("1.00")
            else: fc_overall_chk = Decimal("1.00")

            if include_fc_chk: fc_unsecure_chk = (fc_overall_chk / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP)
            else: fc_unsecure_chk = None

            cfg_overall_ir  = _extract_interest_rates(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else []
            cfg_secure_ir   = _extract_interest_rates(df.at[idx, "bs1-addon-1"]) if "bs1-addon-1" in df.columns else []
            cfg_unsec_ir    = _extract_interest_rates(df.at[idx, "bs2-addon-1"]) if "bs2-addon-1" in df.columns else []
            cfg_tenure      = df.at[idx, "tenure"] if "tenure" in df.columns else ""
            cfg_slab3today  = _extract_slab3_today(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else None
            cfg_secure_ltv  = df.at[idx, "bs1-ltv"] if "bs1-ltv" in df.columns else ""
            cfg_bs1_ln      = df.at[idx, "bs1-legalName"] if "bs1-legalName" in df.columns else ""
            cfg_pf_min_v, cfg_pf_max_v = _extract_pf_config_values(df.at[idx, "bs2-charge-2"]) if "bs2-charge-2" in df.columns else (None, None)
            
            if is_nopf_chk and "bs2-charge-2" in df.columns: cfg_fc_v = _extract_charge_value(df.at[idx, "bs2-charge-2"])
            elif "bs2-charge-3" in df.columns: cfg_fc_v = _extract_charge_value(df.at[idx, "bs2-charge-3"])
            else: cfg_fc_v = None

            df.at[idx, "CHK Overall IR Config"]    = ",".join(cfg_overall_ir)
            df.at[idx, "CHK Overall IR Calc"]      = ",".join([str(x) for x in result["overall_slabs"]])
            df.at[idx, "CHK Secure IR Config"]     = ",".join(cfg_secure_ir)
            df.at[idx, "CHK Secure IR Calc"]       = ",".join([str(x) for x in result["secure_slabs"]])
            df.at[idx, "CHK Unsecure IR Config"]   = ",".join(cfg_unsec_ir)
            df.at[idx, "CHK Unsecure IR Calc"]     = ",".join([str(x) for x in result["unsecure_slabs"]])
            df.at[idx, "CHK Tenure Config"]        = str(cfg_tenure)
            df.at[idx, "CHK Tenure Calc"]          = str(final_ten_chk)
            df.at[idx, "CHK Slab3 ToDay Config"]   = str(cfg_slab3today) if cfg_slab3today is not None else ""
            df.at[idx, "CHK Slab3 ToDay Calc"]     = str(tenure_days_chk)
            df.at[idx, "CHK Secure LTV Config"]    = str(cfg_secure_ltv)
            df.at[idx, "CHK Secure LTV Calc"]      = str(result["secure_ltv"])
            df.at[idx, "CHK BS1 LegalName Config"] = str(cfg_bs1_ln)
            
            if advanced_mode_chk == "GPA Mode" and custom_ln_chk:
                df.at[idx, "CHK BS1 LegalName Calc"] = "Rupeek Delight" if "delight" in custom_ln_chk.lower() else "Rupeek Royal"
            elif is_90d_chk: df.at[idx, "CHK BS1 LegalName Calc"] = "Rupeek Ultra"
            else: df.at[idx, "CHK BS1 LegalName Calc"] = f"Rupeek {scheme_chk}"
                
            df.at[idx, "CHK PF Config Min"]        = str(cfg_pf_min_v) if cfg_pf_min_v is not None else ""
            df.at[idx, "CHK PF Calc Min"]          = str(min_unsecure_pf_chk) if min_unsecure_pf_chk is not None else ""
            df.at[idx, "CHK PF Config Max"]        = str(cfg_pf_max_v) if cfg_pf_max_v is not None else ""
            df.at[idx, "CHK PF Calc Max"]          = str(max_unsecure_pf_chk) if max_unsecure_pf_chk is not None else ""
            df.at[idx, "CHK FC Config"]            = str(cfg_fc_v) if cfg_fc_v is not None else "None"
            df.at[idx, "CHK FC Calc"]              = str(fc_unsecure_chk) if fc_unsecure_chk is not None else "None"

            if "fulfillmentChannels" in df.columns:
                fc_val = str(df.at[idx, "fulfillmentChannels"]).lower()
                df.at[idx, "FED CLM Flag"] = "⚠️ FED CLM — keep as separate CLM flow" if "fedclm" in fc_val else ""
            else: df.at[idx, "FED CLM Flag"] = ""

            cfg_bs2_ln_chk = str(df.at[idx, "bs2-legalName"]) if "bs2-legalName" in df.columns else ""
            df.at[idx, "CHK BS2 LegalName Config"] = cfg_bs2_ln_chk
            df.at[idx, "CHK BS2 LegalName Calc"] = cfg_bs2_ln_chk

            lm_appname_chk = str(df.at[idx, "lm_appname"]) if "lm_appname" in df.columns else ""
            ap_str = str(df.at[idx, "applicableProcesses"]).lower() if "applicableProcesses" in df.columns else ""
            flow_chk = "RTN" if "retention" in ap_str else "RWL" if "renewal" in ap_str else "FWD"
            
            cfg_ap_col = str(df.at[idx, "applicableProcesses"]) if "applicableProcesses" in df.columns else ""
            df.at[idx, "CHK ApplicableProcesses Config"] = cfg_ap_col
            df.at[idx, "CHK ApplicableProcesses Calc"] = get_applicable_processes_string(flow_chk, refname, lm_appname_chk)
            
            flags_str = str(df.at[idx, "schemeFlags"]) if "schemeFlags" in df.columns else "{}"
            try: flags_dict = json.loads(flags_str)
            except: flags_dict = {}
            cfg_balance = "True" if flags_dict.get("renewalAtOutstanding") in [True, "true", "True"] else "False"
            
            name_for_bal = refname.lower() + " " + str(df.at[idx, "lm_appname"]).lower() if "lm_appname" in df.columns else refname.lower()
            calc_balance = "True" if "balance" in name_for_bal else "False"
            
            df.at[idx, "CHK Balance Config"] = cfg_balance
            df.at[idx, "CHK Balance Calc"] = calc_balance

        return df

    def _normalize_decimal_list(val):
        if str(val) == "None": return "None"
        parts = [p.strip() for p in str(val).split(',') if p.strip()]
        out = []
        for p in parts:
            try: out.append(str(Decimal(p).quantize(Decimal("0.01"), ROUND_HALF_UP)))
            except Exception: out.append(p)
        return ','.join(out)

    def _build_status_df(checked_df, exclude_fc=False):
        rows = []
        for idx in checked_df.index:
            refname = str(checked_df.at[idx, "refName"]) if "refName" in checked_df.columns else str(idx)
            masterschemeid = str(checked_df.at[idx, "masterschemeid"]) if "masterschemeid" in checked_df.columns else ""
            row_result = {"masterschemeid": masterschemeid, "refName": refname}
            all_pass = True
            for label, cfg_col, calc_col in CHECK_FIELDS:
                cfg_val  = str(checked_df.at[idx, cfg_col]).strip()  if cfg_col  in checked_df.columns else ""
                calc_val = str(checked_df.at[idx, calc_col]).strip() if calc_col in checked_df.columns else ""
                
                if cfg_val == "" and calc_val == "": 
                    status = "—"
                elif label in ("ApplicableProcesses",): 
                    norm_cfg = _normalize_process_list(cfg_val)
                    norm_calc = _normalize_process_list(calc_val)
                    if norm_cfg == norm_calc: 
                        status = "✅ OK"
                    elif norm_calc == "renewal-retention" and norm_cfg == "fresh-loan,renewal-retention":
                        status = "✅ OK"
                    else: 
                        status = f"❌ Config: {cfg_val}  |  Expected: {calc_val}"
                        all_pass = False
                elif label == "BS1 LegalName":
                    if cfg_val.lower() == calc_val.lower(): 
                        status = "✅ OK"
                    else:
                        status = f"❌ Config: {cfg_val}  |  Expected: {calc_val}"
                        all_pass = False
                elif label == "BS2 LegalName":
                    status = f"ℹ️ {cfg_val}" if cfg_val else "—"
                elif label == "FC" and exclude_fc:
                    status = f"⚠️ Skipped (Config: {cfg_val} | Expected: {calc_val})"
                elif _normalize_decimal_list(cfg_val) == _normalize_decimal_list(calc_val): 
                    status = "✅ OK"
                else:
                    status = f"❌ Config: {cfg_val}  |  Expected: {calc_val}"
                    all_pass = False
                row_result[label] = status
            row_result["Overall"] = "✅ ALL OK" if all_pass else "❌ ERRORS"
            rows.append(row_result)
        return pd.DataFrame(rows)

    if phase_2_mode == "Use Phase 1 Output":
        if "df" in st.session_state and not st.session_state.df.empty:
            raw_checker_df = st.session_state.df.copy()
            st.write(f"Loaded **{len(raw_checker_df)}** scheme rows directly from Phase 1.")
            
            product_type_chk = st.selectbox(
                "Fallback Product Type",
                ["30D Jumping", "90D Jumping", "HIP 30D Jumping", "HIP 90D Jumping"],
                key="product_type_chk"
            )

            if st.button("Run Check", key="run_check_p1_btn"):
                if "rectified_df" in st.session_state: del st.session_state["rectified_df"]
                checked_df = _run_checker_on_df(raw_checker_df, product_type_chk, advanced_mode_chk)
                st.session_state["checked_df"] = checked_df
                status_df = _build_status_df(checked_df, exclude_fc=exclude_fc_check)
                log_validation_results_to_db(status_df, raw_api_df=raw_checker_df)
        else:
            st.warning("No generated schemes found. Please complete Phase 1 first or choose 'Upload CSV'.")

    elif phase_2_mode == "Upload CSV":
        uploaded_checker = st.file_uploader("Upload Scheme CSV", type=["csv"], key="checker_upload")
        if uploaded_checker is not None:
            raw_checker_df = pd.read_csv(uploaded_checker)
            st.write(f"Loaded **{len(raw_checker_df)}** scheme rows.")
            
            product_type_chk = st.selectbox(
                "Fallback Product Type (Used if not in CSV)",
                ["30D Jumping", "90D Jumping", "HIP 30D Jumping", "HIP 90D Jumping"],
                key="product_type_chk"
            )

            if st.button("Run Check", key="run_check_btn"):
                if "rectified_df" in st.session_state: del st.session_state["rectified_df"]
                checked_df = _run_checker_on_df(raw_checker_df, product_type_chk, advanced_mode_chk)
                st.session_state["checked_df"] = checked_df
                
                status_df = _build_status_df(checked_df, exclude_fc=exclude_fc_check)
                log_validation_results_to_db(status_df, raw_api_df=raw_checker_df)

    elif phase_2_mode == "Live DB / API Fetch":
        if not DB_DRIVER_AVAILABLE:
            st.error("The `psycopg2` library is required to connect to the DB. Please run `pip install psycopg2-binary`")
        else:
            with st.form("api_sync_form"):
                st.markdown("#### Application Connection")
                jwt_token = st.text_input("JWT Token", type="password", help="Paste your 24hr JWT auth token here")
                
                force_run_all = st.checkbox("Force run all active schemes (Ignore validation history)", value=False)
                submitted = st.form_submit_button("Fetch and Validate")

            if submitted:
                if not jwt_token:
                    st.error("Please provide the JWT Token.")
                else:
                    try:
                        st.info("Connecting to Database...")
                        conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
                        cursor = conn.cursor()
                        query = """
                            SELECT masterschemeid
FROM dm.rpt_scheme_details
WHERE enabled = 'true'
  AND (
        lower(lm_appname) LIKE '%fbl%'
        OR lower(lm_appname) LIKE '%fed%'
      )
  AND lower(lm_appname) NOT LIKE '%monthly%'
  AND lower(lm_appname) NOT LIKE '%override%'
  AND lower(lm_appname) NOT LIKE '%dpd%'
  AND lower(lm_appname) NOT LIKE '%rnrp%'
  AND lower(lm_appname) NOT LIKE '%part%'
  AND lower(lm_appname) NOT LIKE '%ito%'
                        """
                        cursor.execute(query)
                        rows = cursor.fetchall()
                        conn.close()
                        
                        master_ids = [row[0] for row in rows if row[0]]
                        st.success(f"Fetched {len(master_ids)} Master Scheme IDs from Database.")

                        if not force_run_all:
                            checked_ids = fetch_previously_validated_schemes()
                            if checked_ids:
                                master_ids = [mid for mid in master_ids if str(mid) not in checked_ids]
                            st.info(f"Filtered out previously checked schemes. {len(master_ids)} remaining to process.")

                        if master_ids:
                            headers = {
                                "Authorization": f"JWT {jwt_token}",
                                "Content-Type": "application/json"
                            }
                            
                            fetched_data = []
                            progress_bar = st.progress(0)
                            status_text = st.empty()

                            for idx, m_id in enumerate(master_ids):
                                status_text.text(f"Fetching data for {m_id} ({idx + 1}/{len(master_ids)})...")
                                try:
                                    response = requests.get(f"https://schemeengapi.rupeek.com/api/v1/masterschemes/{m_id}", headers=headers)
                                    if response.status_code == 200:
                                        payload = response.json()
                                        row_data = parse_api_scheme_to_row(m_id, payload)
                                        fetched_data.append(row_data)
                                    else:
                                        fetched_data.append({"masterschemeid": m_id, "Error": f"HTTP {response.status_code}"})
                                except Exception as e:
                                    fetched_data.append({"masterschemeid": m_id, "Error": str(e)})
                                
                                progress_bar.progress((idx + 1) / len(master_ids))
                            
                            status_text.text("Fetch complete. Building dataframe...")
                            
                            raw_api_df = pd.DataFrame(fetched_data)
                            st.write("### Fetched Raw Data Preview")
                            st.dataframe(raw_api_df.head(10))

                            if "rectified_df" in st.session_state: del st.session_state["rectified_df"]
                            checked_df = _run_checker_on_df(raw_api_df, product_type_fallback="30D Jumping", advanced_mode_chk=advanced_mode_chk)
                            st.session_state["checked_df"] = checked_df
                            
                            status_df = _build_status_df(checked_df, exclude_fc=exclude_fc_check)
                            log_validation_results_to_db(status_df, raw_api_df=raw_api_df)
                        else:
                            st.success("No new schemes to process!")

                    except Exception as e:
                        st.error(f"Error executing Live Sync: {e}")

    if "checked_df" in st.session_state:
        status_df = _build_status_df(st.session_state["checked_df"], exclude_fc=exclude_fc_check)
        
        display_status_df = status_df
        if show_errors_only:
            display_status_df = status_df[status_df["Overall"] != "✅ ALL OK"]

        total   = len(status_df)
        errors  = int((status_df["Overall"] != "✅ ALL OK").sum())
        st.markdown(f"### Results: {total - errors} / {total} schemes OK &nbsp;&nbsp; {'🟢' if errors == 0 else '🔴'} {errors} with errors")

        def _highlight_overall(val):
            if "ALL OK" in str(val): return "background-color: #d4edda; color: #155724; font-weight: bold"
            elif "ERRORS" in str(val): return "background-color: #f8d7da; color: #721c24; font-weight: bold"
            return ""

        def _highlight_cell(val):
            v = str(val)
            if v.startswith("✅"): return "background-color: #d4edda"
            elif v.startswith("❌"): return "background-color: #f8d7da"
            elif v.startswith("⚠️"): return "background-color: #fff3cd; color: #856404"
            return ""

        styled = display_status_df.style.applymap(_highlight_cell).applymap(_highlight_overall, subset=["Overall"])
        st.dataframe(styled, use_container_width=True)

        st.download_button("Download Check Report CSV", status_df.to_csv(index=False), "check_report.csv", key="dl_check_report")

        if errors > 0:
            st.markdown("---")
            st.write(f"**{errors} scheme(s) have errors.** Click below to auto-rectify them.")
            if st.button("Rectify Errors", key="rectify_btn"):
                rectified_df = st.session_state["checked_df"].copy()
                for idx in rectified_df.index:
                    row = rectified_df.loc[idx]
                    refname = str(rectified_df.at[idx, "refName"]).strip() if "refName" in rectified_df.columns else ""

                    overall_ltv    = extract_ltv_from_code(refname)
                    requested_tenure = extract_tenure(refname)
                    monthly_opp    = extract_opp(refname)
                    pf_min, pf_max = extract_pf_range(refname)
                    if pf_min is None:
                        ep = extract_pf(refname)
                        pf_min, pf_max = ep, ep

                    if "customerLtv" in rectified_df.columns:
                        ltv_col = _parse_decimal(rectified_df.at[idx, "customerLtv"])
                        if ltv_col is not None: overall_ltv = ltv_col
                    if "tenure" in rectified_df.columns:
                        ten_col = _parse_int(rectified_df.at[idx, "tenure"])
                        if ten_col is not None: requested_tenure = ten_col

                    if not all([overall_ltv, requested_tenure, monthly_opp]): continue

                    row_prod_type = str(rectified_df.at[idx, "Product Type"]).strip() if "Product Type" in rectified_df.columns else ""
                    if not row_prod_type or str(row_prod_type).lower() == "nan": 
                        row_prod_type = "30D Jumping"

                    is_hip_r  = "hip" in row_prod_type.lower()
                    is_90d_r  = "90d" in row_prod_type.lower()

                    refname_nopf = not bool(re.search(r"PF\s*[-:]?\s*[0-9]", refname, re.IGNORECASE))
                    is_nopf_r    = refname_nopf
                    overall_pf   = None if is_nopf_r else (pf_max if pf_max is not None else pf_min)

                    if is_90d_r:
                        scheme_r = "Royal"
                        final_ten_r = requested_tenure
                    else:
                        scheme_r, final_ten_r = decision_engine(overall_ltv, monthly_opp, requested_tenure)

                    secure_ltv_ov_r = None
                    custom_ln_r = ""
                    
                    if "Custom Sec LTV" in rectified_df.columns:
                        custom_sec_ltv_r = _parse_decimal(rectified_df.at[idx, "Custom Sec LTV"])
                        if custom_sec_ltv_r is not None: secure_ltv_ov_r = custom_sec_ltv_r
                    elif "bs1-ltv" in rectified_df.columns:
                        secure_ltv_ov_r = _parse_decimal(rectified_df.at[idx, "bs1-ltv"])

                    if "Custom Legal Name" in rectified_df.columns:
                        cln_raw_r = rectified_df.at[idx, "Custom Legal Name"]
                        custom_ln_r = str(cln_raw_r).strip() if pd.notna(cln_raw_r) else ""
                    elif "bs1-legalName" in rectified_df.columns:
                        cln_raw_r = rectified_df.at[idx, "bs1-legalName"]
                        custom_ln_r = str(cln_raw_r).strip() if pd.notna(cln_raw_r) else ""
                        
                    if custom_ln_r:
                        if "delight" in custom_ln_r.lower(): scheme_r = "Delight"
                        elif "royal" in custom_ln_r.lower(): scheme_r = "Royal"

                    result_r = interest_engine(scheme_r, final_ten_r, overall_ltv, monthly_opp, secure_ltv_override=secure_ltv_ov_r)

                    tenure_days_r = get_tenure_days(final_ten_r)
                    secure_ltv_r  = result_r["secure_ltv"]
                    denom_r = Decimal("1") - (secure_ltv_r / overall_ltv)
                    if denom_r == 0: continue

                    slab_days_r = [(0, 90), (91, 120), (121, int(final_ten_r) * 30)] if is_90d_r else None

                    if "bs1-ltv" in rectified_df.columns: rectified_df.at[idx, "bs1-ltv"] = float(result_r["secure_ltv"])
                    if "bs1-tenure" in rectified_df.columns: rectified_df.at[idx, "bs1-tenure"] = final_ten_r
                    if "bs2-tenure" in rectified_df.columns:
                        if is_hip_r: rectified_df.at[idx, "bs2-tenure"] = 48 if final_ten_r in (12,6,7) else 24
                        else: rectified_df.at[idx, "bs2-tenure"] = final_ten_r
                    if "tenure" in rectified_df.columns: rectified_df.at[idx, "tenure"] = final_ten_r
                        
                    if "bs1-legalName" in rectified_df.columns:
                        if custom_ln_r:
                            rectified_df.at[idx, "bs1-legalName"] = "Rupeek Delight" if "delight" in custom_ln_r.lower() else "Rupeek Royal"
                        elif is_90d_r: rectified_df.at[idx, "bs1-legalName"] = "Rupeek Ultra"
                        else: rectified_df.at[idx, "bs1-legalName"] = f"Rupeek {scheme_r}"

                    for col, slabs in [("OverallInterestCalculation", result_r["overall_slabs"]), ("bs1-addon-1", result_r["secure_slabs"])]:
                        if col in rectified_df.columns:
                            rectified_df.at[idx, col] = update_interest_json(rectified_df.at[idx, col], slabs, tenure_days_r, slab_days=slab_days_r)

                    bs2_addon_days_r = (48 if final_ten_r in (12, 6, 7) else 24) * 30 if is_hip_r else tenure_days_r
                    bs2_slab_days_r = [(0, 90), (91, 120), (121, bs2_addon_days_r)] if is_90d_r else None

                    if "bs2-addon-1" in rectified_df.columns:
                        rectified_df.at[idx, "bs2-addon-1"] = update_interest_json(rectified_df.at[idx, "bs2-addon-1"], result_r["unsecure_slabs"], bs2_addon_days_r, slab_days=bs2_slab_days_r)
                    if "bs2-calculation" in rectified_df.columns:
                        rectified_df.at[idx, "bs2-calculation"] = update_interest_json(rectified_df.at[idx, "bs2-calculation"], result_r["unsecure_slabs"], bs2_addon_days_r, slab_days=bs2_slab_days_r)

                    is_flexi_r  = any(t in str(rectified_df.at[idx, "refName"]).lower() for t in ["flexipf", "flexi pf", "flexi-pf"])
                    
                    ap_str_r = str(rectified_df.at[idx, "applicableProcesses"]).lower() if "applicableProcesses" in rectified_df.columns else ""
                    flow_text_r = "RTN" if "retention" in ap_str_r else "RWL" if "renewal" in ap_str_r else "FWD"
                    lm_appname_r = str(rectified_df.at[idx, "lm_appname"]) if "lm_appname" in rectified_df.columns else ""
                    
                    app_proc_list_r = get_applicable_processes_list(flow_text_r, refname, lm_appname_r)

                    if "applicableProcesses" in rectified_df.columns:
                        rectified_df.at[idx, "applicableProcesses"] = ",".join(app_proc_list_r)

                    min_pf_i_r = pf_min if pf_min is not None else overall_pf
                    max_pf_i_r = pf_max if pf_max is not None else overall_pf
                    min_unsec_pf_r = (min_pf_i_r / denom_r).quantize(Decimal("0.00"), ROUND_HALF_UP) if min_pf_i_r is not None else None
                    max_unsec_pf_r = (max_pf_i_r / denom_r).quantize(Decimal("0.00"), ROUND_HALF_UP) if max_pf_i_r is not None else None
                    charge_pf_r = (max_unsec_pf_r if is_flexi_r else min_unsec_pf_r) if not is_nopf_r else None

                    if not is_nopf_r and charge_pf_r is not None and min_unsec_pf_r is not None and max_unsec_pf_r is not None:
                        if "chargeText" in rectified_df.columns:
                            overall_pf_for_ct = max_pf_i_r if is_flexi_r else min_pf_i_r
                            rectified_df.at[idx, "chargeText"] = update_charge_text(rectified_df.at[idx, "chargeText"], charge_pf_r, overall_pf_for_ct)
                        if "bs2-charge-2" in rectified_df.columns:
                            rectified_df.at[idx, "bs2-charge-2"] = update_bs2_charge_2(rectified_df.at[idx, "bs2-charge-2"], charge_pf_r, min_unsec_pf_r, max_unsec_pf_r, is_flexi_r, applicable_processes=app_proc_list_r)

                    include_fc_r_val = rectified_df.at[idx, "Include FC"] if "Include FC" in rectified_df.columns else True
                    if pd.isna(include_fc_r_val) or str(include_fc_r_val).strip() == "": include_fc_r = True
                    elif str(include_fc_r_val).strip().lower() in ["false", "0", "no"]: include_fc_r = False
                    else: include_fc_r = bool(include_fc_r_val)
                    
                    fc_val_r_val = rectified_df.at[idx, "FC Value"] if "FC Value" in rectified_df.columns else "1.00"
                    if pd.notna(fc_val_r_val) and str(fc_val_r_val).strip(): fc_overall_r = _parse_decimal(fc_val_r_val) or Decimal("1.00")
                    else: fc_overall_r = Decimal("1.00")
                    
                    fc_dur_r = 3 if final_ten_r in (6, 7, 12) else 2

                    if include_fc_r:
                        fc_unsec_r = (fc_overall_r / denom_r).quantize(Decimal("0.00"), ROUND_HALF_UP)
                        if is_nopf_r:
                            if "bs2-charge-2" in rectified_df.columns: rectified_df.at[idx, "bs2-charge-2"] = update_foreclosure_charge(rectified_df.at[idx, "bs2-charge-2"], fc_unsec_r, fc_dur_r, app_proc_list_r)
                            if "bs2-charge-3" in rectified_df.columns: rectified_df.at[idx, "bs2-charge-3"] = "{}"
                            if "bs2-NoOfCharges" in rectified_df.columns: rectified_df.at[idx, "bs2-NoOfCharges"] = 2
                        else:
                            if "bs2-charge-3" in rectified_df.columns: rectified_df.at[idx, "bs2-charge-3"] = update_foreclosure_charge(rectified_df.at[idx, "bs2-charge-3"], fc_unsec_r, fc_dur_r, app_proc_list_r)
                            if "bs2-NoOfCharges" in rectified_df.columns: rectified_df.at[idx, "bs2-NoOfCharges"] = 3
                    else:
                        fc_unsec_r = None
                        if is_nopf_r:
                            if "bs2-charge-2" in rectified_df.columns: rectified_df.at[idx, "bs2-charge-2"] = "{}"
                            if "bs2-charge-3" in rectified_df.columns: rectified_df.at[idx, "bs2-charge-3"] = "{}"
                            if "bs2-NoOfCharges" in rectified_df.columns: rectified_df.at[idx, "bs2-NoOfCharges"] = 1
                        else:
                            if "bs2-charge-3" in rectified_df.columns: rectified_df.at[idx, "bs2-charge-3"] = "{}"
                            if "bs2-NoOfCharges" in rectified_df.columns: rectified_df.at[idx, "bs2-NoOfCharges"] = 2
                                
                    if "bs2-legalName" in rectified_df.columns:
                        if is_hip_r:
                            hip_unsecure_tenure_r = 48 if final_ten_r in (12,6,7) else 24
                            hip_encoding_r = "th7.si5" if final_ten_r == 12 else "f8"
                            hip_ln_r = str(rectified_df.at[idx, "bs2-legalName"])
                            if re.search(r'th7\.si5|f8', hip_ln_r, re.IGNORECASE): hip_ln_r = re.sub(r'th7\.si5|f8', hip_encoding_r, hip_ln_r, count=1, flags=re.IGNORECASE)
                            else: hip_ln_r = re.sub(r'48(?:\.00)?%|37\.65%', hip_encoding_r, hip_ln_r, count=1)
                            hip_ln_r = re.sub(r'\b(?:24|48)M\b', f'{hip_unsecure_tenure_r}M', hip_ln_r, count=1, flags=re.IGNORECASE)
                            
                            if include_fc_r:
                                fc_str_hip_r = str(((fc_unsec_r * 2).quantize(Decimal("1"), ROUND_HALF_UP) / 2).quantize(Decimal("0.00")))
                                hip_ln_r = re.sub(r'(PF\s+HIP\s+)[0-9]+(?:\.[0-9]+)?(%)', rf'\g<1>{fc_str_hip_r}\g<2>', hip_ln_r, count=1, flags=re.IGNORECASE)
                                hip_ln_r = re.sub(r'\bFC(?:\s*[-:]?\s*\d+D)?\b', 'FC', hip_ln_r, flags=re.IGNORECASE)
                                if int(fc_dur_r) == 3: hip_ln_r = re.sub(r'\bFC\b', 'FC 90D', hip_ln_r, count=1, flags=re.IGNORECASE)
                                elif int(fc_dur_r) == 4: hip_ln_r = re.sub(r'\bFC\b', 'FC 120D', hip_ln_r, count=1, flags=re.IGNORECASE)
                            else:
                                hip_ln_r = re.sub(r'(PF\s+HIP\s+)[0-9]+(?:\.[0-9]+)?(%)', '', hip_ln_r, flags=re.IGNORECASE)
                                hip_ln_r = re.sub(r'\bFC(?:\s*[-:]?\s*\d+D)?\b', '', hip_ln_r, flags=re.IGNORECASE)
                                
                            rectified_df.at[idx, "bs2-legalName"] = re.sub(r'\s+', ' ', hip_ln_r).strip()
                        else:
                            encoding_r = "th7.si5" if final_ten_r == 12 else "f8"
                            updated_legal_name_r = update_bs2_legal_name(rectified_df.at[idx, "bs2-legalName"], final_ten_r, encoding_r)
                            has_pf_in_name_r = not is_nopf_r
                            rectified_df.at[idx, "bs2-legalName"] = update_bs2_legal_name_pf_fc(updated_legal_name_r, fc_unsec_r, fc_dur_r, has_pf_in_name_r, include_fc=include_fc_r)
                            
                    # Rectify Balance Flag
                    flags_r = {"isLoanCalcSplitv2Enabled": True}
                    if is_hip_r: flags_r["isHIP"] = True
                    name_for_bal_r = refname.lower() + " " + str(rectified_df.at[idx, "lm_appname"]).lower() if "lm_appname" in rectified_df.columns else refname.lower()
                    if "balance" in name_for_bal_r: flags_r["renewalAtOutstanding"] = True
                    rectified_df.at[idx, "schemeFlags"] = json.dumps(flags_r)

                out_cols = [c for c in rectified_df.columns if not c.startswith("CHK") and c not in ["Error", "lm_appname"]]
                rectified_out = rectified_df[out_cols]
                st.session_state["rectified_df"] = rectified_out
                st.success("Rectification complete!")

        if "rectified_df" in st.session_state:
            st.subheader("Rectified Schemes")
            st.dataframe(st.session_state["rectified_df"], use_container_width=True)
            st.download_button(
                "Download Rectified CSV",
                st.session_state["rectified_df"].to_csv(index=False),
                "rectified_schemes.csv",
                key="dl_rectified"
            )

# ============================================================
# PHASE 3: PUSH TO PRODUCTION
# ============================================================

with tab3:
    st.subheader("Phase 3 — Push to Production (Create/Disable Schemes)")
    st.info("📊 **Database Logs:** Execution results are saved to `temp.scheme_creation_logs` and `temp.scheme_disable_logs`.")
    st.write("Review your finalized schemes and execute them directly in the system via API.")
    
    with st.expander("⚙️ Phase 3 Settings", expanded=False):
        api_delay = st.number_input("API Delay per Row (seconds)", min_value=0.0, max_value=10.0, value=0.0, step=0.5, help="Add a pause between each API call to avoid rate limits.")

    phase_3_mode = st.radio("Select Data Source", ["Use Phase 2 Output", "Upload Final Scheme CSV"], horizontal=True)
    
    phase_3_action = st.radio("Action to Perform", ["Create Schemes", "Disable/Delete Schemes"], horizontal=True)
    
    jwt_token_create = st.text_input("JWT Authorization Token", type="password", help="Paste your active JWT token here")

    create_df = pd.DataFrame()
    if phase_3_mode == "Use Phase 2 Output":
        if "rectified_df" in st.session_state and not st.session_state.rectified_df.empty:
            create_df = st.session_state.rectified_df.copy()
            st.write(f"Loaded **{len(create_df)}** schemes for execution directly from Phase 2 (Rectified).")
        elif "checked_df" in st.session_state and not st.session_state.checked_df.empty:
            raw_chk = st.session_state.checked_df.copy()
            out_cols = [c for c in raw_chk.columns if not c.startswith("CHK") and c not in ["Error", "lm_appname"]]
            create_df = raw_chk[out_cols]
            st.write(f"Loaded **{len(create_df)}** schemes for execution directly from Phase 2 (Validated).")
        else:
            st.warning("No validated schemes found. Please run the Checker in Phase 2 first, or upload a CSV.")
    else:
        uploaded_create = st.file_uploader("Upload Final Scheme CSV", type=["csv"], key="create_upload")
        if uploaded_create is not None:
            create_df = pd.read_csv(uploaded_create)
            st.write(f"Loaded **{len(create_df)}** schemes for execution from CSV.")
            
    if not create_df.empty:
        btn_label = "Disable/Enable Schemes via API" if phase_3_action == "Disable/Delete Schemes" else "Create Schemes via API"
        if st.button(btn_label, key="push_api_btn"):
            if not jwt_token_create:
                st.error("JWT Token is required.")
            else:
                progress_bar_create = st.progress(0)
                status_text_create = st.empty()
                
                results = []
                headers = {
                    "Authorization": f"JWT {jwt_token_create}",
                    "Content-Type": "application/json"
                }
                
                if phase_3_action == "Disable/Delete Schemes":
                    url = "https://api.rupeek.com/coresvc/api/superuser/v2/schemeupdation"
                else:
                    url = "https://api.rupeek.com/coresvc/api/superuser/v2/schemecreation"
                
                for idx, row in create_df.iterrows():
                    refname = str(row.get("refName", f"Row {idx}"))
                    status_text_create.text(f"Processing scheme: {refname} ({idx + 1}/{len(create_df)})...")
                    
                    # Auto Route Logic for Create
                    if phase_3_action == "Disable/Delete Schemes":
                        api_template = "Disable/Delete Scheme"
                    else:
                        fc_val = str(row.get("fulfillmentChannels", "")).lower()
                        pt_val = str(row.get("Product Type", "")).lower()
                        flags_val = str(row.get("schemeFlags", "")).lower()
                        name_val = str(row.get("refName", "")).lower()
                        
                        if "clm" in fc_val or "clm" in pt_val or "clm" in name_val:
                            api_template = "CLM Models"
                        elif "hip" in pt_val or "hip" in name_val or "hip" in flags_val:
                            api_template = "HIP (Default)"
                        else:
                            api_template = "Non-HIP (Fed)"
                    
                    payload = build_creation_payload(row, api_template)
                    
                    if api_template == "Disable/Delete Scheme":
                        m_id = payload.get("masterSchemeId", "")
                        dis_val = payload.get("disable", "true")
                        action_comment = "Disabled" if str(dis_val).lower() == "true" else "Enabled"

                        if not m_id:
                            results.append({
                                "Scheme Name (refName)": refname, 
                                "MasterSchemeId": m_id,
                                "Disable Flag": dis_val,
                                "Action": "Error",
                                "API Template Used": api_template,
                                "Status": "❌ Error", 
                                "API Response": "Missing masterSchemeId in row"
                            })
                            continue
                            
                    try:
                        response = requests.post(url, headers=headers, json=payload)
                        resp_text = response.text
                        if response.status_code in [200, 201]:
                            status_icon = "✅ Success"
                        else:
                            status_icon = "❌ Failed"
                            
                        res_dict = {
                            "Scheme Name (refName)": refname, 
                            "Status": status_icon, 
                            "API Template Used": api_template,
                            "API Response": resp_text
                        }
                        if api_template == "Disable/Delete Scheme":
                            res_dict["MasterSchemeId"] = m_id
                            res_dict["Disable Flag"] = dis_val
                            res_dict["Action"] = action_comment
                            
                        results.append(res_dict)
                        
                    except Exception as e:
                        res_dict = {
                            "Scheme Name (refName)": refname, 
                            "Status": "❌ Error", 
                            "API Template Used": api_template,
                            "API Response": str(e)
                        }
                        if api_template == "Disable/Delete Scheme":
                            res_dict["MasterSchemeId"] = m_id
                            res_dict["Disable Flag"] = dis_val
                            res_dict["Action"] = "Error"
                        results.append(res_dict)
                        
                    progress_bar_create.progress((idx + 1) / len(create_df))
                    
                    # Apply API Delay
                    if api_delay > 0:
                        time.sleep(api_delay)
                    
                status_text_create.text("API execution process complete!")
                
                if phase_3_action == "Disable/Delete Schemes":
                    log_disable_results_to_db(results)
                else:
                    log_creation_results_to_db(results)

                res_df = pd.DataFrame(results)
                
                def _highlight_status(val):
                    if "Success" in str(val): return "background-color: #d4edda; color: #155724; font-weight: bold"
                    elif "Failed" in str(val) or "Error" in str(val): return "background-color: #f8d7da; color: #721c24; font-weight: bold"
                    return ""
                
                st.dataframe(res_df.style.applymap(_highlight_status, subset=["Status"]), use_container_width=True)

                st.download_button("Download API Execution Logs", res_df.to_csv(index=False), "api_execution_logs.csv", key="dl_api_logs")


# ============================================================
# PHASE 4: INJECT SCHEMES
# ============================================================

with tab4:
    st.subheader("Phase 4 — Inject Schemes")
    st.write("Inject specific schemes directly into a customer's profile.")
    
    with st.expander("⚙️ Phase 4 Settings", expanded=True):
        st.markdown("<small>Update this password when it expires (every 10 days).</small>", unsafe_allow_html=True)
        # Pull the default value from secrets so it isn't hardcoded in the script!
        try:
            default_inject_pass = st.secrets.get("INJECT_PASS", "VEDiPv")
        except:
            default_inject_pass = "VEDiPv"
            
        inject_pass = st.text_input("Injection API Password", value=default_inject_pass, type="password")

    col1, col2 = st.columns(2)
    with col1:
        customer_id_input = st.text_input("Customer ID", help="E.g. a5fcbf37-b115-4b56-9")
    with col2:
        scheme_ids_input = st.text_input("Scheme IDs", help="Comma-separated IDs. E.g. Y9MX309E")

    if st.button("Inject Schemes", type="primary"):
        if not customer_id_input or not scheme_ids_input:
            st.error("Both Customer ID and Scheme IDs are required.")
        else:
            url = "https://casapi.rupeek.com/api/v1/inject-schemes"
            params = {
                "customerId": customer_id_input.strip(),
                "schemeIds": scheme_ids_input.strip()
            }
            auth = ("category", inject_pass)

            with st.spinner("Injecting schemes..."):
                try:
                    response = requests.get(url, params=params, auth=auth)
                    if response.status_code in [200, 201]:
                        st.success("✅ Schemes successfully injected!")
                        st.json(response.json() if response.text else {"status": "Success, empty response"})
                    else:
                        st.error(f"❌ Failed to inject. HTTP {response.status_code}")
                        st.write(response.text)
                except Exception as e:
                    st.error(f"Error during injection: {e}")
