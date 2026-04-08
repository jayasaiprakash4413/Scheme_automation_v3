import streamlit as st
import pandas as pd
import json
import re
from copy import deepcopy
from pathlib import Path
from decimal import Decimal, getcontext, ROUND_HALF_UP

getcontext().prec = 50

st.set_page_config(layout="wide")
st.title("Final Scheme Configuration Engine")

tab1, tab2 = st.tabs(["Phase 1 — Generate", "Phase 2 — Check & Rectify"])

# ============================================================
# CONSTANTS (DO NOT TOUCH)
# ============================================================

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

SUMMARY_INPUT_COLUMNS = ["customerLtv", "TS", "slab1 ROI", "PF Tag", "Flow", "PF val", "Tenure"]

CHECKER_COLUMNS = [
    "CHK PF Config Min", "CHK PF Config Max", "CHK PF Calc Min", "CHK PF Calc Max",
    "CHK FC Config", "CHK FC Calc",
    "CHK Overall IR Config", "CHK Overall IR Calc",
    "CHK Secure IR Config", "CHK Secure IR Calc",
    "CHK Unsecure IR Config", "CHK Unsecure IR Calc",
    "CHK Tenure Config", "CHK Tenure Calc",
    "CHK Slab3 ToDay Config", "CHK Slab3 ToDay Calc",
    "CHK Secure LTV Config", "CHK Secure LTV Calc",
    "CHK BS1 LegalName Config", "CHK BS1 LegalName Calc"
]


def _normalize_construct_keys(construct):
    normalized = dict(construct)
    if "Description" in normalized and "description" not in normalized:
        normalized["description"] = normalized.pop("Description")
    return normalized


def _load_dummy_construct(filename):
    path = Path(__file__).resolve().parent / filename
    with path.open("r", encoding="utf-8") as file:
        loaded = json.load(file)
    return _normalize_construct_keys(loaded)


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


def _determine_ts_label(scheme_min, scheme_max):
    min_int = _parse_int(scheme_min)
    max_int = _parse_int(scheme_max)

    if min_int == 30000 and max_int == 299999:
        return "<3L"
    if min_int == 300000 and max_int == 599999:
        return "3-6L"
    if min_int == 600000 and max_int == 1199999:
        return "6-12L"
    if min_int == 1200000 and max_int == 10000000:
        return ">12L"
    if min_int == 30000 and max_int == 10000000:
        return "ALL TS"
    return None


def _ts_label_to_min_max(ts_label):
    """Given a TS tag string, return (scheme_min, scheme_max) integers."""
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
        updated = re.sub(
            r'\|\|\s*PF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%\s*(?:[-–]\s*[0-9]+(?:\.[0-9]+)?\s*%)?',
            '',
            updated,
            flags=re.IGNORECASE
        )
        updated = re.sub(
            r'PF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%\s*(?:[-–]\s*[0-9]+(?:\.[0-9]+)?\s*%)?',
            '',
            updated,
            count=1,
            flags=re.IGNORECASE
        )
        updated = re.sub(r'\s*flexi\s*pf\b|\s*flexi-pf\b|\s*flexipf\b', '', updated, flags=re.IGNORECASE)
        updated = re.sub(r'\|\|\s*\|\|', '||', updated)
        updated = re.sub(r'\s+', ' ', updated).strip()
        return updated

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
    elif flow == "FWD":
        updated = re.sub(r'\bRenewal\b', 'FL TO', updated, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', updated).strip()


def _update_refname_ts(refname, ts_tag):
    """Update the TS label in refname using the tag string directly."""
    if ts_tag is None:
        return str(refname)
    tag = str(ts_tag).strip().upper().replace(" ", "")
    label_map = {
        "<3L":   "<3L",
        "3-6L":  "3-6L",
        "6-12L": "6-12L",
        ">12L":  ">12L",
        "ALLTS": "ALL TS",
    }
    ts_label = label_map.get(tag)
    if ts_label is None:
        return str(refname)

    updated = re.sub(
        r'(<\s*3L|3\s*-\s*6L|6\s*-\s*12L|>\s*12L|ALL\s*TS|<\s*6L|>\s*6L)',
        ts_label,
        str(refname),
        count=1,
        flags=re.IGNORECASE
    )
    return re.sub(r'\s+', ' ', updated).strip()


def _pick_dummy_construct(refname, pf_tag=None, product_type=None):
    if product_type is not None and str(product_type).strip().lower() == "hip":
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

        # FIX: Merge input columns properly, handling case-insensitive key collisions
        # If template has "tenure" (lowercase) and input has "Tenure" (uppercase),
        # update the lowercase key instead of creating a duplicate
        for column, value in input_row.items():
            if pd.notna(value):
                col_lower = column.lower()
                # Check if lowercase version exists in template_row
                if col_lower in template_row and col_lower != column:
                    # Update the lowercase key with input value
                    template_row[col_lower] = value
                else:
                    # Add as-is (either no lowercase version exists, or it's already lowercase)
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
        try:
            min_dec = Decimal(str(min_val)) if min_val is not None else None
        except Exception:
            min_dec = None
        try:
            max_dec = Decimal(str(max_val)) if max_val is not None else None
        except Exception:
            max_dec = None
        if min_dec is not None or max_dec is not None:
            return min_dec, max_dec

    charge = data.get("chargeValue")
    try:
        charge_dec = Decimal(str(charge)) if charge is not None else None
    except Exception:
        charge_dec = None

    return charge_dec, charge_dec


def _drop_empty_rows(df):
    if df.empty:
        return df

    def has_value(row):
        for value in row:
            if pd.isna(value):
                continue
            if str(value).strip() != "":
                return True
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

force_flexi_mode = False

# ============================================================
# EXTRACTIONS
# ============================================================

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
    if match:
        return Decimal(match.group(1)), Decimal(match.group(2))
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

# ============================================================
# DECISION ENGINE (12M SAFE)
# ============================================================

def decision_engine(overall_ltv, monthly_opp, requested_tenure):

    secure_s1 = Decimal("9.95")

    if requested_tenure == 12:
        secure_ltv = Decimal("60")
        unsecure_s1 = Decimal("37.65")
    else:
        secure_ltv = Decimal("67")
        unsecure_s1 = Decimal("48.00")

    if overall_ltv <= secure_ltv:
        if requested_tenure in (6, 7):
            return ("Royal", 7)
        return ("Royal", requested_tenure)

    secure_weight = secure_ltv / overall_ltv
    unsecure_weight = (overall_ltv - secure_ltv) / overall_ltv

    min_opp = (secure_weight * secure_s1) / Decimal("12")
    max_opp = (
        secure_weight * secure_s1 +
        unsecure_weight * unsecure_s1
    ) / Decimal("12")

    min_opp = min_opp.quantize(Decimal("0.01"), ROUND_HALF_UP)
    max_opp = max_opp.quantize(Decimal("0.01"), ROUND_HALF_UP)

    if min_opp <= monthly_opp <= max_opp:
        if requested_tenure in (6, 7):
            return ("Delight", 6)
        return ("Delight", requested_tenure)

    if requested_tenure in (6, 7):
        return ("Royal", 7)

    return ("Royal", requested_tenure)

# ============================================================
# INTEREST ENGINE
# ============================================================

def secure_slab3(tenure):
    r = Decimal("0.229")
    m = Decimal("12")
    t = Decimal(str(tenure))
    compound = (Decimal("1") + r/m) ** t
    result = (compound - Decimal("1")) * m / t
    return (result * 100).quantize(Decimal("0.00"), ROUND_HALF_UP)

def interest_engine(scheme, tenure, overall_ltv, monthly_opp, secure_ltv_override=None):
    # secure_ltv_override: when provided, bypasses the tenure-based secure_ltv lookup.
    # Used for 90D jumping 6M where Royal LTV must be 66, not 67.

    if scheme == "Delight":
        secure_s1 = SECURE_S1_DELIGHT
        secure_s2 = SECURE_S2_DELIGHT
    else:
        secure_s1 = SECURE_S1_ROYAL
        secure_s2 = SECURE_S2_ROYAL

    # secure_ltv = Decimal("67") if tenure != 12 else Decimal("60")
    if secure_ltv_override is not None:
        secure_ltv = secure_ltv_override
    elif tenure == 6:
        secure_ltv = Decimal("67")
    elif tenure == 7:
        secure_ltv = Decimal("66")
    else:
        secure_ltv=Decimal("60")
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

    s2 = (
        secure_weight * secure_s2 +
        unsecure_weight * calc_unsecure[1]
    ).quantize(Decimal("0.00"), ROUND_HALF_UP)

    s3 = (
        secure_weight * secure_s3 +
        unsecure_weight * calc_unsecure[2]
    ).quantize(Decimal("0.00"), ROUND_HALF_UP)

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
        "name": "Foreclosure",
        "chargeType": "foreclosure",
        "chargeCalculationType": "fixed-percentage",
        "applicableProcesses": ["fresh-loan", "renewal", "release"],
        "chargeValue": 0,
        "maxValue": 100000,
        "cityId": None,
        "percentageOn": "loanamount",
        "chargesMetaData": {"duration": 2},
        "minValue": 999
    }

    try:
        data = json.loads(json_str) if str(json_str).strip() else fallback
        if not isinstance(data, dict):
            data = dict(fallback)
    except Exception:
        data = dict(fallback)

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


def update_bs2_legal_name_pf_fc(text, pf_value, fc_duration_months, has_pf_in_refname):
    updated = str(text).strip()

    updated = re.sub(r'\bFC(?:\s*[-:]?\s*\d+D)?\b', 'FC', updated, flags=re.IGNORECASE)

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
            updated = re.sub(
                r'PF\s*[-:]?\s*[0-9]+(?:\.[0-9]+)?\s*%',
                f'PF {pf_str}',
                updated,
                count=1,
                flags=re.IGNORECASE
            )
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

    updated = re.sub(r'\s+', ' ', updated).strip()
    return updated

# ============================================================
# JSON UPDATE
# ============================================================

def _find_slab_list(node):
    if isinstance(node, dict):
        if "interestSlabs" in node and isinstance(node["interestSlabs"], list):
            return node["interestSlabs"]
        for value in node.values():
            found = _find_slab_list(value)
            if found is not None:
                return found
    elif isinstance(node, list):
        if len(node) >= 3 and all(isinstance(item, dict) for item in node):
            if any("interestRate" in item for item in node):
                return node
        for item in node:
            found = _find_slab_list(item)
            if found is not None:
                return found
    return None


def update_interest_json(json_str, slabs, tenure_days, slab_days=None):
    # slab_days: optional list of (fromDay, toDay) tuples, one per slab.
    # When provided (90D jumping), each slab's fromDay and toDay are overwritten.
    # When None (30D jumping / default), only the last slab's toDay is updated.
    try:
        data = json.loads(json_str)
    except Exception:
        return json_str

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
        if "toDay" in data:
            data["toDay"] = tenure_days
        return json.dumps(data)

    return json.dumps(data)

with tab1:
    # ============================================================
    # STREAMLIT FLOW
    # ============================================================

    st.subheader("Input Parameters")

    with st.expander("Demo constructs available", expanded=False):
        c1, c2 = st.columns([1, 1])
        c1.download_button("Download Flexi demo JSON", json.dumps(DUMMY_FLEXI_CONSTRUCT, indent=2), "Flexi_pf_demo.json", mime="application/json")
        c2.download_button("Download Fixed demo JSON", json.dumps(DUMMY_FIXED_CONSTRUCT, indent=2), "fixed_pf_demo.json", mime="application/json")

    # ── Top-level flow selector: Jumping (30D/90D mix) or HIP ──────────────────
    FLOW_OPTIONS = ["Jumping (30D / 90D)", "HIP"]
    if "selected_flow" not in st.session_state:
        st.session_state.selected_flow = FLOW_OPTIONS[0]

    selected_flow = st.selectbox(
        "Flow",
        options=FLOW_OPTIONS,
        index=FLOW_OPTIONS.index(st.session_state.selected_flow),
        key="flow_selector"
    )
    st.session_state.selected_flow = selected_flow
    is_hip_flow = selected_flow == "HIP"

    # Jumping flow columns include "Product Type" so each row can be 30D or 90D.
    # HIP flow columns omit it — HIP is injected automatically on Compute.
    JUMPING_COLUMNS = ["customerLtv", "TS", "slab1 ROI", "PF Tag", "Flow", "PF val", "Tenure", "Product Type"]
    HIP_COLUMNS     = ["customerLtv", "TS", "slab1 ROI", "PF Tag", "Flow", "PF val", "Tenure"]

    JUMPING_DEFAULTS = pd.DataFrame([
        {"customerLtv": 77, "TS": "6-12L", "slab1 ROI": 1.09, "PF Tag": "Nopf",  "Flow": "RWL", "PF val": "0.50%",      "Tenure": 6,  "Product Type": "30D Jumping"},
        {"customerLtv": 77, "TS": "3-6L",  "slab1 ROI": 1.09, "PF Tag": "Flexi", "Flow": "FWD", "PF val": "0.1%-0.70%", "Tenure": 12, "Product Type": "90D Jumping"},
    ], columns=JUMPING_COLUMNS)

    HIP_DEFAULTS = pd.DataFrame([
        {"customerLtv": 75, "TS": "<3L", "slab1 ROI": 1.19, "PF Tag": "Flexi", "Flow": "FWD", "PF val": "0.70%-1.00%", "Tenure": 7},
    ], columns=HIP_COLUMNS)

    # Separate session-state keys so switching flows doesn't wipe entered data
    state_key = "input_df_HIP" if is_hip_flow else "input_df_Jumping"
    if state_key not in st.session_state:
        st.session_state[state_key] = (HIP_DEFAULTS if is_hip_flow else JUMPING_DEFAULTS).copy()

    if is_hip_flow:
        st.write("Enter HIP rows below and click **Compute**.")
        edited_input_df = st.data_editor(
            st.session_state[state_key],
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "PF Tag": st.column_config.SelectboxColumn(options=["Flexi", "Fixed", "Nopf"]),
                "Flow":   st.column_config.SelectboxColumn(options=["FWD", "RWL"]),
            },
            key="editor_HIP"
        )
    else:
        st.write("Enter Jumping rows below — set **Product Type** per row to `30D Jumping` or `90D Jumping`.")
        edited_input_df = st.data_editor(
            st.session_state[state_key],
            use_container_width=True,
            num_rows="dynamic",
            column_config={
                "Product Type": st.column_config.SelectboxColumn(options=["30D Jumping", "90D Jumping"]),
                "PF Tag":       st.column_config.SelectboxColumn(options=["Flexi", "Fixed", "Nopf"]),
                "Flow":         st.column_config.SelectboxColumn(options=["FWD", "RWL"]),
            },
            key="editor_Jumping"
        )

    if st.button("Compute"):
        if True:

            st.session_state[state_key] = edited_input_df.copy()
            cleaned_input_df = _drop_empty_rows(edited_input_df)
            if cleaned_input_df.empty:
                st.warning("Please enter at least one input row before computing.")
                st.stop()

            cleaned_input_df = cleaned_input_df.copy()
            # HIP flow: inject "HIP" as Product Type (column not shown in table)
            # Jumping flow: Product Type already present per row — no injection needed
            if is_hip_flow:
                cleaned_input_df["Product Type"] = "HIP"

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
                input_roi = _get_row_value(row, ["slab1 ROI", "slab1ROI", "roi", "slab1_opp"])
                input_pf_tag = _get_row_value(row, ["PF Tag", "pf_tag", "pfTag"])
                input_flow = _get_row_value(row, ["Flow", "flow"])
                input_pf_val = _get_row_value(row, ["PF val", "PF Value", "pf_val", "pfValue"])
                input_tenure = _get_row_value(row, ["Tenure", "tenure"])
                input_product_type = _get_row_value(row, ["Product Type", "product_type", "productType"])
                pt_lower = str(input_product_type).strip().lower() if input_product_type is not None else ""
                is_90d_jumping = pt_lower == "90d jumping"
                is_hip = pt_lower == "hip"

                refname = _update_refname_ltv_code(refname, input_ltv)
                refname = _update_refname_opp(refname, input_roi)
                refname = _update_refname_pf(refname, input_pf_tag, input_pf_val)
                refname = _update_refname_flow(refname, input_flow)
                refname = _update_refname_ts(refname, input_ts_tag)

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

                scheme_min_val = _parse_int(input_scheme_min)
                df.at[idx, "SchemeMin"] = str(scheme_min_val) if scheme_min_val is not None else None or df.at[idx, "SchemeMin"]
                scheme_max_val = _parse_int(input_scheme_max)
                df.at[idx, "SchemeMax"] = str(scheme_max_val) if scheme_max_val is not None else None or df.at[idx, "SchemeMax"]
                df.at[idx, "customerLtv"] = float(overall_ltv)

                if is_90d_jumping:
                    # 90D jumping always uses Royal, tenure stays as requested (no decision_engine override)
                    scheme = "Royal"
                    final_tenure = requested_tenure
                else:
                    scheme, final_tenure = decision_engine(
                        overall_ltv,
                        monthly_opp,
                        requested_tenure
                    )
                # For 90D jumping 6M: Royal LTV is 66, not 67.
                # (30D jumps 6M→7M via decision_engine so it already uses 66 naturally.)
                # This also correctly propagates into PF and FC via the denominator.
                secure_ltv_override_90d = Decimal("66") if (is_90d_jumping and final_tenure == 6) else None

                df.at[idx, "tenure"] = int(final_tenure)
                if "Tenure" in df.columns:
                    # Assign with correct dtype: if column is numeric, use int; if string, use str
                    try:
                        # Try assigning as integer if column is numeric
                        df.at[idx, "Tenure"] = int(final_tenure)
                    except (ValueError, TypeError):
                        # Fall back to string if numeric assignment fails
                        df.at[idx, "Tenure"] = int(final_tenure)
                # HIP: secure tenure = final_tenure from decision engine; unsecure tenure is always 24M
                if is_hip:
                    if "bs1-tenure" in df.columns:
                        try:
                            df.at[idx, "bs1-tenure"] = int(final_tenure)
                        except (ValueError, TypeError):
                            df.at[idx, "bs1-tenure"] = str(final_tenure)
                    if "bs2-tenure" in df.columns:
                        try:
                            df.at[idx, "bs2-tenure"] = int(24)
                        except (ValueError, TypeError):
                            df.at[idx, "bs2-tenure"] = str(24)
                refname = update_refname_tenure(refname, final_tenure)
                refname = re.sub(r'(%)(\d{1,2}M\b)', r'\1 \2', refname)
                df.at[idx, "refName"] = refname
                if "refno" in df.columns:
                    df.at[idx, "refno"] = refname

                if "description" in df.columns:
                    flow_text = str(input_flow).strip().upper() if input_flow is not None else ""
                    if flow_text == "RWL":
                        df.at[idx, "description"] = "RWL"
                    elif flow_text == "FWD":
                        df.at[idx, "description"] = "FL TO"

                if "applicableProcesses" in df.columns:
                    flow_text = str(input_flow).strip().upper() if input_flow is not None else ""
                    if flow_text == "RWL":
                        df.at[idx, "applicableProcesses"] = "renewal"
                    elif flow_text == "FWD":
                        df.at[idx, "applicableProcesses"] = "fresh-loan,takeover-loan"

                if "bs1-legalName" in df.columns:
                    if is_90d_jumping:
                        df.at[idx, "bs1-legalName"] = "Rupeek Ultra"
                    else:
                        df.at[idx, "bs1-legalName"] = f"Rupeek {scheme}"

                result = interest_engine(
                    scheme,
                    final_tenure,
                    overall_ltv,
                    monthly_opp,
                    secure_ltv_override=secure_ltv_override_90d
                )

                if "bs1-ltv" in df.columns:
                    df.at[idx, "bs1-ltv"] = float(result["secure_ltv"])

                tenure_days = get_tenure_days(final_tenure)

                # For 90D jumping: override slab day boundaries (0-90, 91-120, 121-tenure*30)
                # For 30D jumping / all others: slab_days=None preserves existing behaviour
                if is_90d_jumping:
                    last_day = int(final_tenure) * 30
                    slab_days_override = [(0, 90), (91, 120), (121, last_day)]
                else:
                    slab_days_override = None

                df.at[idx, "OverallInterestCalculation"] = update_interest_json(
                    df.at[idx, "OverallInterestCalculation"],
                    result["overall_slabs"],
                    tenure_days,
                    slab_days=slab_days_override
                )

                df.at[idx, "bs1-addon-1"] = update_interest_json(
                    df.at[idx, "bs1-addon-1"],
                    result["secure_slabs"],
                    tenure_days,
                    slab_days=slab_days_override
                )

                # HIP: unsecure addon last slab toDay is 24M * 30 = 720 days (fixed unsecure tenure)
                bs2_addon1_tenure_days = 720 if is_hip else tenure_days
                df.at[idx, "bs2-addon-1"] = update_interest_json(
                    df.at[idx, "bs2-addon-1"],
                    result["unsecure_slabs"],
                    bs2_addon1_tenure_days,
                    slab_days=slab_days_override
                )

                if "bs2-calculation" in df.columns:
                    df.at[idx, "bs2-calculation"] = update_interest_json(
                        df.at[idx, "bs2-calculation"],
                        result["unsecure_slabs"],
                        tenure_days,
                        slab_days=slab_days_override
                    )

                secure_ltv = result["secure_ltv"]
                denominator = Decimal("1") - (secure_ltv / overall_ltv)
                if denominator == 0:
                    continue

                min_pf_input = pf_min if pf_min is not None else overall_pf
                max_pf_input = pf_max if pf_max is not None else overall_pf

                min_unsecure_pf = (min_pf_input / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP) if min_pf_input is not None else None
                max_unsecure_pf = (max_pf_input / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP) if max_pf_input is not None else None

                flow_pf_tag = str(input_pf_tag).strip().lower() if input_pf_tag is not None else ""
                refname_lower = str(df.at[idx, "refName"]).lower()
                is_flexi = flow_pf_tag == "flexi" or any(token in refname_lower for token in ["flexipf", "flexi pf", "flexi-pf"])
                charge_pf_value = (max_unsecure_pf if is_flexi else min_unsecure_pf) if not is_nopf else None

                # Compute applicable_processes here so it is available for both
                # the PF charge (bs2-charge-2) and the FC charge (bs2-charge-3)
                if "applicableProcesses" in df.columns:
                    applicable_processes = [p.strip() for p in str(df.at[idx, "applicableProcesses"]).split(",") if p.strip()]
                else:
                    applicable_processes = ["fresh-loan", "takeover-loan"] if (str(input_flow).strip().upper() == "FWD") else ["renewal"] if (str(input_flow).strip().upper() == "RWL") else ["fresh-loan", "release"]

                if is_nopf:
                    if "chargeText" in df.columns:
                        df.at[idx, "chargeText"] = "{}"
                else:
                    if "chargeText" in df.columns and charge_pf_value is not None:
                        charge_text_overall_pf = max_pf_input if is_flexi else min_pf_input
                        df.at[idx, "chargeText"] = update_charge_text(
                            df.at[idx, "chargeText"],
                            charge_pf_value,
                            charge_text_overall_pf
                        )

                    if "bs2-charge-2" in df.columns and charge_pf_value is not None and min_unsecure_pf is not None and max_unsecure_pf is not None:
                        df.at[idx, "bs2-charge-2"] = update_bs2_charge_2(
                            df.at[idx, "bs2-charge-2"],
                            charge_pf_value,
                            min_unsecure_pf,
                            max_unsecure_pf,
                            is_flexi,
                            applicable_processes=applicable_processes
                        )

                foreclosure_overall = Decimal("1.00")
                foreclosure_duration = 3 if final_tenure in (6, 7, 12) else 2
                foreclosure_unsecure = (foreclosure_overall / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP)

                if is_nopf:
                    if "bs2-charge-2" in df.columns:
                        df.at[idx, "bs2-charge-2"] = update_foreclosure_charge(
                            df.at[idx, "bs2-charge-2"],
                            foreclosure_unsecure,
                            foreclosure_duration,
                            applicable_processes
                        )
                    if "bs2-charge-3" in df.columns:
                        df.at[idx, "bs2-charge-3"] = "{}"
                    if "bs2-NoOfCharges" in df.columns:
                        df.at[idx, "bs2-NoOfCharges"] = 2
                else:
                    if "bs2-charge-3" in df.columns:
                        df.at[idx, "bs2-charge-3"] = update_foreclosure_charge(
                            df.at[idx, "bs2-charge-3"],
                            foreclosure_unsecure,
                            foreclosure_duration,
                            applicable_processes
                        )
                    if "bs2-NoOfCharges" in df.columns:
                        df.at[idx, "bs2-NoOfCharges"] = 3

                if "bs2-legalName" in df.columns:
                    if is_hip:
                        # HIP legalname format: "Rupeek Loan f8 24M PF HIP X.XX% FC 90D"
                        # 24M is fixed (unsecure tenure); f8 encoding is always f8 for HIP.
                        # Replace the % value inside "PF HIP X.XX%" with foreclosure_unsecure.
                        # Do NOT run the standard update_bs2_legal_name_pf_fc which would add a second PF.
                        hip_ln = str(df.at[idx, "bs2-legalName"])
                        fc_str_hip = str(((foreclosure_unsecure * 2).quantize(Decimal("1"), ROUND_HALF_UP) / 2).quantize(Decimal("0.00")))
                        hip_ln = re.sub(
                            r'(PF\s+HIP\s+)[0-9]+(?:\.[0-9]+)?(%)',
                            rf'\g<1>{fc_str_hip}\g<2>',
                            hip_ln, count=1, flags=re.IGNORECASE
                        )
                        # Normalise FC duration marker
                        hip_ln = re.sub(r'\bFC(?:\s*[-:]?\s*\d+D)?\b', 'FC', hip_ln, flags=re.IGNORECASE)
                        if int(foreclosure_duration) == 3:
                            hip_ln = re.sub(r'\bFC\b', 'FC 90D', hip_ln, count=1, flags=re.IGNORECASE)
                        elif int(foreclosure_duration) == 4:
                            hip_ln = re.sub(r'\bFC\b', 'FC 120D', hip_ln, count=1, flags=re.IGNORECASE)
                        df.at[idx, "bs2-legalName"] = re.sub(r'\s+', ' ', hip_ln).strip()
                    else:
                        encoding = "th7.si5" if final_tenure == 12 else "f8"
                        updated_legal_name = update_bs2_legal_name(
                            df.at[idx, "bs2-legalName"],
                            final_tenure,
                            encoding
                        )
                        has_pf_in_name = not is_nopf
                        df.at[idx, "bs2-legalName"] = update_bs2_legal_name_pf_fc(
                            updated_legal_name,
                            foreclosure_unsecure,
                            foreclosure_duration,
                            has_pf_in_name
                        )

                # Re-read configured values after all updates so checker compares final configured vs calculated outputs.
                configured_overall_ir_now = _extract_interest_rates(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else []
                configured_secure_ir_now = _extract_interest_rates(df.at[idx, "bs1-addon-1"]) if "bs1-addon-1" in df.columns else []
                configured_unsecure_ir_now = _extract_interest_rates(df.at[idx, "bs2-addon-1"]) if "bs2-addon-1" in df.columns else []
                configured_tenure_now = df.at[idx, "tenure"] if "tenure" in df.columns else ""
                configured_todayslab3_now = _extract_slab3_today(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else None
                configured_secure_ltv_now = df.at[idx, "bs1-ltv"] if "bs1-ltv" in df.columns else ""
                configured_bs1_legalname_now = df.at[idx, "bs1-legalName"] if "bs1-legalName" in df.columns else ""

                configured_pf_min_now, configured_pf_max_now = (None, None)
                if "bs2-charge-2" in df.columns:
                    configured_pf_min_now, configured_pf_max_now = _extract_pf_config_values(df.at[idx, "bs2-charge-2"])

                if is_nopf and "bs2-charge-2" in df.columns:
                    configured_fc_now = _extract_charge_value(df.at[idx, "bs2-charge-2"])
                elif "bs2-charge-3" in df.columns:
                    configured_fc_now = _extract_charge_value(df.at[idx, "bs2-charge-3"])
                else:
                    configured_fc_now = None

                df.at[idx, "CHK PF Config Min"] = str(configured_pf_min_now) if configured_pf_min_now is not None else ""
                df.at[idx, "CHK PF Config Max"] = str(configured_pf_max_now) if configured_pf_max_now is not None else ""
                df.at[idx, "CHK PF Calc Min"] = str(min_unsecure_pf) if min_unsecure_pf is not None else ""
                df.at[idx, "CHK PF Calc Max"] = str(max_unsecure_pf) if max_unsecure_pf is not None else ""

                df.at[idx, "CHK FC Config"] = str(configured_fc_now) if configured_fc_now is not None else ""
                df.at[idx, "CHK FC Calc"] = str(foreclosure_unsecure)

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

            df = finalize_output_columns(df)
            st.session_state.df = df

            st.success("Computation Complete")
            st.subheader("Updated Schemes")
            st.dataframe(df, use_container_width=True)

    if "df" in st.session_state:
        st.download_button(
            "Download Updated CSV",
            st.session_state.df.to_csv(index=False),
            "updated_scheme.csv"
        )

with tab2:
    st.subheader("Phase 2 — Checker")
    st.write("Upload a scheme CSV to validate all computed fields against what the engine would calculate from the refname.")

    uploaded_checker = st.file_uploader("Upload Scheme CSV", type=["csv"], key="checker_upload")

    # Check fields: (display_label, config_col, calc_col)
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
    ]

    def _run_checker_on_df(raw_df, product_type="30D Jumping"):
        """Re-uses the exact Phase 1 compute engine on a full-scheme CSV.
        Returns df with CHK columns populated."""
        df = raw_df.copy()
        for checker_column in CHECKER_COLUMNS:
            if checker_column not in df.columns:
                df[checker_column] = ""

        for idx in df.index:
            row = df.loc[idx]
            refname = str(df.at[idx, "refName"]).strip() if "refName" in df.columns else ""

            # ── Extract inputs from refname & existing columns ──────────────
            overall_ltv    = extract_ltv_from_code(refname)
            requested_tenure = extract_tenure(refname)
            monthly_opp    = extract_opp(refname)
            pf_min, pf_max = extract_pf_range(refname)
            if pf_min is None:
                extracted_pf = extract_pf(refname)
                pf_min, pf_max = extracted_pf, extracted_pf

            # Read customerLtv column if available (more reliable than code)
            if "customerLtv" in df.columns:
                ltv_col = _parse_decimal(df.at[idx, "customerLtv"])
                if ltv_col is not None:
                    overall_ltv = ltv_col

            # Read tenure column if available
            if "tenure" in df.columns:
                ten_col = _parse_int(df.at[idx, "tenure"])
                if ten_col is not None:
                    requested_tenure = ten_col

            if not all([overall_ltv, requested_tenure, monthly_opp]):
                continue

            # ── Product type is explicitly selected by the user via dropdown ──
            refname_lower  = refname.lower()
            is_hip_chk     = (product_type == "HIP")
            is_90d_chk     = (product_type == "90D Jumping")

            # PF tag
            refname_nopf = not bool(re.search(r"PF\s*[-:]?\s*[0-9]", refname, re.IGNORECASE))
            is_nopf_chk  = refname_nopf

            overall_pf = None if is_nopf_chk else (pf_max if pf_max is not None else pf_min)

            # ── Decision / scheme ───────────────────────────────────────────
            if is_hip_chk or is_90d_chk:
                scheme_chk     = "Royal"
                final_ten_chk  = requested_tenure
            else:
                scheme_chk, final_ten_chk = decision_engine(overall_ltv, monthly_opp, requested_tenure)

            secure_ltv_ov = Decimal("66") if ((is_hip_chk or is_90d_chk) and final_ten_chk == 6) else None

            result = interest_engine(scheme_chk, final_ten_chk, overall_ltv, monthly_opp,
                                     secure_ltv_override=secure_ltv_ov)

            tenure_days_chk = get_tenure_days(final_ten_chk)

            secure_ltv    = result["secure_ltv"]
            denominator   = Decimal("1") - (secure_ltv / overall_ltv)
            if denominator == 0:
                continue

            # PF
            min_pf_i = pf_min if pf_min is not None else overall_pf
            max_pf_i = pf_max if pf_max is not None else overall_pf
            min_unsecure_pf_chk = (min_pf_i / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP) if min_pf_i is not None else None
            max_unsecure_pf_chk = (max_pf_i / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP) if max_pf_i is not None else None

            # FC
            fc_overall_chk    = Decimal("1.00")
            fc_duration_chk   = 3 if final_ten_chk in (6, 7, 12) else 2
            fc_unsecure_chk   = (fc_overall_chk / denominator).quantize(Decimal("0.00"), ROUND_HALF_UP)

            # ── Read configured values from the uploaded CSV ─────────────────
            cfg_overall_ir  = _extract_interest_rates(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else []
            cfg_secure_ir   = _extract_interest_rates(df.at[idx, "bs1-addon-1"]) if "bs1-addon-1" in df.columns else []
            cfg_unsec_ir    = _extract_interest_rates(df.at[idx, "bs2-addon-1"]) if "bs2-addon-1" in df.columns else []
            cfg_tenure      = df.at[idx, "tenure"] if "tenure" in df.columns else ""
            cfg_slab3today  = _extract_slab3_today(df.at[idx, "OverallInterestCalculation"]) if "OverallInterestCalculation" in df.columns else None
            cfg_secure_ltv  = df.at[idx, "bs1-ltv"] if "bs1-ltv" in df.columns else ""
            cfg_bs1_ln      = df.at[idx, "bs1-legalName"] if "bs1-legalName" in df.columns else ""
            cfg_pf_min_v, cfg_pf_max_v = _extract_pf_config_values(df.at[idx, "bs2-charge-2"]) if "bs2-charge-2" in df.columns else (None, None)
            if is_nopf_chk and "bs2-charge-2" in df.columns:
                cfg_fc_v = _extract_charge_value(df.at[idx, "bs2-charge-2"])
            elif "bs2-charge-3" in df.columns:
                cfg_fc_v = _extract_charge_value(df.at[idx, "bs2-charge-3"])
            else:
                cfg_fc_v = None

            # ── Write CHK columns ────────────────────────────────────────────
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
            df.at[idx, "CHK BS1 LegalName Calc"]   = "Rupeek Ultra" if is_90d_chk else f"Rupeek {scheme_chk}"
            df.at[idx, "CHK PF Config Min"]        = str(cfg_pf_min_v) if cfg_pf_min_v is not None else ""
            df.at[idx, "CHK PF Calc Min"]          = str(min_unsecure_pf_chk) if min_unsecure_pf_chk is not None else ""
            df.at[idx, "CHK PF Config Max"]        = str(cfg_pf_max_v) if cfg_pf_max_v is not None else ""
            df.at[idx, "CHK PF Calc Max"]          = str(max_unsecure_pf_chk) if max_unsecure_pf_chk is not None else ""
            df.at[idx, "CHK FC Config"]            = str(cfg_fc_v) if cfg_fc_v is not None else ""
            df.at[idx, "CHK FC Calc"]              = str(fc_unsecure_chk)

        return df

    def _normalize_decimal_list(val):
        """Normalize a comma-separated decimal string to 2dp for comparison."""
        parts = [p.strip() for p in val.split(',') if p.strip()]
        out = []
        for p in parts:
            try:
                out.append(str(Decimal(p).quantize(Decimal("0.01"), ROUND_HALF_UP)))
            except Exception:
                out.append(p)
        return ','.join(out)

    def _build_status_df(checked_df):
        """Turn CHK columns into a human-readable PASS/FAIL table."""
        rows = []
        for idx in checked_df.index:
            refname = str(checked_df.at[idx, "refName"]) if "refName" in checked_df.columns else str(idx)
            row_result = {"refName": refname}
            all_pass = True
            for label, cfg_col, calc_col in CHECK_FIELDS:
                cfg_val  = str(checked_df.at[idx, cfg_col]).strip()  if cfg_col  in checked_df.columns else ""
                calc_val = str(checked_df.at[idx, calc_col]).strip() if calc_col in checked_df.columns else ""
                if cfg_val == "" and calc_val == "":
                    status = "—"
                elif _normalize_decimal_list(cfg_val) == _normalize_decimal_list(calc_val):
                    status = "✅ OK"
                else:
                    status = f"❌ Config: {cfg_val}  |  Expected: {calc_val}"
                    all_pass = False
                row_result[label] = status
            row_result["Overall"] = "✅ ALL OK" if all_pass else "❌ ERRORS"
            rows.append(row_result)
        return pd.DataFrame(rows)

    if uploaded_checker is not None:
        raw_checker_df = pd.read_csv(uploaded_checker)
        st.write(f"Loaded **{len(raw_checker_df)}** scheme rows.")

        product_type_chk = st.selectbox(
            "Select Product Type",
            ["30D Jumping", "90D Jumping", "HIP"],
            key="product_type_chk"
        )

        if st.button("Run Check", key="run_check_btn"):
            checked_df = _run_checker_on_df(raw_checker_df, product_type_chk)
            st.session_state["checked_df"] = checked_df
            status_df = _build_status_df(checked_df)
            st.session_state["status_df"] = status_df

    if "status_df" in st.session_state:
        status_df = st.session_state["status_df"]
        checked_df = st.session_state["checked_df"]

        # Summary banner
        total   = len(status_df)
        errors  = int((status_df["Overall"] != "✅ ALL OK").sum())
        st.markdown(f"### Results: {total - errors} / {total} schemes OK &nbsp;&nbsp; {'🟢' if errors == 0 else '🔴'} {errors} with errors")

        # Colour the Overall column
        def _highlight_overall(val):
            if "ALL OK" in str(val):
                return "background-color: #d4edda; color: #155724; font-weight: bold"
            elif "ERRORS" in str(val):
                return "background-color: #f8d7da; color: #721c24; font-weight: bold"
            return ""

        def _highlight_cell(val):
            v = str(val)
            if v.startswith("✅"):
                return "background-color: #d4edda"
            elif v.startswith("❌"):
                return "background-color: #f8d7da"
            return ""

        styled = status_df.style.applymap(_highlight_cell).applymap(_highlight_overall, subset=["Overall"])
        st.dataframe(styled, use_container_width=True)

        # Download check report
        st.download_button(
            "Download Check Report CSV",
            status_df.to_csv(index=False),
            "check_report.csv",
            key="dl_check_report"
        )

        # Rectify button — re-runs Phase 1 engine on the uploaded schemes and outputs corrected CSV
        if errors > 0:
            st.markdown("---")
            st.write(f"**{errors} scheme(s) have errors.** Click below to auto-rectify them.")
            if st.button("Rectify Errors", key="rectify_btn"):
                rectified_df = checked_df.copy()
                # Re-apply all Phase 1 updates (interest, PF, FC, legalName, etc.)
                # by running the same engine that Phase 1 uses
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
                        if ltv_col is not None:
                            overall_ltv = ltv_col
                    if "tenure" in rectified_df.columns:
                        ten_col = _parse_int(rectified_df.at[idx, "tenure"])
                        if ten_col is not None:
                            requested_tenure = ten_col

                    if not all([overall_ltv, requested_tenure, monthly_opp]):
                        continue

                    refname_lower = refname.lower()
                    is_hip_r  = "hip" in refname_lower
                    is_90d_r  = False
                    if "OverallInterestCalculation" in rectified_df.columns:
                        s3t = _extract_slab3_today(rectified_df.at[idx, "OverallInterestCalculation"])
                        if s3t is not None:
                            try:
                                if int(s3t) > 60 and not is_hip_r:
                                    is_90d_r = True
                            except Exception:
                                pass

                    refname_nopf = not bool(re.search(r"PF\s*[-:]?\s*[0-9]", refname, re.IGNORECASE))
                    is_nopf_r    = refname_nopf
                    overall_pf   = None if is_nopf_r else (pf_max if pf_max is not None else pf_min)

                    if is_hip_r or is_90d_r:
                        scheme_r, final_ten_r = "Royal", requested_tenure
                    else:
                        scheme_r, final_ten_r = decision_engine(overall_ltv, monthly_opp, requested_tenure)

                    secure_ltv_ov_r = Decimal("66") if ((is_hip_r or is_90d_r) and final_ten_r == 6) else None
                    result_r = interest_engine(scheme_r, final_ten_r, overall_ltv, monthly_opp,
                                               secure_ltv_override=secure_ltv_ov_r)

                    tenure_days_r = get_tenure_days(final_ten_r)
                    secure_ltv_r  = result_r["secure_ltv"]
                    denom_r = Decimal("1") - (secure_ltv_r / overall_ltv)
                    if denom_r == 0:
                        continue

                    # Determine slab_days for 90D
                    slab_days_r = [(0, 90), (91, 120), (121, int(final_ten_r) * 30)] if is_90d_r else None

                    if "bs1-ltv" in rectified_df.columns:
                        rectified_df.at[idx, "bs1-ltv"] = float(result_r["secure_ltv"])
                    if "bs1-legalName" in rectified_df.columns:
                        if is_90d_r:
                            rectified_df.at[idx, "bs1-legalName"] = "Rupeek Ultra"
                        else:
                            rectified_df.at[idx, "bs1-legalName"] = f"Rupeek {scheme_r}"

                    for col, slabs in [
                        ("OverallInterestCalculation", result_r["overall_slabs"]),
                        ("bs1-addon-1",                result_r["secure_slabs"]),
                    ]:
                        if col in rectified_df.columns:
                            rectified_df.at[idx, col] = update_interest_json(
                                rectified_df.at[idx, col], slabs, tenure_days_r, slab_days=slab_days_r)

                    bs2_addon_days_r = 720 if is_hip_r else tenure_days_r
                    if "bs2-addon-1" in rectified_df.columns:
                        rectified_df.at[idx, "bs2-addon-1"] = update_interest_json(
                            rectified_df.at[idx, "bs2-addon-1"], result_r["unsecure_slabs"],
                            bs2_addon_days_r, slab_days=slab_days_r)
                    if "bs2-calculation" in rectified_df.columns:
                        rectified_df.at[idx, "bs2-calculation"] = update_interest_json(
                            rectified_df.at[idx, "bs2-calculation"], result_r["unsecure_slabs"],
                            tenure_days_r, slab_days=slab_days_r)

                    # PF & FC
                    is_flexi_r  = any(t in refname_lower for t in ["flexipf", "flexi pf", "flexi-pf"])
                    flow_text_r = "RWL" if "renewal" in str(rectified_df.at[idx, "applicableProcesses"] if "applicableProcesses" in rectified_df.columns else "").lower() else "FWD"
                    applicable_r = ["renewal"] if flow_text_r == "RWL" else ["fresh-loan", "takeover-loan"]

                    min_pf_i_r = pf_min if pf_min is not None else overall_pf
                    max_pf_i_r = pf_max if pf_max is not None else overall_pf
                    min_unsec_pf_r = (min_pf_i_r / denom_r).quantize(Decimal("0.00"), ROUND_HALF_UP) if min_pf_i_r is not None else None
                    max_unsec_pf_r = (max_pf_i_r / denom_r).quantize(Decimal("0.00"), ROUND_HALF_UP) if max_pf_i_r is not None else None
                    charge_pf_r = (max_unsec_pf_r if is_flexi_r else min_unsec_pf_r) if not is_nopf_r else None

                    if not is_nopf_r and charge_pf_r is not None and min_unsec_pf_r is not None and max_unsec_pf_r is not None:
                        if "chargeText" in rectified_df.columns:
                            overall_pf_for_ct = max_pf_i_r if is_flexi_r else min_pf_i_r
                            rectified_df.at[idx, "chargeText"] = update_charge_text(
                                rectified_df.at[idx, "chargeText"], charge_pf_r, overall_pf_for_ct)
                        if "bs2-charge-2" in rectified_df.columns:
                            rectified_df.at[idx, "bs2-charge-2"] = update_bs2_charge_2(
                                rectified_df.at[idx, "bs2-charge-2"], charge_pf_r,
                                min_unsec_pf_r, max_unsec_pf_r, is_flexi_r,
                                applicable_processes=applicable_r)

                    fc_overall_r  = Decimal("1.00")
                    fc_dur_r      = 3 if final_ten_r in (6, 7, 12) else 2
                    fc_unsec_r    = (fc_overall_r / denom_r).quantize(Decimal("0.00"), ROUND_HALF_UP)

                    if is_nopf_r:
                        if "bs2-charge-2" in rectified_df.columns:
                            rectified_df.at[idx, "bs2-charge-2"] = update_foreclosure_charge(
                                rectified_df.at[idx, "bs2-charge-2"], fc_unsec_r, fc_dur_r, applicable_r)
                    else:
                        if "bs2-charge-3" in rectified_df.columns:
                            rectified_df.at[idx, "bs2-charge-3"] = update_foreclosure_charge(
                                rectified_df.at[idx, "bs2-charge-3"], fc_unsec_r, fc_dur_r, applicable_r)

                # Drop CHK columns from rectified output
                out_cols = [c for c in rectified_df.columns if not c.startswith("CHK")]
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
