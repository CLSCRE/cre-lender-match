#!/usr/bin/env python3
"""
Local server for CRE Lender Match web app.
Serves the HTML frontend and proxies API calls to HMDA, FDIC, Nominatim, FCC.

Usage:
    python server.py
    Then open http://localhost:8080 in your browser.
"""

import json
import sys
import os
import time
import csv
import io
import bisect
import re
import math
from http.server import HTTPServer, SimpleHTTPRequestHandler
from urllib.parse import urlparse, parse_qs, urlencode
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

try:
    import requests
except ImportError:
    print("ERROR: 'requests' package required. Run: pip install requests")
    sys.exit(1)

PORT = 8080
STATIC_DIR = Path(__file__).parent

# API endpoints
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
FCC_URL = "https://geo.fcc.gov/api/census/area"
HMDA_BASE = "https://ffiec.cfpb.gov/v2/data-browser-api/view"
FDIC_BASE = "https://api.fdic.gov/banks"
NCUA_SEARCH_URL = "https://mapping.ncua.gov/api/Search/GetSearchLocations"
NCUA_DETAILS_URL = "https://mapping.ncua.gov/api/CreditUnionDetails/GetCreditUnionDetails"

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "CRE-Lender-Match/1.0"})

# ════════════════════════════════════════════════════════════════════════
# In-memory cache (1-hour TTL)
# ════════════════════════════════════════════════════════════════════════

_cache = {}
CACHE_TTL = 3600


def cache_get(key):
    if key in _cache:
        data, ts = _cache[key]
        if time.time() - ts < CACHE_TTL:
            return data
        del _cache[key]
    return None


def cache_set(key, data):
    _cache[key] = (data, time.time())


# ════════════════════════════════════════════════════════════════════════
# State abbreviation mapping
# ════════════════════════════════════════════════════════════════════════

STATE_ABBR = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT", "delaware": "DE",
    "florida": "FL", "georgia": "GA", "hawaii": "HI", "idaho": "ID",
    "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

# ════════════════════════════════════════════════════════════════════════
# CRE type mapping
# ════════════════════════════════════════════════════════════════════════

CRE_MAP = {
    "all_cre":              {"label": "All CRE",                     "fields": ["LNRECONS", "LNREMULT", "LNRENROT"]},
    "multifamily":          {"label": "Multifamily (5+ units)",      "fields": ["LNREMULT"]},
    "student_housing":      {"label": "Student Housing",             "fields": ["LNREMULT"]},
    "affordable_housing":   {"label": "Affordable Housing (LIHTC)",  "fields": ["LNREMULT"]},
    "senior_housing":       {"label": "Senior / Independent Living", "fields": ["LNREMULT"]},
    "manufactured_housing": {"label": "Manufactured Housing Park",   "fields": ["LNREMULT"]},
    "hospitality":          {"label": "Hospitality / Hotel",         "fields": ["LNRENROT"]},
    "office":               {"label": "Office",                      "fields": ["LNRENROT"]},
    "retail":               {"label": "Retail / Shopping Center",    "fields": ["LNRENROT"]},
    "industrial":           {"label": "Industrial / Warehouse",      "fields": ["LNRENROT"]},
    "self_storage":         {"label": "Self-Storage",                "fields": ["LNRENROT"]},
    "healthcare":           {"label": "Healthcare / Medical Office", "fields": ["LNRENROT"]},
    "senior_care":          {"label": "Senior Care / SNF / ALF",     "fields": ["LNRENROT"]},
    "mixed_use":            {"label": "Mixed-Use",                   "fields": ["LNRENROT", "LNREMULT"]},
    "special_purpose":      {"label": "Special Purpose",             "fields": ["LNRENROT"]},
    "non_res":              {"label": "Non-Residential Investment",  "fields": ["LNRENROT"]},
    "construction":         {"label": "Construction & Development",  "fields": ["LNRECONS"]},
    "land":                 {"label": "Land / Development Site",     "fields": ["LNRECONS"]},
    "owner_occ":            {"label": "Owner-Occupied CRE",          "fields": ["LNRENROW"]},
    "sba_owner_user":       {"label": "SBA Owner-User (504 / 7a)",   "fields": ["LNRENROW"]},
}

_MF = ["multifamilyK"]
_NONOCC = ["nonOccK"]
_CONS = ["constructionK"]
_OWNER = ["ownerOccK"]

NCUA_CRE_MAP = {
    "all_cre":              ["constructionK", "multifamilyK", "nonOccK"],
    "multifamily":          _MF,
    "student_housing":      _MF,
    "affordable_housing":   _MF,
    "senior_housing":       _MF,
    "manufactured_housing": _MF,
    "hospitality":          _NONOCC,
    "office":               _NONOCC,
    "retail":               _NONOCC,
    "industrial":           _NONOCC,
    "self_storage":         _NONOCC,
    "healthcare":           _NONOCC,
    "senior_care":          _NONOCC,
    "mixed_use":            ["nonOccK", "multifamilyK"],
    "special_purpose":      _NONOCC,
    "non_res":              _NONOCC,
    "construction":         _CONS,
    "land":                 _CONS,
    "owner_occ":            _OWNER,
    "sba_owner_user":       _OWNER,
}

# ════════════════════════════════════════════════════════════════════════
# Specialty lender overlay — niche agency/CMBS/SBA/HUD shops that don't
# show up in FDIC/NCUA call reports but are the real lenders for niche
# product types (student housing, hotel, SNF, self-storage, etc.)
# ════════════════════════════════════════════════════════════════════════

_specialty_lenders = None


def load_specialty_lenders():
    global _specialty_lenders
    if _specialty_lenders is not None:
        return _specialty_lenders
    path = STATIC_DIR / "specialty_lenders.json"
    if not path.exists():
        _specialty_lenders = {}
        return _specialty_lenders
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        _specialty_lenders = {k: v for k, v in data.items() if not k.startswith("_")}
        print(f"  Loaded specialty lenders for {len(_specialty_lenders)} product types")
    except Exception as e:
        print(f"  WARNING: could not load specialty_lenders.json: {e}")
        _specialty_lenders = {}
    return _specialty_lenders


def build_specialty_entries(product_type, state):
    """Return lender dicts from specialty_lenders.json for the product type, shaped to match
    the FDIC/NCUA schema so they merge cleanly into the unified results."""
    specialty = load_specialty_lenders()
    entries = specialty.get(product_type, [])
    out = []
    for e in entries:
        out.append({
            "name": e.get("name", ""),
            "type": e.get("type", "Specialty"),
            "city": "", "state": state or "",
            "website": e.get("website", ""),
            "address": "", "zip": "",
            "phone": "", "ceo": "",
            "assetsM": 0,
            "portfolioK": 0, "portfolioM": 0, "crePct": 0,
            "multifamilyK": 0, "constructionK": 0, "nonOccK": 0, "ownerOccK": 0,
            "dataSource": "Specialty",
            "specialtyFocus": e.get("focus", ""),
            "specialtyNotes": e.get("notes", ""),
            "hmdaDeals": 0, "hmdaVolume": 0,
            "hmdaMinLoan": 0, "hmdaMaxLoan": 0, "hmdaAvgLoan": 0,
        })
    return out

# ════════════════════════════════════════════════════════════════════════
# NCUA cached data loader
# ════════════════════════════════════════════════════════════════════════

_ncua_data = None


def load_ncua_cache():
    global _ncua_data
    if _ncua_data is not None:
        return _ncua_data

    ncua_dir = STATIC_DIR / "ncua_data"
    if not ncua_dir.exists():
        _ncua_data = {}
        return _ncua_data

    files = sorted(ncua_dir.glob("cre_data_*.json"), reverse=True)
    if not files:
        _ncua_data = {}
        return _ncua_data

    print(f"  Loading NCUA cache from {files[0].name}...")
    with open(files[0]) as f:
        _ncua_data = json.load(f)
    print(f"  Loaded {len(_ncua_data)} credit unions")
    return _ncua_data


# ════════════════════════════════════════════════════════════════════════
# Name matching utilities
# ════════════════════════════════════════════════════════════════════════

SKIP_WORDS = {
    "THE", "OF", "AND", "INC", "LLC", "NA", "NATIONAL", "ASSOCIATION",
    "BANK", "FSB", "SSB", "COMPANY", "CORP", "CORPORATION", "GROUP",
    "FEDERAL", "CREDIT", "UNION", "SAVINGS", "TRUST", "N.A.", "CO",
}


def normalize_name(name):
    clean = name.upper().replace(",", " ").replace(".", " ").replace("-", " ").strip()
    return set(w for w in clean.split() if w not in SKIP_WORDS and len(w) > 1)


def name_similarity(name1, name2):
    w1 = normalize_name(name1)
    w2 = normalize_name(name2)
    if not w1 or not w2:
        return 0
    common = len(w1 & w2)
    smaller = min(len(w1), len(w2))
    score = common / smaller if smaller > 0 else 0
    u1, u2 = name1.upper().strip(), name2.upper().strip()
    if u1 in u2 or u2 in u1:
        score = max(score, 0.8)
    return score


# ════════════════════════════════════════════════════════════════════════
# FDIC batch fetch (3 API calls for entire state)
# ════════════════════════════════════════════════════════════════════════

def fetch_fdic_batch(state, min_assets_m=50):
    cache_key = f"fdic_batch:{state}:{min_assets_m}"
    cached = cache_get(cache_key)
    if cached:
        print(f"  FDIC batch: cache hit for {state}")
        return cached

    min_assets_k = min_assets_m * 1000
    inst_fields = "CERT,NAME,CITY,STALP,ASSET,DEP,EQ,WEBADDR,ADDRESS,ZIP,OFFDOM,SPECGRPN"
    fin_fields = "REPDTE,CERT,ASSET,EQ,DEP,LNRE,LNRECONS,LNREMULT,LNRENRES,LNRENROW,LNRENROT,STALP"

    # Step 1: Get latest report date
    print(f"  FDIC: getting latest report date...")
    resp = SESSION.get(f"{FDIC_BASE}/financials", params={
        "sort_by": "REPDTE", "sort_order": "DESC", "limit": "1", "fields": "REPDTE",
    }, timeout=15)
    resp.raise_for_status()
    repdte_data = resp.json().get("data", [])
    if not repdte_data:
        return {"institutions": {}, "financials": {}}
    repdte = repdte_data[0]["data"]["REPDTE"]
    print(f"  FDIC: latest report date = {repdte}")

    # Step 2: Get all financials for state (batch)
    print(f"  FDIC: fetching financials for {state}...")
    resp = SESSION.get(f"{FDIC_BASE}/financials", params={
        "filters": f"STALP:{state} AND REPDTE:{repdte} AND ASSET:[{min_assets_k} TO *]",
        "fields": fin_fields,
        "limit": "10000",
        "sort_by": "ASSET",
        "sort_order": "DESC",
    }, timeout=60)
    resp.raise_for_status()
    fin_list = resp.json().get("data", [])
    financials = {}
    for item in fin_list:
        d = item.get("data", {})
        cert = d.get("CERT")
        if cert:
            financials[str(cert)] = d
    print(f"  FDIC: got financials for {len(financials)} banks")

    # Step 3: Get institution details (names, websites, addresses)
    print(f"  FDIC: fetching institution details for {state}...")
    resp = SESSION.get(f"{FDIC_BASE}/institutions", params={
        "filters": f"STALP:{state} AND ACTIVE:1 AND ASSET:[{min_assets_k} TO *]",
        "fields": inst_fields,
        "limit": "10000",
        "sort_by": "ASSET",
        "sort_order": "DESC",
    }, timeout=30)
    resp.raise_for_status()
    inst_list = resp.json().get("data", [])
    institutions = {}
    for item in inst_list:
        d = item.get("data", {})
        cert = d.get("CERT")
        if cert:
            institutions[str(cert)] = d
    print(f"  FDIC: got details for {len(institutions)} banks")

    result = {"institutions": institutions, "financials": financials, "repdte": repdte}
    cache_set(cache_key, result)
    return result


# ════════════════════════════════════════════════════════════════════════
# NCUA Details API enrichment (phone, CEO, website)
# ════════════════════════════════════════════════════════════════════════

_ncua_details_cache = {}


def _fmt_phone(raw):
    """Format a 10-digit phone string as (XXX) XXX-XXXX."""
    digits = ''.join(c for c in str(raw or '') if c.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return raw or ""


def _fetch_ncua_detail(charter):
    """Fetch phone/CEO/website for one CU. Returns dict or None."""
    if charter in _ncua_details_cache:
        return _ncua_details_cache[charter]
    try:
        resp = SESSION.get(f"{NCUA_DETAILS_URL}/{charter}", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("isError"):
            return None
        result = {
            "phone": _fmt_phone(data.get("creditUnionPhone", "")),
            "ceo": data.get("creditUnionCeo", ""),
            "website": data.get("creditUnionWebsite", ""),
        }
        _ncua_details_cache[charter] = result
        return result
    except Exception:
        return None


def enrich_ncua_details(cu_list):
    """Batch-enrich a list of CU dicts with phone/CEO/website via NCUA Details API."""
    # Only enrich CUs that are missing contact info
    to_enrich = [cu for cu in cu_list if not cu.get("phone") and not cu.get("ceo")]
    if not to_enrich:
        return

    charters = [cu["charter"] for cu in to_enrich]
    print(f"  NCUA Details: enriching {len(charters)} credit unions with phone/CEO/website...")

    results = {}
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(_fetch_ncua_detail, ch): ch for ch in charters}
        for future in futures:
            ch = futures[future]
            try:
                detail = future.result()
                if detail:
                    results[ch] = detail
            except Exception:
                pass

    # Apply results
    enriched = 0
    for cu in to_enrich:
        detail = results.get(cu["charter"])
        if detail:
            cu["phone"] = detail.get("phone", "")
            cu["ceo"] = detail.get("ceo", "")
            if not cu.get("website"):
                cu["website"] = detail.get("website", "")
            enriched += 1

    print(f"  NCUA Details: enriched {enriched}/{len(to_enrich)} credit unions")


# ════════════════════════════════════════════════════════════════════════
# NCUA state filter (from cached JSON)
# ════════════════════════════════════════════════════════════════════════

def get_ncua_for_state(state, min_assets_m=50):
    ncua = load_ncua_cache()
    if not ncua:
        return []

    min_assets = min_assets_m * 1_000_000
    results = []
    for cu_num, cu in ncua.items():
        if cu.get("state") != state:
            continue
        total_assets = cu.get("total_assets", 0) or 0
        if total_assets < min_assets:
            continue

        mf = cu.get("multifamily", 0) or 0
        construction = cu.get("construction_commercial", 0) or 0
        nonocc = cu.get("nonocc_nonfarm", 0) or 0
        owner_occ = cu.get("owner_occ_nonfarm", 0) or 0
        cre_total = mf + construction + nonocc
        cre_pct = round(cre_total / total_assets * 100, 1) if total_assets > 0 else 0

        results.append({
            "charter": cu_num,
            "name": cu.get("cu_name", ""),
            "type": "Credit Union",
            "city": cu.get("city", ""),
            "state": cu.get("state", ""),
            "assetsM": round(total_assets / 1_000_000, 1),
            "multifamilyK": round(mf / 1000, 1),
            "constructionK": round(construction / 1000, 1),
            "nonOccK": round(nonocc / 1000, 1),
            "ownerOccK": round(owner_occ / 1000, 1),
            "crePct": cre_pct,
            "phone": cu.get("phone", ""),
            "ceo": cu.get("ceo", ""),
            "address": cu.get("address", ""),
            "zip": cu.get("zip", ""),
            "website": cu.get("website", ""),
            "dataSource": "NCUA",
        })

    # Enrich with phone/CEO/website from NCUA Details API
    enrich_ncua_details(results)

    return results


# ════════════════════════════════════════════════════════════════════════
# HMDA fetch for deal (multifamily originations)
# ════════════════════════════════════════════════════════════════════════

def fetch_hmda_for_deal(county_fips, state, years="2022,2023,2024"):
    cache_key = f"hmda_deal:{county_fips or state}:{years}"
    cached = cache_get(cache_key)
    if cached:
        print(f"  HMDA: cache hit")
        return cached

    # Get filers directory
    filers = {}
    for year in years.split(","):
        try:
            resp = SESSION.get(f"{HMDA_BASE}/filers", params={"years": year.strip()}, timeout=60)
            resp.raise_for_status()
            for inst in resp.json().get("institutions", []):
                if inst.get("lei") and inst.get("name"):
                    filers[inst["lei"]] = inst["name"]
        except Exception as e:
            print(f"  Warning: filers {year} failed: {e}")

    # Get origination records
    hmda_params = {
        "years": years,
        "actions_taken": "1",
        "total_units": "5-24,25-49,50-99,100-149,>149",
    }
    if county_fips:
        hmda_params["counties"] = county_fips
    else:
        hmda_params["states"] = state

    print(f"  HMDA: fetching originations...")
    resp = SESSION.get(f"{HMDA_BASE}/csv", params=hmda_params, timeout=180)
    resp.raise_for_status()

    text = resp.text
    if not text.strip():
        result = {"filers": filers, "loans": []}
        cache_set(cache_key, result)
        return result

    reader = csv.DictReader(io.StringIO(text))
    loans = []
    for row in reader:
        loans.append({
            "lei": row.get("lei", ""),
            "loan_amount": row.get("loan_amount", "0"),
            "activity_year": row.get("activity_year", ""),
            "county_code": row.get("county_code", ""),
        })

    print(f"  HMDA: got {len(loans)} originations")
    result = {"filers": filers, "loans": loans}
    cache_set(cache_key, result)
    return result


# ════════════════════════════════════════════════════════════════════════
# Build unified lender list from FDIC + NCUA
# ════════════════════════════════════════════════════════════════════════

def build_lender_list(fdic_data, ncua_lenders, product_type, state=""):
    cre_config = CRE_MAP.get(product_type, CRE_MAP["all_cre"])
    fdic_fields = cre_config["fields"]
    ncua_fields = NCUA_CRE_MAP.get(product_type, NCUA_CRE_MAP["all_cre"])

    lenders = []

    # Specialty lender overlay (agency/CMBS/SBA/HUD) for niche product types.
    # These aren't in FDIC/NCUA call reports but are the real lenders for the niche.
    lenders.extend(build_specialty_entries(product_type, state))

    # FDIC banks
    for cert, inst in fdic_data.get("institutions", {}).items():
        fin = fdic_data.get("financials", {}).get(cert, {})
        if not fin:
            continue

        portfolio_k = sum((fin.get(f, 0) or 0) for f in fdic_fields)
        if portfolio_k <= 0:
            continue

        asset_k = fin.get("ASSET", 0) or 0
        construction = fin.get("LNRECONS", 0) or 0
        multifamily = fin.get("LNREMULT", 0) or 0
        non_occ = fin.get("LNRENROT", 0) or 0
        owner_occ = fin.get("LNRENROW", 0) or 0
        cre_total = construction + multifamily + non_occ
        cre_pct = round(cre_total / asset_k * 100, 1) if asset_k > 0 else 0

        lenders.append({
            "cert": cert,
            "name": inst.get("NAME", ""),
            "type": "Bank",
            "city": inst.get("CITY", ""),
            "state": inst.get("STALP", ""),
            "website": inst.get("WEBADDR", ""),
            "address": inst.get("ADDRESS", ""),
            "zip": inst.get("ZIP", ""),
            "phone": "",
            "ceo": "",
            "assetsM": round(asset_k / 1000, 1),
            "portfolioK": portfolio_k,
            "portfolioM": round(portfolio_k / 1000, 1),
            "crePct": cre_pct,
            "multifamilyK": multifamily,
            "constructionK": construction,
            "nonOccK": non_occ,
            "ownerOccK": owner_occ,
            "dataSource": "FDIC",
            "hmdaDeals": 0,
            "hmdaVolume": 0,
            "hmdaMinLoan": 0,
            "hmdaMaxLoan": 0,
            "hmdaAvgLoan": 0,
        })

    # NCUA credit unions
    for cu in ncua_lenders:
        portfolio_k = sum((cu.get(f, 0) or 0) for f in ncua_fields)
        if portfolio_k <= 0:
            continue

        lenders.append({
            "charter": cu.get("charter"),
            "name": cu.get("name", ""),
            "type": "Credit Union",
            "city": cu.get("city", ""),
            "state": cu.get("state", ""),
            "website": cu.get("website", ""),
            "address": cu.get("address", ""),
            "zip": cu.get("zip", ""),
            "phone": cu.get("phone", ""),
            "ceo": cu.get("ceo", ""),
            "assetsM": cu.get("assetsM", 0),
            "portfolioK": portfolio_k,
            "portfolioM": round(portfolio_k / 1000, 1),
            "crePct": cu.get("crePct", 0),
            "multifamilyK": cu.get("multifamilyK", 0),
            "constructionK": cu.get("constructionK", 0),
            "nonOccK": cu.get("nonOccK", 0),
            "ownerOccK": cu.get("ownerOccK", 0),
            "dataSource": "NCUA",
            "hmdaDeals": 0,
            "hmdaVolume": 0,
            "hmdaMinLoan": 0,
            "hmdaMaxLoan": 0,
            "hmdaAvgLoan": 0,
        })

    return lenders


# ════════════════════════════════════════════════════════════════════════
# Attach HMDA origination data to lenders
# ════════════════════════════════════════════════════════════════════════

def attach_hmda_data(lenders, hmda_data, county_fips):
    if not hmda_data or not hmda_data.get("loans"):
        return

    filers = hmda_data.get("filers", {})
    loans = hmda_data.get("loans", [])

    # Aggregate loans by LEI
    lei_agg = {}
    for loan in loans:
        lei = loan.get("lei", "")
        if not lei:
            continue
        if lei not in lei_agg:
            lei_agg[lei] = {"deals": 0, "volume": 0, "amounts": [], "name": filers.get(lei, lei)}
        try:
            amt = int(float(loan.get("loan_amount", 0) or 0))
        except (ValueError, TypeError):
            amt = 0
        lei_agg[lei]["deals"] += 1
        lei_agg[lei]["volume"] += amt
        lei_agg[lei]["amounts"].append(amt)

    # Build name index for lenders
    lender_by_name = {}
    for i, lender in enumerate(lenders):
        name = lender.get("name", "").upper().strip()
        if name:
            lender_by_name[name] = i

    # Match HMDA LEIs to lenders by name
    matched_leis = set()
    for lei, agg in lei_agg.items():
        hmda_name = agg["name"]
        best_idx = None
        best_sim = 0

        # Try exact match first
        upper_name = hmda_name.upper().strip()
        if upper_name in lender_by_name:
            best_idx = lender_by_name[upper_name]
            best_sim = 1.0
        else:
            # Fuzzy match
            for lname, idx in lender_by_name.items():
                sim = name_similarity(hmda_name, lname)
                if sim > best_sim and sim >= 0.4:
                    best_sim = sim
                    best_idx = idx

        if best_idx is not None:
            lender = lenders[best_idx]
            amounts = agg["amounts"]
            lender["hmdaDeals"] = agg["deals"]
            lender["hmdaVolume"] = agg["volume"]
            lender["hmdaMinLoan"] = min(amounts) if amounts else 0
            lender["hmdaMaxLoan"] = max(amounts) if amounts else 0
            lender["hmdaAvgLoan"] = round(agg["volume"] / agg["deals"]) if agg["deals"] > 0 else 0
            matched_leis.add(lei)

    # Add unmatched HMDA-only lenders
    for lei, agg in lei_agg.items():
        if lei in matched_leis:
            continue
        if agg["deals"] < 2:
            continue

        amounts = agg["amounts"]
        name = agg["name"]
        cu_keywords = ["CREDIT UNION", " FCU", "FEDERAL CREDIT", " CU,"]
        inst_type = "Credit Union" if any(kw in name.upper() for kw in cu_keywords) else "Bank"

        lenders.append({
            "name": name,
            "type": inst_type,
            "city": "",
            "state": "",
            "website": "",
            "address": "",
            "zip": "",
            "phone": "",
            "ceo": "",
            "assetsM": 0,
            "portfolioK": 0,
            "portfolioM": 0,
            "crePct": 0,
            "multifamilyK": 0,
            "constructionK": 0,
            "nonOccK": 0,
            "ownerOccK": 0,
            "dataSource": "HMDA",
            "hmdaDeals": agg["deals"],
            "hmdaVolume": agg["volume"],
            "hmdaMinLoan": min(amounts) if amounts else 0,
            "hmdaMaxLoan": max(amounts) if amounts else 0,
            "hmdaAvgLoan": round(agg["volume"] / agg["deals"]) if agg["deals"] > 0 else 0,
        })


# ════════════════════════════════════════════════════════════════════════
# Match scoring algorithm
# ════════════════════════════════════════════════════════════════════════

def compute_match_scores(lenders, deal):
    has_hmda = any(l.get("hmdaDeals", 0) > 0 for l in lenders)

    # Pre-compute percentile data
    portfolios = sorted(l.get("portfolioK", 0) for l in lenders if l.get("portfolioK", 0) > 0)

    for lender in lenders:
        # Specialty lenders (agency/CMBS/SBA/HUD overlay) don't have call-report data,
        # so we can't score them with the standard formula. Give them a fixed score
        # that lands in the "good" tier since they're curated for the product type.
        if lender.get("dataSource") == "Specialty":
            score = 75
        elif has_hmda:
            score = _score_with_hmda(lender, deal, lenders, portfolios)
        else:
            score = _score_portfolio(lender, deal, portfolios)

        lender["matchScore"] = score
        if score >= 80:
            lender["scoreTier"] = "excellent"
        elif score >= 60:
            lender["scoreTier"] = "good"
        elif score >= 40:
            lender["scoreTier"] = "fair"
        else:
            lender["scoreTier"] = "weak"

    lenders.sort(key=lambda x: x.get("matchScore", 0), reverse=True)
    for i, l in enumerate(lenders, 1):
        l["rank"] = i


def _score_portfolio(lender, deal, sorted_portfolios):
    score = 0.0
    loan_amount = deal.get("loan_amount", 0) or 0

    # Portfolio Size (35%) - percentile rank
    portfolio_k = lender.get("portfolioK", 0) or 0
    if portfolio_k > 0 and sorted_portfolios:
        idx = bisect.bisect_left(sorted_portfolios, portfolio_k)
        percentile = idx / len(sorted_portfolios)
        score += percentile * 35
    elif portfolio_k > 0:
        score += 17

    # CRE Concentration (25%)
    cre_pct = lender.get("crePct", 0) or 0
    if cre_pct >= 25:
        score += 25
    elif cre_pct >= 15:
        score += 22
    elif cre_pct >= 10:
        score += 18
    elif cre_pct >= 5:
        score += 12
    else:
        score += max(2, cre_pct * 2)

    # Asset Size Fit (25%)
    assets_dollars = (lender.get("assetsM", 0) or 0) * 1_000_000
    if assets_dollars > 0 and loan_amount > 0:
        ratio = loan_amount / assets_dollars
        if 0.001 <= ratio <= 0.03:
            score += 25
        elif 0.0005 <= ratio <= 0.05:
            score += 18
        elif ratio < 0.0005:
            score += 8
        else:
            score += 5
    elif assets_dollars > 0:
        score += 13

    # Geographic Presence (15%)
    if lender.get("state") == deal.get("state"):
        score += 15
    else:
        score += 3

    return min(100, round(score))


def _score_with_hmda(lender, deal, all_lenders, sorted_portfolios):
    score = 0.0
    loan_amount = deal.get("loan_amount", 0) or 0

    # Origination Activity (30%)
    deals = lender.get("hmdaDeals", 0) or 0
    max_deals = max((l.get("hmdaDeals", 0) or 0 for l in all_lenders), default=1) or 1
    if deals > 0:
        deal_ratio = min(1.0, deals / max_deals)
        score += (0.3 + 0.7 * deal_ratio) * 30
    else:
        # No HMDA deals but has portfolio → small base score
        portfolio_k = lender.get("portfolioK", 0) or 0
        if portfolio_k > 0 and sorted_portfolios:
            idx = bisect.bisect_left(sorted_portfolios, portfolio_k)
            percentile = idx / len(sorted_portfolios)
            score += percentile * 10

    # Loan Size Fit (25%)
    hmda_min = lender.get("hmdaMinLoan", 0) or 0
    hmda_max = lender.get("hmdaMaxLoan", 0) or 0
    if hmda_min and hmda_max and loan_amount > 0:
        if hmda_min <= loan_amount <= hmda_max:
            score += 25
        elif loan_amount < hmda_min:
            ratio = loan_amount / hmda_min if hmda_min > 0 else 0
            score += max(5, ratio * 20)
        else:
            ratio = hmda_max / loan_amount if loan_amount > 0 else 0
            score += max(5, ratio * 20)
    elif loan_amount == 0:
        score += 15
    elif deals > 0:
        score += 12

    # CRE Concentration (20%)
    cre_pct = lender.get("crePct", 0) or 0
    if cre_pct >= 25:
        score += 20
    elif cre_pct >= 15:
        score += 17
    elif cre_pct >= 10:
        score += 14
    elif cre_pct >= 5:
        score += 10
    else:
        score += max(2, cre_pct * 1.5)

    # Asset Size Fit (15%)
    assets_dollars = (lender.get("assetsM", 0) or 0) * 1_000_000
    if assets_dollars > 0 and loan_amount > 0:
        ratio = loan_amount / assets_dollars
        if 0.001 <= ratio <= 0.03:
            score += 15
        elif 0.0005 <= ratio <= 0.05:
            score += 10
        elif ratio < 0.0005:
            score += 5
        else:
            score += 3
    elif assets_dollars > 0:
        score += 8

    # Geographic Presence (10%)
    if lender.get("state") == deal.get("state"):
        score += 10
    else:
        score += 2

    return min(100, round(score))


# ════════════════════════════════════════════════════════════════════════
# HTTP Handler
# ════════════════════════════════════════════════════════════════════════

class LenderMatchHandler(SimpleHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(STATIC_DIR), **kwargs)

    def end_headers(self):
        if self.path and self.path.endswith('.html'):
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        super().end_headers()

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path

        if path == "/":
            self.path = "/lender_search.html"
            return super().do_GET()

        # API routes
        if path == "/api/geocode":
            return self.handle_geocode(parsed)
        if path == "/api/geocode-suggest":
            return self.handle_geocode_suggest(parsed)
        if path == "/api/deal-search":
            return self.handle_deal_search(parsed)
        if path == "/api/cre-portfolio":
            return self.handle_cre_portfolio(parsed)
        if path == "/api/hmda/filers":
            return self.handle_hmda_filers(parsed)
        if path == "/api/hmda/loans":
            return self.handle_hmda_loans(parsed)
        if path == "/api/fdic/search":
            return self.handle_fdic_search(parsed)
        if path == "/api/fdic/financials":
            return self.handle_fdic_financials(parsed)
        if path == "/api/ncua/search":
            return self.handle_ncua_search(parsed)
        if path.startswith("/api/ncua/details/"):
            return self.handle_ncua_details(parsed, path)
        if path == "/api/branches":
            return self.handle_branches(parsed)

        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        content_len = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(content_len)) if content_len > 0 else {}

        if path == "/api/enrich/officers":
            return self.handle_find_officers(body)
        if path == "/api/enrich/contacts":
            return self.handle_enrich_contacts(body)

        self.send_error_json("Not found", 404)

    def send_json(self, data, status=200):
        body = json.dumps(data).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def send_error_json(self, message, status=500):
        self.send_json({"error": message}, status)

    # ── Geocode Suggest: return multiple Nominatim matches ──
    def handle_geocode_suggest(self, parsed):
        params = parse_qs(parsed.query)
        q = params.get("q", [""])[0].strip()

        if len(q) < 3:
            return self.send_json([])

        try:
            nom_resp = SESSION.get(NOMINATIM_URL, params={
                "q": q, "format": "json", "limit": "5",
                "countrycodes": "us", "addressdetails": "1",
            }, timeout=10)
            nom_data = nom_resp.json()

            results = []
            for item in nom_data:
                addr = item.get("address", {})
                state_name = addr.get("state", "")
                state_abbr = STATE_ABBR.get(state_name.lower(), "")
                city = (addr.get("city", "") or addr.get("town", "")
                        or addr.get("village", "") or addr.get("hamlet", ""))
                county = addr.get("county", "").replace(" County", "")

                # Build a clean short display name
                parts = []
                house = addr.get("house_number", "")
                road = addr.get("road", "")
                if house and road:
                    parts.append(f"{house} {road}")
                elif road:
                    parts.append(road)
                if city:
                    parts.append(city)
                if state_abbr:
                    parts.append(state_abbr)
                display_short = ", ".join(parts) if parts else item.get("display_name", q)

                results.append({
                    "display": display_short,
                    "full": item.get("display_name", ""),
                    "city": city,
                    "state": state_abbr,
                    "county": county,
                    "lat": item.get("lat"),
                    "lon": item.get("lon"),
                })

            self.send_json(results)
        except Exception as e:
            self.send_json([])

    # ── Geocode: lat/lon OR address OR city+state -> county FIPS ──
    def handle_geocode(self, parsed):
        params = parse_qs(parsed.query)
        lat = params.get("lat", [""])[0]
        lon = params.get("lon", [""])[0]
        address = params.get("address", [""])[0]
        city = params.get("city", [""])[0]
        state = params.get("state", [""])[0]

        try:
            # If lat/lon provided, skip Nominatim entirely
            if lat and lon:
                state_abbr = params.get("state", [""])[0]
                nom_city = params.get("city", [""])[0]
            else:
                if not address and not city:
                    return self.send_error_json("lat/lon, address, or city required", 400)

                # Nominatim geocode
                if address:
                    nom_resp = SESSION.get(NOMINATIM_URL, params={
                        "q": address, "format": "json", "limit": "1",
                        "countrycodes": "us", "addressdetails": "1",
                    }, timeout=15)
                else:
                    nom_resp = SESSION.get(NOMINATIM_URL, params={
                        "city": city, "state": state, "country": "US",
                        "format": "json", "limit": "1", "addressdetails": "1",
                    }, timeout=15)

                nom_data = nom_resp.json()
                if not nom_data:
                    return self.send_error_json("Could not geocode location", 404)

                lat = nom_data[0]["lat"]
                lon = nom_data[0]["lon"]
                addr_details = nom_data[0].get("address", {})
                nom_state = addr_details.get("state", "")
                nom_city = (addr_details.get("city", "") or addr_details.get("town", "")
                            or addr_details.get("village", ""))
                state_abbr = STATE_ABBR.get(nom_state.lower(), state or "")

            # FCC -> county FIPS
            fcc_resp = SESSION.get(FCC_URL, params={
                "lat": lat, "lon": lon, "format": "json",
            }, timeout=15)
            fcc_data = fcc_resp.json()
            results = fcc_data.get("results", [])
            if not results:
                return self.send_error_json("No county found for this location", 404)

            county_name = results[0].get("county_name", "").replace(" County", "")
            county_fips = results[0].get("county_fips", "")

            self.send_json({
                "fips": county_fips,
                "name": county_name,
                "state": state_abbr,
                "city": nom_city,
                "lat": lat,
                "lon": lon,
            })
        except Exception as e:
            self.send_error_json(str(e))

    # ── Deal Search: main endpoint ──
    def handle_deal_search(self, parsed):
        params = parse_qs(parsed.query)
        state = params.get("state", [""])[0]
        county_fips = params.get("county", [""])[0]
        county_name = params.get("county_name", [""])[0]
        product_type = params.get("product_type", ["all_cre"])[0]
        loan_amount = int(params.get("loan_amount", ["0"])[0] or 0)
        loan_purpose = params.get("loan_purpose", [""])[0]
        min_assets = int(params.get("min_assets", ["50"])[0] or 50)
        years = params.get("years", ["2022,2023,2024"])[0]

        if not state:
            return self.send_error_json("state required", 400)

        cre_label = CRE_MAP.get(product_type, CRE_MAP["all_cre"])["label"]

        print(f"\n{'='*50}")
        print(f"  Deal Search: {cre_label} in {state}")
        print(f"  Loan: ${loan_amount:,} | Purpose: {loan_purpose} | Min Assets: ${min_assets}M")
        if county_fips:
            print(f"  County: {county_name} (FIPS {county_fips})")
        print(f"{'='*50}")

        try:
            t0 = time.time()

            # Parallel fetch: FDIC + NCUA + HMDA
            with ThreadPoolExecutor(max_workers=3) as executor:
                fdic_future = executor.submit(fetch_fdic_batch, state, min_assets)
                ncua_future = executor.submit(get_ncua_for_state, state, min_assets)
                hmda_future = executor.submit(fetch_hmda_for_deal, county_fips, state, years)

                fdic_data = fdic_future.result()
                ncua_lenders = ncua_future.result()
                hmda_data = hmda_future.result()

            t_fetch = time.time() - t0
            print(f"  Fetch complete in {t_fetch:.1f}s")

            # Build unified lender list (includes specialty overlay for niche types)
            lenders = build_lender_list(fdic_data, ncua_lenders, product_type, state=state)
            print(f"  Built lender list: {len(lenders)} lenders")

            # Attach HMDA data
            if hmda_data:
                attach_hmda_data(lenders, hmda_data, county_fips)
                print(f"  HMDA attached: {sum(1 for l in lenders if l.get('hmdaDeals', 0) > 0)} lenders with deals")

            # Compute match scores
            deal = {
                "state": state,
                "county_fips": county_fips,
                "product_type": product_type,
                "loan_amount": loan_amount,
                "loan_purpose": loan_purpose,
            }
            compute_match_scores(lenders, deal)

            # Remove internal fields
            for l in lenders:
                l.pop("_hmda_key", None)

            # Stats
            stats = {
                "total": len(lenders),
                "excellent": sum(1 for l in lenders if l.get("scoreTier") == "excellent"),
                "good": sum(1 for l in lenders if l.get("scoreTier") == "good"),
                "fair": sum(1 for l in lenders if l.get("scoreTier") == "fair"),
                "weak": sum(1 for l in lenders if l.get("scoreTier") == "weak"),
                "banks": sum(1 for l in lenders if l.get("type") == "Bank"),
                "creditUnions": sum(1 for l in lenders if l.get("type") == "Credit Union"),
            }

            t_total = time.time() - t0
            print(f"  Total: {stats['total']} lenders ({stats['excellent']} excellent, {stats['good']} good) in {t_total:.1f}s")

            self.send_json({
                "deal": {
                    "state": state,
                    "county": county_name,
                    "county_fips": county_fips,
                    "product_type": product_type,
                    "product_label": cre_label,
                    "loan_amount": loan_amount,
                    "loan_purpose": loan_purpose,
                },
                "lenders": lenders,
                "stats": stats,
            })

        except Exception as e:
            print(f"  Deal search error: {e}")
            import traceback
            traceback.print_exc()
            self.send_error_json(str(e))

    # ── CRE Portfolio (batch rewrite) ──
    def handle_cre_portfolio(self, parsed):
        params = parse_qs(parsed.query)
        state = params.get("state", [""])[0]
        property_type = params.get("property_type", ["all_cre"])[0]
        min_assets_m = int(params.get("min_assets", ["50"])[0])

        if not state:
            return self.send_error_json("state required", 400)

        cre_config = CRE_MAP.get(property_type, CRE_MAP["all_cre"])
        print(f"  Portfolio search: {cre_config['label']} in {state} (min ${min_assets_m}M)")

        try:
            fdic_data = fetch_fdic_batch(state, min_assets_m)
            ncua_lenders = get_ncua_for_state(state, min_assets_m)
            lenders = build_lender_list(fdic_data, ncua_lenders, property_type, state=state)

            # Sort by portfolio size
            lenders.sort(key=lambda x: x.get("portfolioK", 0), reverse=True)
            for i, r in enumerate(lenders, 1):
                r["rank"] = i

            print(f"  Returning {len(lenders)} ranked lenders")
            self.send_json(lenders)

        except Exception as e:
            print(f"  Portfolio search error: {e}")
            self.send_error_json(str(e))

    # ── HMDA Filers ──
    def handle_hmda_filers(self, parsed):
        params = parse_qs(parsed.query)
        years = params.get("years", ["2022,2023,2024"])[0]

        filers = {}
        for year in years.split(","):
            try:
                resp = SESSION.get(f"{HMDA_BASE}/filers", params={"years": year.strip()}, timeout=60)
                resp.raise_for_status()
                data = resp.json()
                for inst in data.get("institutions", []):
                    if inst.get("lei") and inst.get("name"):
                        filers[inst["lei"]] = inst["name"]
            except Exception as e:
                print(f"  Warning: filers {year} failed: {e}")

        self.send_json(filers)

    # ── HMDA Loans CSV ──
    def handle_hmda_loans(self, parsed):
        params = parse_qs(parsed.query)
        years = params.get("years", [""])[0]
        county = params.get("county", [""])[0]
        state = params.get("state", [""])[0]
        units = params.get("units", ["5-24,25-49,50-99,100-149,>149"])[0]
        loan_purpose = params.get("loan_purpose", [""])[0]

        hmda_params = {
            "years": years,
            "actions_taken": "1",
            "total_units": units,
        }
        if loan_purpose:
            hmda_params["loan_purposes"] = loan_purpose
        if county:
            hmda_params["counties"] = county
        elif state:
            hmda_params["states"] = state
        else:
            return self.send_error_json("county or state required", 400)

        try:
            url = f"{HMDA_BASE}/csv"
            print(f"  Fetching HMDA loans: {hmda_params}")
            resp = SESSION.get(url, params=hmda_params, timeout=180)
            resp.raise_for_status()

            text = resp.text
            if not text.strip():
                return self.send_json([])

            reader = csv.DictReader(io.StringIO(text))
            loans = []
            for row in reader:
                loans.append({
                    "lei": row.get("lei", ""),
                    "loan_amount": row.get("loan_amount", "0"),
                    "activity_year": row.get("activity_year", ""),
                    "total_units": row.get("total_units", ""),
                    "county_code": row.get("county_code", ""),
                    "state_code": row.get("state_code", ""),
                    "interest_rate": row.get("interest_rate", ""),
                    "property_value": row.get("property_value", ""),
                    "loan_type": row.get("loan_type", ""),
                    "loan_purpose": row.get("loan_purpose", ""),
                    "lien_status": row.get("lien_status", ""),
                })
            print(f"  Returned {len(loans)} loans")
            self.send_json(loans)
        except Exception as e:
            print(f"  HMDA error: {e}")
            self.send_error_json(str(e))

    # ── FDIC Institution Search ──
    def handle_fdic_search(self, parsed):
        params = parse_qs(parsed.query)
        name = params.get("name", [""])[0]

        if not name:
            return self.send_error_json("name required", 400)

        skip_words = {"THE", "OF", "AND", "INC", "LLC", "NA", "NATIONAL", "ASSOCIATION",
                      "BANK", "FSB", "SSB", "COMPANY", "CORP", "CORPORATION", "GROUP"}
        clean = name.replace(",", " ").replace(".", " ").replace("-", " ").strip()
        words = [w for w in clean.split() if w.upper() not in skip_words and len(w) > 1]
        if not words:
            words = clean.split()[:1]

        keyword = words[0] if words else name.split()[0]
        fields = "CERT,NAME,CITY,STALP,ASSET,DEP,EQ,WEBADDR,OFFDOM,SPECGRPN,ADDRESS,ZIP,ACTIVE"

        try:
            resp = SESSION.get(f"{FDIC_BASE}/institutions", params={
                "filters": f"ACTIVE:1 AND NAME:{keyword}*",
                "fields": fields,
                "limit": "10",
                "sort_by": "ASSET",
                "sort_order": "DESC",
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("data", [])

            if not results:
                return self.send_json(None)

            name_upper = name.upper()
            best_match = None
            best_score = 0
            for r in results:
                rdata = r.get("data", {})
                rname = (rdata.get("NAME", "") or "").upper()
                score = sum(1 for w in words if w.upper() in rname)
                if name_upper.split(",")[0].strip() in rname or rname in name_upper:
                    score += 10
                if score > best_score:
                    best_score = score
                    best_match = rdata

            self.send_json(best_match if best_score >= 1 else None)
        except Exception as e:
            self.send_json(None)

    # ── FDIC Financials ──
    def handle_fdic_financials(self, parsed):
        params = parse_qs(parsed.query)
        cert = params.get("cert", [""])[0]

        if not cert:
            return self.send_error_json("cert required", 400)

        try:
            fields = "REPDTE,CERT,ASSET,EQ,LNRE,LNRECONS,LNREMULT,LNRENRES,LNRENROW,LNRENROT"
            resp = SESSION.get(f"{FDIC_BASE}/financials", params={
                "filters": f"CERT:{cert}",
                "fields": fields,
                "sort_by": "REPDTE",
                "sort_order": "DESC",
                "limit": "1",
            }, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("data", [])
            self.send_json(results[0]["data"] if results else None)
        except Exception as e:
            self.send_json(None)

    # ── NCUA Credit Union Search ──
    def handle_ncua_search(self, parsed):
        params = parse_qs(parsed.query)
        name = params.get("name", [""])[0]

        if not name:
            return self.send_error_json("name required", 400)

        payload = {
            "searchText": name,
            "rdSearchType": "cuname",
            "rdSearchRadiusList": None,
            "is_mainOffice": True,
            "is_mdi": False, "is_member": False, "is_drive": False,
            "is_atm": False, "is_shared": False, "is_bilingual": False,
            "is_credit_builder": False, "is_fin_counseling": False,
            "is_homebuyer": False, "is_school": False, "is_low_wire": False,
            "is_no_draft": False, "is_no_tax": False, "is_payday": False,
            "skip": 0, "take": 10,
            "sort_item": "", "sort_direction": "",
        }

        try:
            resp = SESSION.post(NCUA_SEARCH_URL, json=payload, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            results = data.get("list", [])

            if not results:
                return self.send_json(None)

            name_upper = name.upper()
            best = None
            best_score = 0
            for r in results:
                rname = (r.get("creditUnionName") or "").upper()
                name_words = set(name_upper.split())
                r_words = set(rname.split())
                score = len(name_words & r_words)
                if name_upper in rname or rname in name_upper:
                    score += 10
                if score > best_score:
                    best_score = score
                    best = r

            if not best or best_score < 1:
                return self.send_json(None)

            self.send_json({
                "charter": best.get("creditUnionNumber"),
                "name": best.get("creditUnionName", ""),
                "city": best.get("city", ""),
                "state": best.get("state", ""),
                "zip": best.get("zipcode", ""),
                "address": best.get("street", ""),
                "phone": best.get("phone", ""),
                "website": best.get("url", ""),
            })
        except Exception as e:
            self.send_json(None)

    # ── NCUA Credit Union Details ──
    def handle_ncua_details(self, parsed, path):
        parts = path.strip("/").split("/")
        if len(parts) < 4:
            return self.send_error_json("charter number required", 400)

        charter = parts[3]
        try:
            resp = SESSION.get(f"{NCUA_DETAILS_URL}/{charter}", timeout=15)
            resp.raise_for_status()
            data = resp.json()
            if data.get("isError"):
                return self.send_json(None)

            self.send_json({
                "charter": data.get("creditUnionCharter", ""),
                "name": data.get("creditUnionName", ""),
                "assets": data.get("creditUnionAssets", "0"),
                "members": data.get("creditUnionNom", ""),
                "ceo": data.get("creditUnionCeo", ""),
                "peerGroup": data.get("creditUnionPeerGroup", ""),
                "website": data.get("creditUnionWebsite", ""),
                "city": data.get("creditUnionCity", ""),
                "state": data.get("creditUnionState", ""),
                "address": data.get("creditUnionAddress", ""),
                "zip": data.get("creditUnionZip", ""),
                "phone": data.get("creditUnionPhone", ""),
            })
        except Exception as e:
            self.send_json(None)

    # ── Branch Discovery ──
    def handle_branches(self, parsed):
        params = parse_qs(parsed.query)
        lender_type = params.get("type", [""])[0]
        cert = params.get("cert", [""])[0]
        charter = params.get("charter", [""])[0]
        name = params.get("name", [""])[0]
        lat = params.get("lat", [""])[0]
        lon = params.get("lon", [""])[0]

        try:
            if lender_type == "Bank" and cert:
                branches = self._fetch_fdic_branches(cert)
            elif lender_type == "Credit Union" and name:
                branches = self._fetch_ncua_branches(name, charter)
            else:
                return self.send_error_json("type + cert or name required", 400)

            if lat and lon:
                deal_lat, deal_lon = float(lat), float(lon)
                for b in branches:
                    b_lat = b.get("lat")
                    b_lon = b.get("lon")
                    if b_lat and b_lon:
                        b["distanceMi"] = round(_haversine(deal_lat, deal_lon, b_lat, b_lon), 1)
                    else:
                        b["distanceMi"] = 9999
                branches.sort(key=lambda x: x["distanceMi"])

            self.send_json(branches)
        except Exception as e:
            self.send_error_json(str(e))

    def _fetch_fdic_branches(self, cert):
        cache_key = f"fdic_branches:{cert}"
        cached = cache_get(cache_key)
        if cached:
            return cached

        fields = "UNINUM,OFFNAME,MAINOFF,ADDRESS,CITY,STALP,ZIP,STNAME,CBSA_METRO_FLG,LATITUDE,LONGITUDE"
        resp = SESSION.get(f"{FDIC_BASE}/locations", params={
            "filters": f"CERT:{cert}", "fields": fields,
            "limit": "500", "sort_by": "MAINOFF", "sort_order": "DESC",
        }, timeout=30)
        resp.raise_for_status()
        data = resp.json().get("data", [])

        branches = []
        for item in data:
            d = item.get("data", {})
            branches.append({
                "id": d.get("UNINUM", ""),
                "name": d.get("OFFNAME", ""),
                "mainOffice": d.get("MAINOFF", 0) == 1,
                "address": d.get("ADDRESS", ""),
                "city": d.get("CITY", ""),
                "state": d.get("STALP", ""),
                "zip": d.get("ZIP", ""),
                "lat": d.get("LATITUDE"), "lon": d.get("LONGITUDE"),
            })

        cache_set(cache_key, branches)
        return branches

    def _fetch_ncua_branches(self, name, charter):
        cache_key = f"ncua_branches:{charter or name}"
        cached = cache_get(cache_key)
        if cached:
            return cached

        payload = {
            "searchText": name, "rdSearchType": "cuname",
            "rdSearchRadiusList": None,
            "is_mainOffice": False,
            "is_mdi": False, "is_member": False, "is_drive": False,
            "is_atm": False, "is_shared": False, "is_bilingual": False,
            "is_credit_builder": False, "is_fin_counseling": False,
            "is_homebuyer": False, "is_school": False, "is_low_wire": False,
            "is_no_draft": False, "is_no_tax": False, "is_payday": False,
            "skip": 0, "take": 100, "sort_item": "", "sort_direction": "",
        }

        resp = SESSION.post(NCUA_SEARCH_URL, json=payload, timeout=15)
        resp.raise_for_status()
        results = resp.json().get("list", [])

        name_upper = name.upper().strip()
        branches = []
        for r in results:
            rname = (r.get("creditUnionName") or "").upper().strip()
            r_charter = str(r.get("creditUnionNumber", ""))
            if charter and r_charter != str(charter):
                continue
            if not charter and rname != name_upper:
                continue
            branches.append({
                "id": r_charter,
                "name": r.get("creditUnionName", ""),
                "mainOffice": r.get("isMainOffice", False),
                "address": r.get("street", ""),
                "city": r.get("city", ""),
                "state": r.get("state", ""),
                "zip": r.get("zipcode", ""),
                "phone": _fmt_phone(r.get("phone", "")),
                "lat": r.get("latitude"), "lon": r.get("longitude"),
            })

        cache_set(cache_key, branches)
        return branches

    # ── Serper: Find Loan Officers ──
    def handle_find_officers(self, body):
        serper_key = os.environ.get("SERPER_API_KEY", "")
        if not serper_key:
            return self.send_error_json("SERPER_API_KEY not configured")

        lenders = body.get("lenders", [])
        if not lenders:
            return self.send_error_json("lenders list required", 400)

        results = {}
        for lender in lenders:
            name = lender.get("name", "")
            city = lender.get("city", "")
            state = lender.get("state", "")
            website = lender.get("website", "")
            lender_id = lender.get("cert") or lender.get("charter") or name

            officers = []
            queries = [
                f'"{name}" "commercial real estate" loan officer {city} {state}',
                f'site:{website} commercial lending team' if website else
                f'"{name}" commercial lending officer {state}',
            ]

            for q in queries:
                if len(officers) >= 5:
                    break
                try:
                    resp = SESSION.post("https://google.serper.dev/search", json={
                        "q": q, "num": 10, "gl": "us",
                    }, headers={
                        "X-API-KEY": serper_key,
                        "Content-Type": "application/json",
                    }, timeout=15)
                    resp.raise_for_status()
                    found = _extract_officers_from_serp(resp.json(), name)
                    for o in found:
                        if o["name"] not in {x["name"] for x in officers}:
                            officers.append(o)
                except Exception:
                    pass
                time.sleep(0.5)

            domain = ""
            if website:
                domain = website.replace("https://", "").replace("http://", "").replace("www.", "").split("/")[0]
            for o in officers:
                if domain:
                    parts = o["name"].split()
                    if len(parts) >= 2:
                        first, last = parts[0].lower(), parts[-1].lower()
                        o["emailGuesses"] = [
                            f"{first}.{last}@{domain}",
                            f"{first[0]}{last}@{domain}",
                        ]

            results[str(lender_id)] = officers

        self.send_json(results)

    # ── Tracerfy: Skip Trace Contacts ──
    def handle_enrich_contacts(self, body):
        tracerfy_key = os.environ.get("TRACERFY_API_KEY", "")
        if not tracerfy_key:
            return self.send_error_json("TRACERFY_API_KEY not configured")

        officers = body.get("officers", [])
        if not officers:
            return self.send_error_json("officers list required", 400)

        csv_buf = io.StringIO()
        writer = csv.writer(csv_buf)
        writer.writerow([
            "owner_first_name", "owner_last_name",
            "property_address", "property_city", "property_state", "property_zip",
            "address", "city", "state", "zip",
        ])
        for o in officers:
            parts = o.get("name", "").split()
            first = parts[0] if parts else ""
            last = parts[-1] if len(parts) > 1 else ""
            writer.writerow([first, last,
                             o.get("branchAddress", ""), o.get("branchCity", ""),
                             o.get("branchState", ""), o.get("branchZip", ""),
                             o.get("branchAddress", ""), o.get("branchCity", ""),
                             o.get("branchState", ""), o.get("branchZip", "")])

        try:
            files = {"file": ("officers.csv", csv_buf.getvalue(), "text/csv")}
            data = {
                "first_name_column": "owner_first_name",
                "last_name_column": "owner_last_name",
                "address_column": "property_address",
                "city_column": "property_city",
                "state_column": "property_state",
                "zip_column": "property_zip",
                "mail_address_column": "address",
                "mail_city_column": "city",
                "mail_state_column": "state",
                "mail_zip_column": "zip",
            }

            resp = SESSION.post("https://tracerfy.com/v1/api/trace/",
                                files=files, data=data,
                                headers={"Authorization": f"Bearer {tracerfy_key}"},
                                timeout=30)
            resp.raise_for_status()
            upload_result = resp.json()
            job_id = upload_result.get("queue_id") or upload_result.get("job_id") or upload_result.get("id")
            if not job_id:
                return self.send_error_json("No job ID from Tracerfy")

            enriched = None
            for _ in range(36):
                time.sleep(5)
                poll_resp = SESSION.get(f"https://tracerfy.com/v1/api/queue/{job_id}",
                                       headers={"Authorization": f"Bearer {tracerfy_key}"},
                                       timeout=15)
                poll_resp.raise_for_status()
                poll_data = poll_resp.json()

                if isinstance(poll_data, list):
                    enriched = poll_data
                    break
                if isinstance(poll_data, dict):
                    status = (poll_data.get("status") or "").lower()
                    if status in ("complete", "completed", "done") or poll_data.get("completed"):
                        enriched = (poll_data.get("results") or poll_data.get("records")
                                    or poll_data.get("data") or [])
                        break

            if enriched is None:
                return self.send_error_json("Tracerfy job timed out")

            contacts = []
            for i, o in enumerate(officers):
                record = enriched[i] if i < len(enriched) else {}
                phones = record.get("phones") or record.get("phone_numbers") or []
                emails = record.get("emails") or record.get("email_addresses") or []
                if not phones:
                    phones = [record.get(f"phone{j}") for j in range(1, 9) if record.get(f"phone{j}")]
                if not emails:
                    emails = [record.get(f"email{j}") for j in range(1, 6) if record.get(f"email{j}")]
                contacts.append({
                    "name": o.get("name", ""), "title": o.get("title", ""),
                    "lender": o.get("lender", ""), "branch": o.get("branchName", ""),
                    "phones": phones[:3], "emails": emails[:3],
                    "emailGuesses": o.get("emailGuesses", []),
                    "hit": bool(phones or emails),
                })

            self.send_json({
                "contacts": contacts,
                "stats": {"total": len(contacts), "hits": sum(1 for c in contacts if c["hit"])},
                "cost": round(len(officers) * 0.02, 2),
            })
        except Exception as e:
            self.send_error_json(str(e))

    def log_message(self, format, *args):
        if "/api/" in (args[0] if args else ""):
            super().log_message(format, *args)


# ════════════════════════════════════════════════════════════════════════
# Serper name extraction helpers (shared)
# ════════════════════════════════════════════════════════════════════════

HUMAN_NAME_RE = re.compile(
    r'\b([A-Z][a-z]{1,15}(?:\s+[A-Z]\.?)?'
    r'(?:\s+[A-Z][a-z]{1,20}|\s+[A-Z][a-z]{1,15}))\b'
)
TITLE_RE = re.compile(
    r'(?:commercial\s+(?:loan|lending|real\s+estate)|CRE|'
    r'(?:senior\s+)?(?:vice\s+president|VP|SVP|EVP)|'
    r'(?:loan|lending|relationship|branch)\s+(?:officer|manager|director|specialist)|'
    r'NMLS|mortgage\s+(?:loan|lending)\s+(?:officer|originator))',
    re.IGNORECASE
)
SKIP_SERP_NAMES = {
    "READ MORE", "LEARN MORE", "SEE ALL", "CLICK HERE", "CONTACT US",
    "PRIVACY POLICY", "TERMS OF", "COOKIE POLICY", "ALL RIGHTS",
}

def _extract_officers_from_serp(serp_data, bank_name):
    officers = []
    seen_names = set()
    texts = []

    kg = serp_data.get("knowledgeGraph", {})
    if kg:
        for k, v in kg.get("attributes", {}).items():
            texts.append(f"{k}: {v}")
        texts.append(kg.get("description", ""))

    ab = serp_data.get("answerBox", {})
    if ab:
        texts.append(ab.get("answer", ""))
        texts.append(ab.get("snippet", ""))

    for result in serp_data.get("organic", []):
        texts.append(result.get("title", ""))
        texts.append(result.get("snippet", ""))

    full_text = " ".join(texts)
    bank_upper = bank_name.upper()

    for match in HUMAN_NAME_RE.finditer(full_text):
        name = match.group(1).strip()
        name_upper = name.upper()
        if name_upper in seen_names:
            continue
        if any(s in name_upper for s in SKIP_SERP_NAMES):
            continue
        if name_upper in bank_upper:
            continue

        start = max(0, match.start() - 100)
        end = min(len(full_text), match.end() + 100)
        context = full_text[start:end]

        title_match = TITLE_RE.search(context)
        if title_match:
            seen_names.add(name_upper)
            officers.append({
                "name": name,
                "title": title_match.group(0).strip(),
                "source": "serper",
            })

    return officers


def _haversine(lat1, lon1, lat2, lon2):
    R = 3959
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def main():
    # Pre-load NCUA cache at startup
    load_ncua_cache()

    port = int(os.environ.get("PORT", PORT))
    host = "0.0.0.0"
    server = HTTPServer((host, port), LenderMatchHandler)
    print(f"{'='*50}")
    print(f"  CRE Lender Match Server")
    print(f"  Open http://localhost:{port} in your browser")
    print(f"  Press Ctrl+C to stop")
    print(f"{'='*50}")

    if not os.environ.get("PORT"):
        import webbrowser
        webbrowser.open(f"http://localhost:{port}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
        server.server_close()


if __name__ == "__main__":
    main()
