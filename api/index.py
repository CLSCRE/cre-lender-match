"""
Vercel serverless function: CRE Lender Match API.
Flask app handling all /api/* routes.
"""

import json
import time
import csv
import io
import bisect
import os
import re
import math
import random
import urllib.parse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify

app = Flask(__name__)

# ════════════════════════════════════════════════════════════════════════
# Shared session
# ════════════════════════════════════════════════════════════════════════

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "CRE-Lender-Match/1.0"})

# ════════════════════════════════════════════════════════════════════════
# API endpoints
# ════════════════════════════════════════════════════════════════════════

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
FCC_URL = "https://geo.fcc.gov/api/census/area"
HMDA_BASE = "https://ffiec.cfpb.gov/v2/data-browser-api/view"
FDIC_BASE = "https://api.fdic.gov/banks"
NCUA_SEARCH_URL = "https://mapping.ncua.gov/api/Search/GetSearchLocations"
NCUA_DETAILS_URL = "https://mapping.ncua.gov/api/CreditUnionDetails/GetCreditUnionDetails"

# ════════════════════════════════════════════════════════════════════════
# In-memory cache (1-hour TTL) — persists across warm invocations
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
    # vercel.json includes "ncua_data/**" in the function bundle; specialty_lenders.json
    # lives at the repo root so we resolve one level up from api/.
    path = Path(__file__).resolve().parent.parent / "specialty_lenders.json"
    if not path.exists():
        _specialty_lenders = {}
        return _specialty_lenders
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        _specialty_lenders = {k: v for k, v in data.items() if not k.startswith("_")}
    except Exception:
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

    # In Vercel, the project root is the working directory
    ncua_dir = Path(__file__).resolve().parent.parent / "ncua_data"
    if not ncua_dir.exists():
        _ncua_data = {}
        return _ncua_data

    files = sorted(ncua_dir.glob("cre_data_*.json"), reverse=True)
    if not files:
        _ncua_data = {}
        return _ncua_data

    with open(files[0]) as f:
        _ncua_data = json.load(f)
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
# FDIC batch fetch
# ════════════════════════════════════════════════════════════════════════

def fetch_fdic_batch(state, min_assets_m=50):
    cache_key = f"fdic_batch:{state}:{min_assets_m}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    min_assets_k = min_assets_m * 1000
    inst_fields = "CERT,NAME,CITY,STALP,ASSET,DEP,EQ,WEBADDR,ADDRESS,ZIP,OFFDOM,SPECGRPN"
    fin_fields = "REPDTE,CERT,ASSET,EQ,DEP,LNRE,LNRECONS,LNREMULT,LNRENRES,LNRENROW,LNRENROT,STALP"

    resp = SESSION.get(f"{FDIC_BASE}/financials", params={
        "sort_by": "REPDTE", "sort_order": "DESC", "limit": "1", "fields": "REPDTE",
    }, timeout=15)
    resp.raise_for_status()
    repdte_data = resp.json().get("data", [])
    if not repdte_data:
        return {"institutions": {}, "financials": {}}
    repdte = repdte_data[0]["data"]["REPDTE"]

    resp = SESSION.get(f"{FDIC_BASE}/financials", params={
        "filters": f"STALP:{state} AND REPDTE:{repdte} AND ASSET:[{min_assets_k} TO *]",
        "fields": fin_fields, "limit": "10000",
        "sort_by": "ASSET", "sort_order": "DESC",
    }, timeout=60)
    resp.raise_for_status()
    fin_list = resp.json().get("data", [])
    financials = {}
    for item in fin_list:
        d = item.get("data", {})
        cert = d.get("CERT")
        if cert:
            financials[str(cert)] = d

    resp = SESSION.get(f"{FDIC_BASE}/institutions", params={
        "filters": f"STALP:{state} AND ACTIVE:1 AND ASSET:[{min_assets_k} TO *]",
        "fields": inst_fields, "limit": "10000",
        "sort_by": "ASSET", "sort_order": "DESC",
    }, timeout=30)
    resp.raise_for_status()
    inst_list = resp.json().get("data", [])
    institutions = {}
    for item in inst_list:
        d = item.get("data", {})
        cert = d.get("CERT")
        if cert:
            institutions[str(cert)] = d

    result = {"institutions": institutions, "financials": financials, "repdte": repdte}
    cache_set(cache_key, result)
    return result


# ════════════════════════════════════════════════════════════════════════
# NCUA Details API enrichment
# ════════════════════════════════════════════════════════════════════════

_ncua_details_cache = {}


def _fmt_phone(raw):
    digits = ''.join(c for c in str(raw or '') if c.isdigit())
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return raw or ""


def _fetch_ncua_detail(charter):
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
    to_enrich = [cu for cu in cu_list if not cu.get("phone") and not cu.get("ceo")]
    if not to_enrich:
        return

    charters = [cu["charter"] for cu in to_enrich]
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

    for cu in to_enrich:
        detail = results.get(cu["charter"])
        if detail:
            cu["phone"] = detail.get("phone", "")
            cu["ceo"] = detail.get("ceo", "")
            if not cu.get("website"):
                cu["website"] = detail.get("website", "")


# ════════════════════════════════════════════════════════════════════════
# NCUA state filter
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

    enrich_ncua_details(results)
    return results


# ════════════════════════════════════════════════════════════════════════
# HMDA fetch
# ════════════════════════════════════════════════════════════════════════

def fetch_hmda_for_deal(county_fips, state, years="2022,2023,2024"):
    cache_key = f"hmda_deal:{county_fips or state}:{years}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    filers = {}
    for year in years.split(","):
        try:
            resp = SESSION.get(f"{HMDA_BASE}/filers", params={"years": year.strip()}, timeout=60)
            resp.raise_for_status()
            for inst in resp.json().get("institutions", []):
                if inst.get("lei") and inst.get("name"):
                    filers[inst["lei"]] = inst["name"]
        except Exception:
            pass

    hmda_params = {
        "years": years, "actions_taken": "1",
        "total_units": "5-24,25-49,50-99,100-149,>149",
    }
    if county_fips:
        hmda_params["counties"] = county_fips
    else:
        hmda_params["states"] = state

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

    result = {"filers": filers, "loans": loans}
    cache_set(cache_key, result)
    return result


# ════════════════════════════════════════════════════════════════════════
# Build unified lender list
# ════════════════════════════════════════════════════════════════════════

def build_lender_list(fdic_data, ncua_lenders, product_type, state=""):
    cre_config = CRE_MAP.get(product_type, CRE_MAP["all_cre"])
    fdic_fields = cre_config["fields"]
    ncua_fields = NCUA_CRE_MAP.get(product_type, NCUA_CRE_MAP["all_cre"])

    lenders = []

    # Specialty lender overlay (agency/CMBS/SBA/HUD) for niche product types.
    lenders.extend(build_specialty_entries(product_type, state))

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
            "cert": cert, "name": inst.get("NAME", ""), "type": "Bank",
            "city": inst.get("CITY", ""), "state": inst.get("STALP", ""),
            "website": inst.get("WEBADDR", ""), "address": inst.get("ADDRESS", ""),
            "zip": inst.get("ZIP", ""), "phone": "", "ceo": "",
            "assetsM": round(asset_k / 1000, 1),
            "portfolioK": portfolio_k, "portfolioM": round(portfolio_k / 1000, 1),
            "crePct": cre_pct,
            "multifamilyK": multifamily, "constructionK": construction,
            "nonOccK": non_occ, "ownerOccK": owner_occ,
            "dataSource": "FDIC",
            "hmdaDeals": 0, "hmdaVolume": 0,
            "hmdaMinLoan": 0, "hmdaMaxLoan": 0, "hmdaAvgLoan": 0,
        })

    for cu in ncua_lenders:
        portfolio_k = sum((cu.get(f, 0) or 0) for f in ncua_fields)
        if portfolio_k <= 0:
            continue
        lenders.append({
            "charter": cu.get("charter"), "name": cu.get("name", ""),
            "type": "Credit Union",
            "city": cu.get("city", ""), "state": cu.get("state", ""),
            "website": cu.get("website", ""), "address": cu.get("address", ""),
            "zip": cu.get("zip", ""), "phone": cu.get("phone", ""),
            "ceo": cu.get("ceo", ""),
            "assetsM": cu.get("assetsM", 0),
            "portfolioK": portfolio_k, "portfolioM": round(portfolio_k / 1000, 1),
            "crePct": cu.get("crePct", 0),
            "multifamilyK": cu.get("multifamilyK", 0),
            "constructionK": cu.get("constructionK", 0),
            "nonOccK": cu.get("nonOccK", 0),
            "ownerOccK": cu.get("ownerOccK", 0),
            "dataSource": "NCUA",
            "hmdaDeals": 0, "hmdaVolume": 0,
            "hmdaMinLoan": 0, "hmdaMaxLoan": 0, "hmdaAvgLoan": 0,
        })

    return lenders


# ════════════════════════════════════════════════════════════════════════
# Attach HMDA data
# ════════════════════════════════════════════════════════════════════════

def attach_hmda_data(lenders, hmda_data, county_fips):
    if not hmda_data or not hmda_data.get("loans"):
        return

    filers = hmda_data.get("filers", {})
    loans = hmda_data.get("loans", [])

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

    lender_by_name = {}
    for i, lender in enumerate(lenders):
        name = lender.get("name", "").upper().strip()
        if name:
            lender_by_name[name] = i

    matched_leis = set()
    for lei, agg in lei_agg.items():
        hmda_name = agg["name"]
        best_idx = None
        best_sim = 0

        upper_name = hmda_name.upper().strip()
        if upper_name in lender_by_name:
            best_idx = lender_by_name[upper_name]
            best_sim = 1.0
        else:
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
            "name": name, "type": inst_type,
            "city": "", "state": "", "website": "", "address": "",
            "zip": "", "phone": "", "ceo": "",
            "assetsM": 0, "portfolioK": 0, "portfolioM": 0, "crePct": 0,
            "multifamilyK": 0, "constructionK": 0, "nonOccK": 0, "ownerOccK": 0,
            "dataSource": "HMDA",
            "hmdaDeals": agg["deals"], "hmdaVolume": agg["volume"],
            "hmdaMinLoan": min(amounts) if amounts else 0,
            "hmdaMaxLoan": max(amounts) if amounts else 0,
            "hmdaAvgLoan": round(agg["volume"] / agg["deals"]) if agg["deals"] > 0 else 0,
        })


# ════════════════════════════════════════════════════════════════════════
# Match scoring
# ════════════════════════════════════════════════════════════════════════

def compute_match_scores(lenders, deal):
    has_hmda = any(l.get("hmdaDeals", 0) > 0 for l in lenders)
    portfolios = sorted(l.get("portfolioK", 0) for l in lenders if l.get("portfolioK", 0) > 0)

    for lender in lenders:
        # Specialty overlay entries (agency/CMBS/SBA/HUD) don't have call-report data,
        # so they can't be scored with the standard formula. Fix them at 75 ("good" tier)
        # since they're curated specifically for this product type.
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

    portfolio_k = lender.get("portfolioK", 0) or 0
    if portfolio_k > 0 and sorted_portfolios:
        idx = bisect.bisect_left(sorted_portfolios, portfolio_k)
        percentile = idx / len(sorted_portfolios)
        score += percentile * 35
    elif portfolio_k > 0:
        score += 17

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

    if lender.get("state") == deal.get("state"):
        score += 15
    else:
        score += 3

    return min(100, round(score))


def _score_with_hmda(lender, deal, all_lenders, sorted_portfolios):
    score = 0.0
    loan_amount = deal.get("loan_amount", 0) or 0

    deals = lender.get("hmdaDeals", 0) or 0
    max_deals = max((l.get("hmdaDeals", 0) or 0 for l in all_lenders), default=1) or 1
    if deals > 0:
        deal_ratio = min(1.0, deals / max_deals)
        score += (0.3 + 0.7 * deal_ratio) * 30
    else:
        portfolio_k = lender.get("portfolioK", 0) or 0
        if portfolio_k > 0 and sorted_portfolios:
            idx = bisect.bisect_left(sorted_portfolios, portfolio_k)
            percentile = idx / len(sorted_portfolios)
            score += percentile * 10

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

    if lender.get("state") == deal.get("state"):
        score += 10
    else:
        score += 2

    return min(100, round(score))


# ════════════════════════════════════════════════════════════════════════
# Flask Routes
# ════════════════════════════════════════════════════════════════════════

@app.route("/api/geocode-suggest")
def geocode_suggest():
    q = request.args.get("q", "").strip()
    if len(q) < 3:
        return jsonify([])

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

        return jsonify(results)
    except Exception:
        return jsonify([])


@app.route("/api/geocode")
def geocode():
    lat = request.args.get("lat", "")
    lon = request.args.get("lon", "")
    address = request.args.get("address", "")
    city = request.args.get("city", "")
    state = request.args.get("state", "")

    try:
        if lat and lon:
            state_abbr = state
            nom_city = city
        else:
            if not address and not city:
                return jsonify({"error": "lat/lon, address, or city required"}), 400

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
                return jsonify({"error": "Could not geocode location"}), 404

            lat = nom_data[0]["lat"]
            lon = nom_data[0]["lon"]
            addr_details = nom_data[0].get("address", {})
            nom_state = addr_details.get("state", "")
            nom_city = (addr_details.get("city", "") or addr_details.get("town", "")
                        or addr_details.get("village", ""))
            state_abbr = STATE_ABBR.get(nom_state.lower(), state or "")

        fcc_resp = SESSION.get(FCC_URL, params={
            "lat": lat, "lon": lon, "format": "json",
        }, timeout=15)
        fcc_data = fcc_resp.json()
        results = fcc_data.get("results", [])
        if not results:
            return jsonify({"error": "No county found for this location"}), 404

        county_name = results[0].get("county_name", "").replace(" County", "")
        county_fips = results[0].get("county_fips", "")

        return jsonify({
            "fips": county_fips, "name": county_name,
            "state": state_abbr, "city": nom_city,
            "lat": lat, "lon": lon,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/deal-search")
def deal_search():
    state = request.args.get("state", "")
    county_fips = request.args.get("county", "")
    county_name = request.args.get("county_name", "")
    product_type = request.args.get("product_type", "all_cre")
    loan_amount = int(request.args.get("loan_amount", "0") or 0)
    loan_purpose = request.args.get("loan_purpose", "")
    min_assets = int(request.args.get("min_assets", "50") or 50)
    years = request.args.get("years", "2022,2023,2024")

    if not state:
        return jsonify({"error": "state required"}), 400

    cre_label = CRE_MAP.get(product_type, CRE_MAP["all_cre"])["label"]

    try:
        with ThreadPoolExecutor(max_workers=3) as executor:
            fdic_future = executor.submit(fetch_fdic_batch, state, min_assets)
            ncua_future = executor.submit(get_ncua_for_state, state, min_assets)
            hmda_future = executor.submit(fetch_hmda_for_deal, county_fips, state, years)

            fdic_data = fdic_future.result()
            ncua_lenders = ncua_future.result()
            hmda_data = hmda_future.result()

        lenders = build_lender_list(fdic_data, ncua_lenders, product_type, state=state)

        if hmda_data:
            attach_hmda_data(lenders, hmda_data, county_fips)

        deal = {
            "state": state, "county_fips": county_fips,
            "product_type": product_type,
            "loan_amount": loan_amount, "loan_purpose": loan_purpose,
        }
        compute_match_scores(lenders, deal)

        for l in lenders:
            l.pop("_hmda_key", None)

        stats = {
            "total": len(lenders),
            "excellent": sum(1 for l in lenders if l.get("scoreTier") == "excellent"),
            "good": sum(1 for l in lenders if l.get("scoreTier") == "good"),
            "fair": sum(1 for l in lenders if l.get("scoreTier") == "fair"),
            "weak": sum(1 for l in lenders if l.get("scoreTier") == "weak"),
            "banks": sum(1 for l in lenders if l.get("type") == "Bank"),
            "creditUnions": sum(1 for l in lenders if l.get("type") == "Credit Union"),
        }

        return jsonify({
            "deal": {
                "state": state, "county": county_name,
                "county_fips": county_fips, "product_type": product_type,
                "product_label": cre_label,
                "loan_amount": loan_amount, "loan_purpose": loan_purpose,
            },
            "lenders": lenders,
            "stats": stats,
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/cre-portfolio")
def cre_portfolio():
    state = request.args.get("state", "")
    property_type = request.args.get("property_type", "all_cre")
    min_assets_m = int(request.args.get("min_assets", "50"))

    if not state:
        return jsonify({"error": "state required"}), 400

    try:
        fdic_data = fetch_fdic_batch(state, min_assets_m)
        ncua_lenders = get_ncua_for_state(state, min_assets_m)
        lenders = build_lender_list(fdic_data, ncua_lenders, property_type, state=state)

        lenders.sort(key=lambda x: x.get("portfolioK", 0), reverse=True)
        for i, r in enumerate(lenders, 1):
            r["rank"] = i

        return jsonify(lenders)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/hmda/filers")
def hmda_filers():
    years = request.args.get("years", "2022,2023,2024")
    filers = {}
    for year in years.split(","):
        try:
            resp = SESSION.get(f"{HMDA_BASE}/filers", params={"years": year.strip()}, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            for inst in data.get("institutions", []):
                if inst.get("lei") and inst.get("name"):
                    filers[inst["lei"]] = inst["name"]
        except Exception:
            pass
    return jsonify(filers)


@app.route("/api/hmda/loans")
def hmda_loans():
    years = request.args.get("years", "")
    county = request.args.get("county", "")
    state = request.args.get("state", "")
    units = request.args.get("units", "5-24,25-49,50-99,100-149,>149")
    loan_purpose = request.args.get("loan_purpose", "")

    hmda_params = {
        "years": years, "actions_taken": "1", "total_units": units,
    }
    if loan_purpose:
        hmda_params["loan_purposes"] = loan_purpose
    if county:
        hmda_params["counties"] = county
    elif state:
        hmda_params["states"] = state
    else:
        return jsonify({"error": "county or state required"}), 400

    try:
        resp = SESSION.get(f"{HMDA_BASE}/csv", params=hmda_params, timeout=180)
        resp.raise_for_status()

        text = resp.text
        if not text.strip():
            return jsonify([])

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
        return jsonify(loans)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/fdic/search")
def fdic_search():
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "name required"}), 400

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
            "fields": fields, "limit": "10",
            "sort_by": "ASSET", "sort_order": "DESC",
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("data", [])
        if not results:
            return jsonify(None)

        name_upper = name.upper()
        best_match = None
        best_score = 0
        for r in results:
            rdata = r.get("data", {})
            rname = (rdata.get("NAME", "") or "").upper()
            s = sum(1 for w in words if w.upper() in rname)
            if name_upper.split(",")[0].strip() in rname or rname in name_upper:
                s += 10
            if s > best_score:
                best_score = s
                best_match = rdata

        return jsonify(best_match if best_score >= 1 else None)
    except Exception:
        return jsonify(None)


@app.route("/api/fdic/financials")
def fdic_financials():
    cert = request.args.get("cert", "")
    if not cert:
        return jsonify({"error": "cert required"}), 400

    try:
        fields = "REPDTE,CERT,ASSET,EQ,LNRE,LNRECONS,LNREMULT,LNRENRES,LNRENROW,LNRENROT"
        resp = SESSION.get(f"{FDIC_BASE}/financials", params={
            "filters": f"CERT:{cert}", "fields": fields,
            "sort_by": "REPDTE", "sort_order": "DESC", "limit": "1",
        }, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        results = data.get("data", [])
        return jsonify(results[0]["data"] if results else None)
    except Exception:
        return jsonify(None)


@app.route("/api/ncua/search")
def ncua_search():
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "name required"}), 400

    payload = {
        "searchText": name, "rdSearchType": "cuname",
        "rdSearchRadiusList": None,
        "is_mainOffice": True, "is_mdi": False, "is_member": False,
        "is_drive": False, "is_atm": False, "is_shared": False,
        "is_bilingual": False, "is_credit_builder": False,
        "is_fin_counseling": False, "is_homebuyer": False,
        "is_school": False, "is_low_wire": False,
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
            return jsonify(None)

        name_upper = name.upper()
        best = None
        best_score = 0
        for r in results:
            rname = (r.get("creditUnionName") or "").upper()
            name_words = set(name_upper.split())
            r_words = set(rname.split())
            s = len(name_words & r_words)
            if name_upper in rname or rname in name_upper:
                s += 10
            if s > best_score:
                best_score = s
                best = r

        if not best or best_score < 1:
            return jsonify(None)

        return jsonify({
            "charter": best.get("creditUnionNumber"),
            "name": best.get("creditUnionName", ""),
            "city": best.get("city", ""),
            "state": best.get("state", ""),
            "zip": best.get("zipcode", ""),
            "address": best.get("street", ""),
            "phone": best.get("phone", ""),
            "website": best.get("url", ""),
        })
    except Exception:
        return jsonify(None)


@app.route("/api/ncua/details/<charter>")
def ncua_details(charter):
    try:
        resp = SESSION.get(f"{NCUA_DETAILS_URL}/{charter}", timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if data.get("isError"):
            return jsonify(None)

        return jsonify({
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
    except Exception:
        return jsonify(None)


# ════════════════════════════════════════════════════════════════════════
# Phase 2: Branch Discovery + Contact Enrichment
# ════════════════════════════════════════════════════════════════════════

SERPER_URL = "https://google.serper.dev/search"
TRACERFY_UPLOAD_URL = "https://tracerfy.com/v1/api/trace/"
TRACERFY_POLL_URL = "https://tracerfy.com/v1/api/queue"


def _haversine(lat1, lon1, lat2, lon2):
    """Distance in miles between two lat/lon points."""
    R = 3959
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


@app.route("/api/branches")
def get_branches():
    """Get branch locations for a bank (FDIC) or credit union (NCUA)."""
    lender_type = request.args.get("type", "")
    cert = request.args.get("cert", "")
    charter = request.args.get("charter", "")
    name = request.args.get("name", "")
    lat = request.args.get("lat", "")
    lon = request.args.get("lon", "")

    try:
        if lender_type == "Bank" and cert:
            branches = _fetch_fdic_branches(cert)
        elif lender_type == "Credit Union" and name:
            branches = _fetch_ncua_branches(name, charter)
        else:
            return jsonify({"error": "type + cert (bank) or type + name (CU) required"}), 400

        # Sort by proximity if lat/lon provided
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

        return jsonify(branches)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _fetch_fdic_branches(cert):
    """Fetch all branch offices for a bank from FDIC locations API."""
    cache_key = f"fdic_branches:{cert}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    fields = "UNINUM,OFFNAME,MAINOFF,ADDRESS,CITY,STALP,ZIP,STNAME,CBSA_METRO_FLG,LATITUDE,LONGITUDE"
    resp = SESSION.get(f"{FDIC_BASE}/locations", params={
        "filters": f"CERT:{cert}",
        "fields": fields,
        "limit": "500",
        "sort_by": "MAINOFF",
        "sort_order": "DESC",
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
            "lat": d.get("LATITUDE"),
            "lon": d.get("LONGITUDE"),
        })

    cache_set(cache_key, branches)
    return branches


def _fetch_ncua_branches(name, charter):
    """Fetch all branch locations for a credit union from NCUA Search API."""
    cache_key = f"ncua_branches:{charter or name}"
    cached = cache_get(cache_key)
    if cached:
        return cached

    payload = {
        "searchText": name,
        "rdSearchType": "cuname",
        "rdSearchRadiusList": None,
        "is_mainOffice": False,
        "is_mdi": False, "is_member": False, "is_drive": False,
        "is_atm": False, "is_shared": False, "is_bilingual": False,
        "is_credit_builder": False, "is_fin_counseling": False,
        "is_homebuyer": False, "is_school": False, "is_low_wire": False,
        "is_no_draft": False, "is_no_tax": False, "is_payday": False,
        "skip": 0, "take": 100,
        "sort_item": "", "sort_direction": "",
    }

    resp = SESSION.post(NCUA_SEARCH_URL, json=payload, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    results = data.get("list", [])

    # Filter to matching CU by charter or name
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
            "lat": r.get("latitude"),
            "lon": r.get("longitude"),
        })

    cache_set(cache_key, branches)
    return branches


# ════════════════════════════════════════════════════════════════════════
# Loan Officer + Branch Contact Discovery
# ════════════════════════════════════════════════════════════════════════

PERPLEXITY_URL = "https://api.perplexity.ai/chat/completions"
SERPER_PLACES_URL = "https://google.serper.dev/places"
BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Nav links worth following from a bank homepage (any match in href or link text)
NAV_KEYWORDS_RE = re.compile(
    r'(team|leadership|executive|officer|commercial|lending|'
    r'business|about[-\s]?us|contact|staff|people|directory)',
    re.IGNORECASE
)

HUMAN_NAME_RE = re.compile(
    r'\b([A-Z][a-z]{1,15}(?:\s+[A-Z]\.?)?'
    r'(?:\s+[A-Z][a-z]{1,20}|\s+[A-Z][a-z]{1,15}))\b'
)
TITLE_RE = re.compile(
    r'(?:commercial\s+(?:loan|lending|real\s+estate|banker|banking)|\bCRE\b|'
    r'(?:senior\s+|executive\s+|assistant\s+)?(?:vice\s+president|VP|SVP|EVP)|'
    r'(?:market\s+president|regional\s+president|president)|'
    r'(?:loan|lending|relationship|branch|portfolio)\s+(?:officer|manager|director|specialist|associate)|'
    r'chief\s+(?:lending|credit|commercial)\s+officer|'
    r'NMLS|mortgage\s+(?:loan|lending)\s+(?:officer|originator))',
    re.IGNORECASE
)
# Title lookalikes we never want alone (without a title nearby)
SKIP_NAMES = {
    "READ MORE", "LEARN MORE", "SEE ALL", "CLICK HERE", "CONTACT US",
    "PRIVACY POLICY", "TERMS OF", "COOKIE POLICY", "ALL RIGHTS",
    "EQUAL HOUSING", "MEMBER FDIC", "MEMBER NCUA", "ROUTING NUMBER",
    "OUR TEAM", "OUR STORY", "OUR LEADERSHIP", "QUICK LINKS",
}
AGENT_NAMES = {
    "CSC", "LEGALZOOM", "CT CORPORATION", "HARBOR COMPLIANCE",
    "NATIONAL REGISTERED", "REGISTERED AGENT",
}

EMAIL_RE = re.compile(r'[\w\.\-\+]+@[\w\.\-]+\.[A-Za-z]{2,}')
PHONE_RE = re.compile(r'(?:\+?1[\s\-\.]?)?\(?\b[2-9]\d{2}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}\b')


def _registrable_domain(website_or_url):
    """Extract domain like 'bank.com' from a URL/website string."""
    if not website_or_url:
        return ""
    s = str(website_or_url).strip()
    if "://" not in s:
        s = "http://" + s
    try:
        host = urllib.parse.urlparse(s).netloc.lower()
    except Exception:
        return ""
    if host.startswith("www."):
        host = host[4:]
    # Strip common subdomain prefixes
    for prefix in ("commercial.", "business.", "personal.", "secure.", "online.", "my."):
        if host.startswith(prefix):
            host = host[len(prefix):]
            break
    return host


def _looks_like_person(name):
    """Filter obvious non-names."""
    if not name:
        return False
    u = name.upper()
    if u in SKIP_NAMES or any(s in u for s in AGENT_NAMES):
        return False
    parts = name.split()
    if len(parts) < 2 or len(parts) > 4:
        return False
    if any(len(p) < 2 for p in parts):
        return False
    # Reject if any part is ALL CAPS acronym-like (e.g. "USAA", "FDIC")
    if any(p.isupper() and len(p) > 2 for p in parts):
        return False
    return True


def _title_looks_cre(title):
    return bool(title and TITLE_RE.search(title))


def _dedupe_officers(officers):
    """Dedupe by lowercase name, preserving the most-populated entry."""
    by_name = {}
    for o in officers:
        key = (o.get("name") or "").lower().strip()
        if not key:
            continue
        existing = by_name.get(key)
        if not existing:
            by_name[key] = o
            continue
        # Merge: keep whichever has more fields filled
        score_new = sum(1 for k in ("title", "email", "phone", "linkedinUrl", "sourceUrl") if o.get(k))
        score_old = sum(1 for k in ("title", "email", "phone", "linkedinUrl", "sourceUrl") if existing.get(k))
        if score_new > score_old:
            merged = {**existing, **{k: v for k, v in o.items() if v}}
            by_name[key] = merged
        else:
            for k, v in o.items():
                if v and not existing.get(k):
                    existing[k] = v
    return list(by_name.values())


def _add_email_guesses(officers, domain):
    """Attach emailGuesses list to each officer based on first/last name."""
    if not domain:
        return officers
    for o in officers:
        if o.get("email"):
            continue
        parts = (o.get("name") or "").split()
        if len(parts) < 2:
            continue
        first = re.sub(r'[^a-z]', '', parts[0].lower())
        last = re.sub(r'[^a-z]', '', parts[-1].lower())
        if not first or not last:
            continue
        o["emailGuesses"] = [
            f"{first}.{last}@{domain}",
            f"{first[0]}{last}@{domain}",
            f"{first}{last[0]}@{domain}",
            f"{first}@{domain}",
        ]
    return officers


# ────────────────────────────────────────────────────────────────────────
# Stage 1: Website scrape
# ────────────────────────────────────────────────────────────────────────

def _fetch_html(url, timeout=8):
    """Fetch a URL and return (html_text, final_url) or (None, None)."""
    try:
        resp = SESSION.get(
            url,
            headers={"User-Agent": BROWSER_UA, "Accept": "text/html,*/*"},
            timeout=timeout,
            allow_redirects=True,
        )
        if resp.status_code != 200:
            return None, None
        # Reject huge pages
        ct = resp.headers.get("Content-Type", "").lower()
        if "html" not in ct and "xml" not in ct:
            return None, None
        if len(resp.content) > 500_000:
            return None, None
        return resp.text, resp.url
    except Exception:
        return None, None


def _find_team_links(homepage_html, base_url):
    """Return up to 6 candidate same-domain URLs to crawl for team/leadership pages."""
    soup = BeautifulSoup(homepage_html, "html.parser")
    base_domain = _registrable_domain(base_url)
    candidates = []
    seen = set()

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text() or "").strip()
        if not href or href.startswith("#") or href.startswith("javascript:"):
            continue
        full = urllib.parse.urljoin(base_url, href)
        dom = _registrable_domain(full)
        if dom and dom != base_domain:
            continue
        # Match on href path OR link text
        combo = f"{href} {text}"
        if not NAV_KEYWORDS_RE.search(combo):
            continue
        # Normalize
        full_norm = full.split("#")[0]
        if full_norm in seen:
            continue
        seen.add(full_norm)
        candidates.append(full_norm)
        if len(candidates) >= 6:
            break

    return candidates


def _looks_js_heavy(html_text):
    """Detect JS-rendered SPA shells with little text content."""
    if not html_text:
        return True
    soup = BeautifulSoup(html_text, "html.parser")
    # Remove scripts/styles for accurate text measurement
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text_len = len(soup.get_text(" ", strip=True))
    if text_len < 200:
        return True
    markers = ("__NEXT_DATA__", "window.__INITIAL_STATE__", "data-reactroot", "ng-app=")
    return text_len < 500 and any(m in html_text for m in markers)


def _extract_officers_from_html(html_text, page_url, bank_name):
    """Parse an HTML page for names+titles, mailto and tel hrefs."""
    officers = []
    seen = set()
    soup = BeautifulSoup(html_text, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    # Pull mailto: and tel: links — high confidence
    mailtos = {}
    tels = {}
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = (a.get_text(" ", strip=True) or "").strip()
        if href.lower().startswith("mailto:"):
            addr = href[7:].split("?")[0].strip()
            if "@" in addr and text:
                mailtos[addr.lower()] = text
        elif href.lower().startswith("tel:"):
            phone = re.sub(r'[^\d+]', '', href[4:])
            if len(phone) >= 10 and text:
                tels[phone] = text

    # Walk block-level tags looking for a name + nearby title
    bank_upper = (bank_name or "").upper()
    blocks = soup.find_all(["div", "section", "article", "li", "p", "td"])
    for blk in blocks:
        text = blk.get_text(" ", strip=True)
        if not text or len(text) > 600:
            continue
        if not TITLE_RE.search(text):
            continue
        # Find name candidates
        for m in HUMAN_NAME_RE.finditer(text):
            nm = m.group(1).strip()
            if not _looks_like_person(nm):
                continue
            if nm.upper() in bank_upper:
                continue
            key = nm.lower()
            if key in seen:
                continue
            # Extract title from proximity
            start = max(0, m.start() - 120)
            end = min(len(text), m.end() + 120)
            ctx = text[start:end]
            tm = TITLE_RE.search(ctx)
            if not tm:
                continue
            seen.add(key)
            # Try to bind email/phone by finding them in the same block
            email_match = EMAIL_RE.search(text)
            phone_match = PHONE_RE.search(text)
            officer = {
                "name": nm,
                "title": tm.group(0).strip(),
                "source": "website",
                "sourceUrl": page_url,
            }
            if email_match:
                officer["email"] = email_match.group(0).strip()
            if phone_match:
                officer["phone"] = phone_match.group(0).strip()
            officers.append(officer)
            if len(officers) >= 10:
                return officers

    # Attach mailto emails to any officer whose name appears in the mailto text
    for addr, text in mailtos.items():
        for o in officers:
            if not o.get("email") and o["name"].lower() in text.lower():
                o["email"] = addr
                break

    return officers


def _stage_website(lender, deadline):
    """Stage 1: fetch homepage + team pages and extract officers."""
    website = lender.get("website") or ""
    if not website:
        return [], {"jsHeavy": False, "pagesHit": 0}
    base = website if "://" in website else f"https://{website}"
    bank_name = lender.get("name", "")

    officers = []
    meta = {"jsHeavy": False, "pagesHit": 0}

    if time.monotonic() > deadline:
        return officers, meta

    home_html, home_url = _fetch_html(base, timeout=8)
    if not home_html:
        return officers, meta
    meta["pagesHit"] += 1

    if _looks_js_heavy(home_html):
        meta["jsHeavy"] = True

    home_officers = _extract_officers_from_html(home_html, home_url or base, bank_name)
    officers.extend(home_officers)

    # Crawl nav links if we still need more
    if len(officers) < 5 and not meta["jsHeavy"]:
        candidates = _find_team_links(home_html, home_url or base)
        for url in candidates[:4]:
            if time.monotonic() > deadline:
                break
            if len(officers) >= 5:
                break
            time.sleep(random.uniform(0.3, 0.8))
            html_text, final_url = _fetch_html(url, timeout=7)
            if not html_text:
                continue
            meta["pagesHit"] += 1
            page_officers = _extract_officers_from_html(html_text, final_url or url, bank_name)
            officers.extend(page_officers)

    return _dedupe_officers(officers), meta


# ────────────────────────────────────────────────────────────────────────
# Stage 2: Serper multi-query (LinkedIn + site search + title variants)
# ────────────────────────────────────────────────────────────────────────

def _parse_linkedin_slug(url):
    """Turn https://linkedin.com/in/jane-doe-cre-abc123 into 'Jane Doe'."""
    try:
        path = urllib.parse.urlparse(url).path
    except Exception:
        return ""
    m = re.match(r'/in/([^/?#]+)', path)
    if not m:
        return ""
    slug = m.group(1)
    # Strip trailing hex disambiguator
    slug = re.sub(r'-[0-9a-f]{6,}$', '', slug)
    parts = [p for p in slug.split('-') if p and not p.isdigit()]
    if len(parts) < 2 or len(parts) > 5:
        return ""
    words = [p.capitalize() for p in parts[:3]]
    return " ".join(words)


def _parse_linkedin_title(title_text):
    """Parse 'Jane Doe - SVP Commercial Lending - Acme Bank | LinkedIn'."""
    if not title_text:
        return None, None
    t = title_text.replace(" | LinkedIn", "").replace(" - LinkedIn", "")
    parts = [p.strip() for p in re.split(r'\s[-–—]\s', t)]
    if len(parts) >= 2:
        name = parts[0]
        title = parts[1] if len(parts) >= 2 else ""
        if _looks_like_person(name):
            return name, title
    return None, None


def _serper_search(query, serper_key, timeout=8):
    """Single Serper search, returning parsed JSON or {}."""
    try:
        resp = SESSION.post(
            SERPER_URL,
            json={"q": query, "num": 10, "gl": "us"},
            headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
            timeout=timeout,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return {}


def _extract_officers_from_serp(serp_data, bank_name):
    """Extract officers from Serper SERP: knowledge graph, organic, LinkedIn."""
    officers = []
    seen = set()
    bank_upper = (bank_name or "").upper()

    texts = []
    kg = serp_data.get("knowledgeGraph") or {}
    for k, v in (kg.get("attributes") or {}).items():
        texts.append(f"{k}: {v}")
    if kg.get("description"):
        texts.append(kg.get("description", ""))
    ab = serp_data.get("answerBox") or {}
    if ab:
        texts.append(ab.get("answer", ""))
        texts.append(ab.get("snippet", ""))

    # LinkedIn-aware parsing of organic results
    for r in serp_data.get("organic", []) or []:
        link = r.get("link", "")
        title = r.get("title", "")
        snippet = r.get("snippet", "")
        texts.append(title)
        texts.append(snippet)

        if "linkedin.com/in/" in link.lower():
            name, role = _parse_linkedin_title(title)
            if not name:
                name = _parse_linkedin_slug(link)
                role = title if _title_looks_cre(title) else ""
            if name and _looks_like_person(name) and name.upper() not in bank_upper:
                key = name.lower()
                if key not in seen:
                    # Accept LinkedIn results even without explicit CRE title — the
                    # query already biased toward commercial lending
                    seen.add(key)
                    officers.append({
                        "name": name,
                        "title": role or "Commercial Lending",
                        "source": "serper-linkedin",
                        "linkedinUrl": link,
                        "sourceUrl": link,
                    })

    full_text = " ".join(t for t in texts if t)

    # Find names-near-title in the combined text
    for match in HUMAN_NAME_RE.finditer(full_text):
        name = match.group(1).strip()
        if not _looks_like_person(name):
            continue
        if name.upper() in bank_upper:
            continue
        key = name.lower()
        if key in seen:
            continue

        start = max(0, match.start() - 120)
        end = min(len(full_text), match.end() + 120)
        ctx = full_text[start:end]
        tm = TITLE_RE.search(ctx)
        if not tm:
            continue
        seen.add(key)
        officers.append({
            "name": name,
            "title": tm.group(0).strip(),
            "source": "serper",
        })

    return officers


def _stage_serper(lender, serper_key, deadline):
    """Stage 2: Run 3 Serper queries in parallel and merge results."""
    if not serper_key or time.monotonic() > deadline:
        return []

    name = lender.get("name", "")
    city = lender.get("city", "")
    state = lender.get("state", "")
    domain = _registrable_domain(lender.get("website") or "")

    loc = city if city else state
    queries = [
        f'site:linkedin.com/in "{name}" ({state} OR "{city}") (commercial OR lending OR VP OR "relationship manager")',
        f'site:{domain} (team OR leadership OR lending OR officer)' if domain else None,
        f'"{name}" "{loc}" ("vice president" OR SVP OR "relationship manager" OR "commercial lender" OR "market president" OR "commercial real estate")',
    ]
    queries = [q for q in queries if q]

    results = []
    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = [ex.submit(_serper_search, q, serper_key) for q in queries]
        for f in as_completed(futures, timeout=max(1, deadline - time.monotonic())):
            try:
                serp = f.result()
            except Exception:
                continue
            results.extend(_extract_officers_from_serp(serp, name))

    return _dedupe_officers(results)


# ────────────────────────────────────────────────────────────────────────
# Stage 3: Perplexity Sonar fallback
# ────────────────────────────────────────────────────────────────────────

PERPLEXITY_PROMPT = (
    "You are a research assistant helping a commercial real estate mortgage broker "
    "identify the right lending contact at a bank or credit union.\n\n"
    "Find current commercial real estate (CRE) lending contacts at {bank} "
    "{location_hint}. Include Vice Presidents, Senior VPs, Market Presidents, "
    "Commercial Lenders, Commercial Relationship Managers, Portfolio Managers, "
    "and Commercial Real Estate Officers — any individual at this institution who "
    "would underwrite or originate a commercial real estate loan.\n\n"
    "Return ONLY a valid JSON object with this shape:\n"
    "{{\"officers\": [{{\"name\": str, \"title\": str, \"email\": str, "
    "\"phone\": str, \"linkedin_url\": str, \"source_url\": str}}]}}\n\n"
    "Use empty string for fields you cannot verify. Every entry MUST have a "
    "source_url citation. Do not fabricate. Prefer the bank's own website and "
    "LinkedIn. If you cannot find individuals, return {{\"officers\": []}}."
)


def _allowed_citation(source_url, bank_domain):
    if not source_url:
        return False
    dom = _registrable_domain(source_url)
    if not dom:
        return False
    if bank_domain and (dom == bank_domain or dom.endswith("." + bank_domain)):
        return True
    return dom in {"linkedin.com", "bankingjournal.aba.com", "americanbanker.com"}


def _stage_perplexity(lender, api_key, deadline):
    """Stage 3: ask Perplexity Sonar for CRE lending contacts with citations."""
    if not api_key or time.monotonic() > deadline:
        return []

    name = lender.get("name", "")
    city = lender.get("city", "")
    state = lender.get("state", "")
    loc_parts = [p for p in [city, state] if p]
    loc_hint = f"in {', '.join(loc_parts)}" if loc_parts else ""
    bank_domain = _registrable_domain(lender.get("website") or "")

    user_prompt = PERPLEXITY_PROMPT.format(bank=name, location_hint=loc_hint)

    try:
        resp = SESSION.post(
            PERPLEXITY_URL,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "sonar-pro",
                "messages": [
                    {"role": "system", "content": "You output only valid JSON."},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0,
                "max_tokens": 1200,
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        content = (data.get("choices") or [{}])[0].get("message", {}).get("content", "")
    except Exception:
        return []

    # Strip markdown fences if present
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r'^```[a-z]*\n?', '', content)
        content = re.sub(r'\n?```$', '', content)

    try:
        parsed = json.loads(content)
    except Exception:
        # Try to pull the first JSON object out of the string
        m = re.search(r'\{[\s\S]*\}', content)
        if not m:
            return []
        try:
            parsed = json.loads(m.group(0))
        except Exception:
            return []

    raw_officers = parsed.get("officers") if isinstance(parsed, dict) else parsed
    if not isinstance(raw_officers, list):
        return []

    officers = []
    bank_upper = (name or "").upper()
    for o in raw_officers:
        if not isinstance(o, dict):
            continue
        nm = (o.get("name") or "").strip()
        if not nm or not _looks_like_person(nm) or nm.upper() in bank_upper:
            continue
        src = (o.get("source_url") or "").strip()
        if not _allowed_citation(src, bank_domain):
            continue
        officer = {
            "name": nm,
            "title": (o.get("title") or "").strip() or "Commercial Lending",
            "source": "perplexity",
            "sourceUrl": src,
        }
        email = (o.get("email") or "").strip()
        phone = (o.get("phone") or "").strip()
        linkedin = (o.get("linkedin_url") or "").strip()
        if email and "@" in email:
            officer["email"] = email
        if phone:
            officer["phone"] = phone
        if linkedin and "linkedin.com" in linkedin.lower():
            officer["linkedinUrl"] = linkedin
        officers.append(officer)

    return _dedupe_officers(officers)


# ────────────────────────────────────────────────────────────────────────
# Stage 4: Branch contact fallback
# ────────────────────────────────────────────────────────────────────────

def _lookup_branch_phone_via_places(bank_name, address, city, state, serper_key):
    """Use Serper Places for a verified Google phone for an FDIC branch."""
    if not serper_key:
        return "", ""
    query_parts = [bank_name]
    if address:
        query_parts.append(address)
    if city:
        query_parts.append(city)
    if state:
        query_parts.append(state)
    q = " ".join(p for p in query_parts if p)
    try:
        resp = SESSION.post(
            SERPER_PLACES_URL,
            json={"q": q, "gl": "us"},
            headers={"X-API-KEY": serper_key, "Content-Type": "application/json"},
            timeout=6,
        )
        resp.raise_for_status()
        data = resp.json()
        places = data.get("places", []) or []
        bank_upper = bank_name.upper()
        for p in places:
            title = (p.get("title") or "").upper()
            if bank_name and bank_upper.split()[0] not in title:
                continue
            phone = p.get("phoneNumber") or ""
            website = p.get("website") or ""
            if phone:
                return phone, website
        # Fallback: first result's phone
        if places and places[0].get("phoneNumber"):
            return places[0]["phoneNumber"], places[0].get("website", "")
    except Exception:
        pass
    return "", ""


def _stage_branch_contact(lender, serper_key, deadline):
    """Stage 4: return a {name, phone, address, ...} branch-contact fallback."""
    if time.monotonic() > deadline:
        return None
    branches = lender.get("branches") or []
    if not branches:
        return None

    # Prefer main office, otherwise the nearest / first branch
    main = next((b for b in branches if b.get("mainOffice")), None)
    branch = main or branches[0]

    name = lender.get("name", "")
    addr = branch.get("address") or ""
    city = branch.get("city") or ""
    state = branch.get("state") or ""
    zip_code = branch.get("zip") or ""
    phone = branch.get("phone") or ""
    source_url = ""

    # FDIC branches lack phone — fetch via Places
    if not phone and time.monotonic() < deadline:
        phone, source_url = _lookup_branch_phone_via_places(
            name, addr, city, state, serper_key
        )

    if not (phone or addr):
        return None

    label = "Main Office" if branch.get("mainOffice") else (branch.get("name") or "Branch")
    return {
        "name": f"{name} — {label}",
        "phone": phone,
        "address": addr,
        "city": city,
        "state": state,
        "zip": zip_code,
        "sourceUrl": source_url or (lender.get("website") or ""),
    }


# ────────────────────────────────────────────────────────────────────────
# Orchestrator
# ────────────────────────────────────────────────────────────────────────

def _enrich_one_lender(lender, serper_key, perplexity_key, deadline):
    """Run Stages 1-4 for a single lender. Returns (officers, branch_contact, meta)."""
    t0 = time.monotonic()
    per_lender_deadline = min(deadline, t0 + 22)
    sources_used = []

    # Cache check (reduces repeat calls across warm function invocations)
    lender_id = lender.get("cert") or lender.get("charter") or lender.get("name")
    cache_key = f"officers_v2:{lender_id}"
    cached = cache_get(cache_key)
    if cached:
        return cached["officers"], cached["branchContact"], {
            **cached.get("meta", {}),
            "cached": True,
        }

    # Stage 1 + Stage 2 in parallel
    stage_deadline_12 = min(per_lender_deadline, t0 + 12)
    officers = []
    meta = {"sources": sources_used, "durationMs": 0, "jsHeavy": False}

    with ThreadPoolExecutor(max_workers=2) as ex:
        f_web = ex.submit(_stage_website, lender, stage_deadline_12)
        f_serp = ex.submit(_stage_serper, lender, serper_key, stage_deadline_12)
        try:
            web_officers, web_meta = f_web.result(timeout=max(1, stage_deadline_12 - time.monotonic()))
            if web_officers:
                sources_used.append("website")
            officers.extend(web_officers)
            meta["jsHeavy"] = web_meta.get("jsHeavy", False)
            meta["pagesHit"] = web_meta.get("pagesHit", 0)
        except Exception:
            pass
        try:
            serp_officers = f_serp.result(timeout=max(1, stage_deadline_12 - time.monotonic()))
            if serp_officers:
                sources_used.append("serper")
            officers.extend(serp_officers)
        except Exception:
            pass

    officers = _dedupe_officers(officers)

    # Stage 3: Perplexity fallback if still empty
    if not officers and time.monotonic() < per_lender_deadline and perplexity_key:
        perp_officers = _stage_perplexity(lender, perplexity_key, per_lender_deadline)
        if perp_officers:
            sources_used.append("perplexity")
            officers.extend(perp_officers)
        officers = _dedupe_officers(officers)

    # Attach email guesses
    domain = _registrable_domain(lender.get("website") or "")
    officers = _add_email_guesses(officers, domain)

    # Stage 4: Branch contact if still zero officers
    branch_contact = None
    if not officers:
        branch_contact = _stage_branch_contact(lender, serper_key, per_lender_deadline)
        if branch_contact:
            sources_used.append("branch")

    meta["durationMs"] = int((time.monotonic() - t0) * 1000)
    meta["sources"] = sources_used

    result = {"officers": officers, "branchContact": branch_contact, "meta": meta}
    cache_set(cache_key, result)
    return officers, branch_contact, meta


@app.route("/api/enrich/officers", methods=["POST"])
def find_officers():
    """4-stage officer + branch-contact discovery.

    Response envelope:
      {
        "officers": { lenderId: [ {name, title, email?, emailGuesses?, phone?, linkedinUrl?, sourceUrl?, source}, ... ] },
        "branchContacts": { lenderId: {name, phone, address, city, state, zip, sourceUrl} | null },
        "meta": { lenderId: {sources: [...], durationMs: int, jsHeavy: bool} }
      }
    """
    serper_key = os.environ.get("SERPER_API_KEY", "")
    perplexity_key = os.environ.get("PERPLEXITY_API_KEY", "")
    if not serper_key:
        return jsonify({"error": "SERPER_API_KEY not configured"}), 500

    body = request.get_json() or {}
    lenders = body.get("lenders", [])
    if not lenders:
        return jsonify({"error": "lenders list required"}), 400

    request_start = time.monotonic()
    request_deadline = request_start + 270  # leave headroom under 300s Vercel cap

    officers_map = {}
    branch_map = {}
    meta_map = {}

    def _lender_id(l):
        return str(l.get("cert") or l.get("charter") or l.get("name"))

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {
            ex.submit(_enrich_one_lender, l, serper_key, perplexity_key, request_deadline): l
            for l in lenders
        }
        for f in as_completed(futures):
            lender = futures[f]
            lid = _lender_id(lender)
            try:
                officers, branch, meta = f.result(
                    timeout=max(1, request_deadline - time.monotonic())
                )
            except Exception as e:
                officers_map[lid] = []
                branch_map[lid] = None
                meta_map[lid] = {"error": str(e)[:200]}
                continue
            officers_map[lid] = officers
            branch_map[lid] = branch
            meta_map[lid] = meta

    return jsonify({
        "officers": officers_map,
        "branchContacts": branch_map,
        "meta": meta_map,
    })


# ════════════════════════════════════════════════════════════════════════
# Tracerfy: Skip Trace Contact Enrichment
# ════════════════════════════════════════════════════════════════════════

@app.route("/api/enrich/contacts", methods=["POST"])
def enrich_contacts():
    """Skip-trace loan officers via Tracerfy to find phones and emails."""
    tracerfy_key = os.environ.get("TRACERFY_API_KEY", "")
    if not tracerfy_key:
        return jsonify({"error": "TRACERFY_API_KEY not configured"}), 500

    body = request.get_json() or {}
    officers = body.get("officers", [])
    if not officers:
        return jsonify({"error": "officers list required"}), 400

    # Build CSV for Tracerfy upload
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
        addr = o.get("branchAddress", "")
        city = o.get("branchCity", "")
        state = o.get("branchState", "")
        zipcode = o.get("branchZip", "")
        writer.writerow([first, last, addr, city, state, zipcode, addr, city, state, zipcode])

    csv_content = csv_buf.getvalue()

    try:
        # Upload to Tracerfy
        files = {"file": ("officers.csv", csv_content, "text/csv")}
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

        resp = SESSION.post(TRACERFY_UPLOAD_URL, files=files, data=data,
                            headers={"Authorization": f"Bearer {tracerfy_key}"},
                            timeout=30)
        resp.raise_for_status()
        upload_result = resp.json()

        job_id = upload_result.get("queue_id") or upload_result.get("job_id") or upload_result.get("id")
        if not job_id:
            return jsonify({"error": "No job ID returned from Tracerfy", "raw": upload_result}), 500

        # Poll for results (max 3 minutes)
        enriched = None
        for _ in range(36):
            time.sleep(5)
            poll_resp = SESSION.get(f"{TRACERFY_POLL_URL}/{job_id}",
                                   headers={"Authorization": f"Bearer {tracerfy_key}"},
                                   timeout=15)
            poll_resp.raise_for_status()
            poll_data = poll_resp.json()

            # Handle various response formats
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
            return jsonify({"error": "Tracerfy job timed out"}), 504

        # Map results back to officers
        contacts = []
        for i, o in enumerate(officers):
            record = enriched[i] if i < len(enriched) else {}
            phones = record.get("phones") or record.get("phone_numbers") or []
            emails = record.get("emails") or record.get("email_addresses") or []

            # Also check phone1-8, email1-5 fields
            if not phones:
                phones = [record.get(f"phone{j}") for j in range(1, 9) if record.get(f"phone{j}")]
            if not emails:
                emails = [record.get(f"email{j}") for j in range(1, 6) if record.get(f"email{j}")]

            contacts.append({
                "name": o.get("name", ""),
                "title": o.get("title", ""),
                "lender": o.get("lender", ""),
                "branch": o.get("branchName", ""),
                "phones": phones[:3],
                "emails": emails[:3],
                "emailGuesses": o.get("emailGuesses", []),
                "hit": bool(phones or emails),
            })

        hit_count = sum(1 for c in contacts if c["hit"])
        return jsonify({
            "contacts": contacts,
            "stats": {"total": len(contacts), "hits": hit_count},
            "cost": round(len(officers) * 0.02, 2),
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500
