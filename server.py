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
    "all_cre":      {"label": "All CRE",                      "fields": ["LNRECONS", "LNREMULT", "LNRENROT"]},
    "construction": {"label": "Construction & Development",    "fields": ["LNRECONS"]},
    "multifamily":  {"label": "Multifamily (5+ units)",        "fields": ["LNREMULT"]},
    "non_res":      {"label": "Non-Residential Investment",    "fields": ["LNRENROT"]},
    "owner_occ":    {"label": "Owner-Occupied CRE",            "fields": ["LNRENROW"]},
}

NCUA_CRE_MAP = {
    "all_cre":      ["constructionK", "multifamilyK", "nonOccK"],
    "construction": ["constructionK"],
    "multifamily":  ["multifamilyK"],
    "non_res":      ["nonOccK"],
    "owner_occ":    ["ownerOccK"],
}

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
            "website": "",
            "dataSource": "NCUA",
        })
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

def build_lender_list(fdic_data, ncua_lenders, product_type):
    cre_config = CRE_MAP.get(product_type, CRE_MAP["all_cre"])
    fdic_fields = cre_config["fields"]
    ncua_fields = NCUA_CRE_MAP.get(product_type, NCUA_CRE_MAP["all_cre"])

    lenders = []

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
            "address": "",
            "zip": "",
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
    product_type = deal.get("product_type", "all_cre")
    is_multifamily = product_type in ("multifamily",)
    has_hmda = any(l.get("hmdaDeals", 0) > 0 for l in lenders)

    # Pre-compute percentile data
    portfolios = sorted(l.get("portfolioK", 0) for l in lenders if l.get("portfolioK", 0) > 0)

    for lender in lenders:
        if is_multifamily and has_hmda:
            score = _score_multifamily(lender, deal, lenders, portfolios)
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


def _score_multifamily(lender, deal, all_lenders, sorted_portfolios):
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

        return super().do_GET()

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

        is_multifamily = product_type in ("multifamily",)
        cre_label = CRE_MAP.get(product_type, CRE_MAP["all_cre"])["label"]

        print(f"\n{'='*50}")
        print(f"  Deal Search: {cre_label} in {state}")
        print(f"  Loan: ${loan_amount:,} | Purpose: {loan_purpose} | Min Assets: ${min_assets}M")
        if county_fips:
            print(f"  County: {county_name} (FIPS {county_fips})")
        print(f"{'='*50}")

        try:
            t0 = time.time()

            # Parallel fetch: FDIC + NCUA + HMDA (if multifamily)
            with ThreadPoolExecutor(max_workers=3) as executor:
                fdic_future = executor.submit(fetch_fdic_batch, state, min_assets)
                ncua_future = executor.submit(get_ncua_for_state, state, min_assets)
                hmda_future = None
                if is_multifamily:
                    hmda_future = executor.submit(fetch_hmda_for_deal, county_fips, state, years)

                fdic_data = fdic_future.result()
                ncua_lenders = ncua_future.result()
                hmda_data = hmda_future.result() if hmda_future else None

            t_fetch = time.time() - t0
            print(f"  Fetch complete in {t_fetch:.1f}s")

            # Build unified lender list
            lenders = build_lender_list(fdic_data, ncua_lenders, product_type)
            print(f"  Built lender list: {len(lenders)} lenders")

            # Attach HMDA data (multifamily only)
            if is_multifamily and hmda_data:
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
            lenders = build_lender_list(fdic_data, ncua_lenders, property_type)

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

    def log_message(self, format, *args):
        if "/api/" in (args[0] if args else ""):
            super().log_message(format, *args)


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
