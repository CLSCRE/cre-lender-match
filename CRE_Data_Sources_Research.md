# CRE Lending Data Sources Research

**Date:** February 11, 2026
**Purpose:** Find APIs that provide commercial real estate lending/origination data across ALL property types (office, retail, industrial, multifamily) to identify active lenders in specific geographic markets.

**Current Stack:** HMDA (multifamily only) + FDIC BankFind (bank financials)

---

## TIER 1: BEST FIT FOR CLS CRE (Accessible, API available, includes lender/origination data)

---

### 1. ATTOM Data (attomdata.com)

| Attribute | Detail |
|---|---|
| **What they provide** | Property tax, deed, mortgage, foreclosure, environmental risk data for 158M+ U.S. properties across 3,000+ counties. Deed and mortgage records include lender name, loan amount, interest rate, origination date. |
| **CRE property types** | ALL types - residential and commercial. Covers office, retail, industrial, multifamily, land, etc. via county recorder deed/mortgage data. |
| **Lender/origination data?** | YES - mortgage origination records including lender name, loan amount, loan type, interest rate, origination date, document type. This is county recorder data (deeds of trust/mortgages). |
| **API available?** | YES - REST API with JSON responses. Well-documented. Also available on RapidAPI marketplace. SDKs for Python, JavaScript, and other languages. |
| **Pricing** | Starts ~$500/month for basic API access. Also available on RapidAPI. Individual reports from $9.99. Enterprise pricing is custom/negotiated. Self-service signup available (no sales call required to start). |
| **Sign-up process** | Self-service click-through agreement on their developer portal. No sales rep required to start testing with production data. |
| **Verdict** | **TOP PICK.** Best balance of data coverage, API accessibility, and pricing for a small brokerage. Covers ALL CRE property types via deed/mortgage records. Self-service signup is a major plus. |

---

### 2. Reonomy (reonomy.com)

| Attribute | Detail |
|---|---|
| **What they provide** | CRE-focused property intelligence for 54M+ commercial parcels. Includes ownership, mortgages, tenants, contacts, sales history, building details. Data aggregated from 3,000+ county assessors and other sources. |
| **CRE property types** | ALL commercial types - office, retail, industrial, multifamily, hotel, land, special purpose. Purpose-built for CRE. |
| **Lender/origination data?** | YES - mortgage data includes lender name (standardized), principal amount at origination, origination dates, maturity dates, interest rates, current and past lenders, pre-foreclosure status. Searchable by origination/maturity date. |
| **API available?** | YES - REST API (v2). Endpoints for property search, property details (including mortgage data), bulk lookups. Rate limit: 5 requests/second. Credit-based system (1 credit per new property detail lookup). |
| **Pricing** | Web app: ~$400/month per user ($4,800/year). Lower tiers may start at $49-$299/month. API access requires separate enterprise/data agreement (contact sales@reonomy.com). 7-day free trial available for web app. |
| **Sign-up process** | Free trial for web app (no credit card). API access requires contacting sales team. |
| **Verdict** | **EXCELLENT for CRE-specific use case.** Built specifically for commercial real estate. Mortgage/lender data is a core feature. The web app alone could be useful for manual research; API enables automation. Now owned by Altus Group. |

---

### 3. BatchData (batchdata.io)

| Attribute | Detail |
|---|---|
| **What they provide** | 150M+ U.S. property records with ownership, sales, mortgages, liens, tax assessments, property characteristics. Processes newly recorded documents daily (24-48 hours from recording). |
| **CRE property types** | ALL types - residential and commercial. Property data includes land use/zoning codes to filter by property type. |
| **Lender/origination data?** | YES - mortgage transaction data, open liens, deed transfers. Includes lender information on recorded mortgages. |
| **API available?** | YES - REST API with Property Search, Property Lookup, and Address Auto-Complete endpoints. Pay-as-you-go or monthly plans. |
| **Pricing** | Starting at $0.01/API call on pay-as-you-go. Monthly plans from $500/month for 20,000 calls. Annual plans up to ~$10,200/year. Transparent, developer-friendly pricing. |
| **Sign-up process** | Self-service. Developer-friendly onboarding. |
| **Verdict** | **STRONG ALTERNATIVE to ATTOM.** Similar county-recorder-based data with more transparent pricing and a developer-friendly approach. Good for a small shop that wants to pay per call rather than commit to a large contract. |

---

### 4. PropMix PubRec (pubrec.propmix.io)

| Attribute | Detail |
|---|---|
| **What they provide** | Public record data on 151M+ properties across 3,100+ counties. Covers property details, tax, assessments, deed history (20+ years), mortgage liens. Over 100 data points per property (scalable to 300+). |
| **CRE property types** | ALL types - residential and commercial. |
| **Lender/origination data?** | YES - Property Mortgage Data API provides first/second mortgage details including loan amounts, interest rate, lien data, loan term, lender information, and calculated LTV ratios. Property Deed Data API provides ownership transfer history. |
| **API available?** | YES - REST APIs with separate endpoints for Property, Deed, Mortgage, Tax data. JSON responses. |
| **Pricing** | Pay-per-successful-call model. Contact for specific pricing. Appears more affordable than enterprise platforms. |
| **Sign-up process** | Self-service through their website. |
| **Verdict** | **GOOD BUDGET OPTION.** Clean, focused API for public record data. Pay-per-use model is friendly for small brokerages. Worth evaluating alongside ATTOM and BatchData. |

---

### 5. First American Data & Analytics (dna.firstam.com)

| Attribute | Detail |
|---|---|
| **What they provide** | Nationwide property database with 600M+ ownership, sales, assessment, and mortgage records on residential and commercial properties. Document images available. |
| **CRE property types** | ALL types - explicit commercial property coverage with unique datasets, standardized data, and robust quality control. |
| **Lender/origination data?** | YES - mortgage records, deed transfers, liens and judgments. Commercial mortgage origination data included. |
| **API available?** | YES - Digital Gateway developer portal (developer.firstam.io). REST APIs for property data, ownership, liens, and document images. |
| **Pricing** | Enterprise pricing - contact sales. Likely $10K+/year. Primarily serves lenders and large institutions. |
| **Sign-up process** | Developer portal available but likely requires sales engagement for commercial data access. |
| **Verdict** | **HIGH QUALITY but likely expensive.** First American is a title company with direct access to county recorder data. Data quality is excellent but pricing may be enterprise-focused. Worth inquiring if they have SMB plans. |

---

## TIER 2: FREE/LOW-COST GOVERNMENT DATA SOURCES

---

### 6. FFIEC CRA Data (ffiec.gov/data/cra)

| Attribute | Detail |
|---|---|
| **What they provide** | Community Reinvestment Act disclosure data. Shows small business and small farm lending by bank, by geography (census tract level). 731 lenders reported in 2024. |
| **CRE property types** | Small business loans (which includes CRE-secured loans, but not broken out by property type). Not specifically categorized as office/retail/industrial. |
| **Lender/origination data?** | YES - shows which banks originated small business loans (many of which are CRE loans) in which geographies, with loan counts and dollar amounts. |
| **API available?** | NO formal API - downloadable flat files (CSV). Can be imported into Excel or databases. Free bulk downloads available from FFIEC and Federal Reserve. |
| **Pricing** | FREE |
| **Sign-up process** | None - public data, direct download. |
| **Verdict** | **FREE AND USEFUL SUPPLEMENT.** While not CRE-specific, banks that are active small business lenders in a market are often also active CRE lenders. Good proxy data. Can be combined with FDIC call report data. |

---

### 7. FDIC Call Reports / FFIEC CDR (cdr.ffiec.gov)

| Attribute | Detail |
|---|---|
| **What they provide** | Quarterly financial data for every FDIC-insured bank. Includes CRE loan concentrations (construction, owner-occupied CRE, non-owner-occupied CRE), total assets, capital ratios. |
| **CRE property types** | Aggregated by broad category: Construction & Land Development, Owner-Occupied CRE, Non-Owner-Occupied CRE, Multifamily. NOT broken out by office/retail/industrial. |
| **Lender/origination data?** | PARTIAL - shows which banks have CRE loan portfolios and their concentrations. Does NOT show individual loan originations or specific properties. You already use this via FDIC BankFind. |
| **API available?** | YES - FDIC BankFind API (banks.data.fdic.gov). JSON/CSV output. Also bulk CSV downloads from FFIEC CDR. |
| **Pricing** | FREE |
| **Sign-up process** | None - public API, no key required. |
| **Verdict** | **ALREADY IN YOUR STACK.** You already use FDIC BankFind. The FFIEC CDR bulk downloads provide additional granularity on CRE loan categories. Consider adding the construction/land and non-owner-occupied CRE concentration metrics to your existing tool. |

---

### 8. NCUA Credit Union Data (ncua.gov)

| Attribute | Detail |
|---|---|
| **What they provide** | Quarterly 5300 Call Report data for all federally insured credit unions. Includes commercial loan portfolios, membership, assets, and financial performance. |
| **CRE property types** | Commercial loans as an aggregate category. Includes commercial real estate but not broken out by property type. |
| **Lender/origination data?** | PARTIAL - shows which credit unions have commercial loan portfolios and their size. Does NOT show individual originations. |
| **API available?** | NO formal API - downloadable ZIP files (CSV) from NCUA website. Can be queried through their online tool. data.gov also hosts the datasets. |
| **Pricing** | FREE |
| **Sign-up process** | None - public data, direct download. |
| **Verdict** | **FREE AND FILLS A GAP.** Your current tool covers banks via FDIC but not credit unions. NCUA data lets you identify credit unions with active CRE lending programs. Worth adding to your existing tool. |

---

## TIER 3: ENTERPRISE/EXPENSIVE (Likely out of reach for small brokerage, but good to know)

---

### 9. MSCI Real Capital Analytics (msci.com/real-capital-analytics)

| Attribute | Detail |
|---|---|
| **What they provide** | Global CRE transaction database - sales, construction, and financing transactions. Tracks capital flows, pricing trends, and individual deals. Over 200,000 users. Can identify most active lenders/brokers by geography and property type. |
| **CRE property types** | ALL types - office, retail, industrial, multifamily, hotel, development sites. |
| **Lender/origination data?** | YES - financing/lending data on commercial transactions. Can identify active originators by geography and property type. This is exactly what you need. |
| **API available?** | YES - API, cloud (Snowflake), web, and mobile delivery. |
| **Pricing** | Enterprise pricing - likely $15K-50K+/year. Targets institutional investors, lenders, and large brokerages. |
| **Sign-up process** | Enterprise sales process. Demo/trial may be available. |
| **Verdict** | **IDEAL DATA but likely too expensive.** This is the gold standard for CRE lending/transaction data. If budget allows, this would be the single best source. Worth requesting a quote to see if they have SMB pricing. |

---

### 10. CoStar (costar.com)

| Attribute | Detail |
|---|---|
| **What they provide** | The dominant CRE data platform - sales/lease comps, property listings, vacancy rates, tenant details, ownership, market analytics. |
| **CRE property types** | ALL types - comprehensive CRE coverage. |
| **Lender/origination data?** | LIMITED - CoStar focuses more on sales comps, listings, and market analytics than on lending/mortgage data specifically. Some transaction data includes financing details. |
| **API available?** | EFFECTIVELY NO for third parties. CoStar is notoriously protective of its data. API access was briefly forced open by DOJ as a merger condition but is now essentially closed. Their data is consumed through their own web platform. |
| **Pricing** | $3,000-$23,000/year depending on market coverage and features. Average ~$15,000/year. |
| **Sign-up process** | Sales process with custom quoting. Annual contracts. |
| **Verdict** | **NOT A FIT FOR THIS PROJECT.** No API access for integration. Expensive. Best for market research through their web portal, not for building automated lender-finding tools. |

---

### 11. CoreLogic / Cotality (corelogic.com)

| Attribute | Detail |
|---|---|
| **What they provide** | 200+ data sources with AI-powered analytics. Property data, mortgage performance tracking, risk models, valuations. 600M+ records. |
| **CRE property types** | ALL types - residential and commercial. Strong on mortgage/title data. |
| **Lender/origination data?** | YES - mortgage origination data, deed records, title data. CoreLogic is one of the largest title/mortgage data aggregators in the U.S. |
| **API available?** | YES - developer.corelogic.com. Enterprise-grade APIs. |
| **Pricing** | Median ~$12,000/year. Enterprise pricing, custom contracts. Minimum commitments likely. |
| **Sign-up process** | Enterprise sales process. |
| **Verdict** | **EXCELLENT DATA, ENTERPRISE PRICING.** CoreLogic has some of the best mortgage/origination data available, but it is cost-prohibitive for smaller players. If budget grows, this would be a top-tier source. |

---

### 12. Trepp (trepp.com)

| Attribute | Detail |
|---|---|
| **What they provide** | CMBS loan and property data, commercial mortgage performance, operating expenses at property level. Largest catalog of securitized CRE loans. |
| **CRE property types** | ALL types covered by CMBS - office, retail, industrial, multifamily, hotel, self-storage, etc. |
| **Lender/origination data?** | YES for CMBS loans - detailed loan-level data including originating lender, servicer, loan terms. However, only covers SECURITIZED mortgages, not portfolio/bank loans. |
| **API available?** | YES - web service API available. Also desktop application. |
| **Pricing** | Enterprise pricing - likely $20K-50K+/year. Premium platform targeting institutional investors and CMBS professionals. |
| **Sign-up process** | Enterprise sales process. |
| **Verdict** | **TOO NARROW AND EXPENSIVE.** Only covers CMBS loans (a subset of all CRE lending). Most bank/credit union loans are portfolio loans not captured here. Not cost-effective for this use case. |

---

### 13. ICE Mortgage Technology / Black Knight (mortgagetech.ice.com)

| Attribute | Detail |
|---|---|
| **What they provide** | 600M+ ownership, sales, assessment, and mortgage records. Daily-refreshed public records (deed, mortgage, foreclosure, assignment/release). Formerly Black Knight, acquired by ICE in 2023. |
| **CRE property types** | ALL types - residential and commercial with unique commercial datasets. |
| **Lender/origination data?** | YES - commercial mortgage origination records, deed data, lender information. |
| **API available?** | YES - Developer Portal with APIs. Primarily serves lenders, servicers, and fintech companies. |
| **Pricing** | Enterprise pricing - ICE/Black Knight is a major financial data provider. Likely $10K-50K+/year. |
| **Sign-up process** | Enterprise sales process. Developer portal available. |
| **Verdict** | **HIGH QUALITY, ENTERPRISE ONLY.** Excellent commercial mortgage data but almost certainly too expensive and enterprise-focused for a small brokerage. |

---

### 14. Moody's Analytics CRE (moodyscre.com)

| Attribute | Detail |
|---|---|
| **What they provide** | CRE market analytics, property-level data, rent forecasts, valuations. Formerly REIS. Includes Catylist lease-level data. Data Buffet platform with automation options. |
| **CRE property types** | ALL types - comprehensive CRE market coverage. |
| **Lender/origination data?** | LIMITED - focused more on market analytics, rents, and property performance than on lending/origination data. |
| **API available?** | YES - API delivery through Data Buffet platform. Open architecture. |
| **Pricing** | Premium/enterprise pricing. "Higher end" per industry reviews. Likely $15K-40K+/year. |
| **Sign-up process** | Enterprise sales process. |
| **Verdict** | **NOT THE RIGHT FIT.** Strong on market analytics but weak on the specific lender/origination data you need. Expensive. Better suited for investors and appraisers. |

---

### 15. Cherre (cherre.com)

| Attribute | Detail |
|---|---|
| **What they provide** | Real estate data management/integration platform. Connects multiple data sources (including NCREIF, Trepp, and others) through a single GraphQL API. Acts as a data aggregation layer. |
| **CRE property types** | ALL types (depends on connected data sources). |
| **Lender/origination data?** | DEPENDS on which data sources are connected. Platform itself is an integration layer, not a primary data source. |
| **API available?** | YES - GraphQL API. Well-architected for developers. |
| **Pricing** | Enterprise pricing - venture-backed startup targeting institutional real estate. |
| **Sign-up process** | Enterprise sales/demo process. |
| **Verdict** | **NOT A FIT.** This is a data integration platform, not a data source. Designed for enterprises that already subscribe to multiple data feeds and want to unify them. |

---

### 16. NCREIF (ncreif.org)

| Attribute | Detail |
|---|---|
| **What they provide** | Property-level performance benchmarks for institutional real estate. Quarterly returns, cap rates, NOI data. The NPI (NCREIF Property Index) is the standard institutional benchmark. |
| **CRE property types** | ALL types - office, retail, industrial, multifamily, hotel. |
| **Lender/origination data?** | NO - focused on property performance/returns, not lending data. |
| **API available?** | NO - data delivered through spreadsheets and their Query Tool. |
| **Pricing** | Membership/subscription required. Pricing not public. Members must either contribute data or pay for access. |
| **Sign-up process** | Membership application - must be pre-screened. Contributing members must have RE assets under management. |
| **Verdict** | **NOT A FIT.** No lending data, no API, membership-gated. Designed for institutional investors tracking portfolio performance. |

---

### 17. Precisely (developer.precisely.com)

| Attribute | Detail |
|---|---|
| **What they provide** | Location intelligence and property data. Formerly Pitney Bowes Software & Data (acquired by Syncsort in 2019, rebranded as Precisely). Offers geocoding, parcel boundaries, property attributes. |
| **CRE property types** | Property data covers all types but is primarily parcel/location-focused. |
| **Lender/origination data?** | LIMITED - has property data API but focus is on location intelligence (geocoding, boundaries, demographics) rather than mortgage/lending data. |
| **API available?** | YES - developer.precisely.com/apis/property. Well-documented developer portal. |
| **Pricing** | Not publicly listed. Contact sales. |
| **Sign-up process** | Developer portal with documentation. Sales engagement for full access. |
| **Verdict** | **NOT THE RIGHT FIT.** Good for geocoding and parcel data but not a primary source for mortgage/lending origination data. |

---

### 18. LightBox (lightboxre.com)

| Attribute | Detail |
|---|---|
| **What they provide** | CRE data analytics and location intelligence. Acquired Digital Map Products and Real Capital Markets. Offers parcel data, zoning, property characteristics. 10,000+ data sets. |
| **CRE property types** | ALL types - comprehensive CRE data. |
| **Lender/origination data?** | LIMITED through their platform. RCM (Real Capital Markets) side has transaction data but is focused on deal marketing, not lending analytics. |
| **API available?** | YES - Developer portal with APIs for property, zoning, geocoding. Self-service developer portal. |
| **Pricing** | "Flexible licensing from startup to enterprise." Contact for pricing. |
| **Sign-up process** | Developer portal. Sales engagement for full commercial access. |
| **Verdict** | **NICHE FIT.** Good for parcel/zoning data but not a primary source for lender origination data. |

---

## TIER 4: BONUS / ALTERNATIVE SOURCES

---

### 19. CRED iQ (cred-iq.com)

| Attribute | Detail |
|---|---|
| **What they provide** | CMBS and Agency CRE loan data. $2T+ of CRE data. Loan-level detail on securitized and agency mortgages. Delinquency tracking, financial statements, maturing loans. |
| **CRE property types** | ALL types covered by CMBS/Agency programs. |
| **Lender/origination data?** | YES for securitized loans - includes originator, servicer, loan terms, maturity dates. |
| **API available?** | YES - bulk data feed, API, and web platform. CSV/JSON/XML formats. |
| **Pricing** | Described as "materially less expensive" than Trepp. Positioned for small-to-medium teams. Likely $5K-15K/year range. |
| **Sign-up process** | Free trial available. Sales process for full access. |
| **Verdict** | **WORTH EXPLORING for CMBS data.** More affordable than Trepp. If you want to supplement your bank/credit union data with CMBS lending data, this is the budget-friendly option. |

---

### 20. CompStak (compstak.com)

| Attribute | Detail |
|---|---|
| **What they provide** | Crowdsourced CRE lease and sales comp data. 60K+ monthly comps. Market rent algorithms. 40K+ contributing members from brokerages and appraisal firms. |
| **CRE property types** | ALL commercial types. |
| **Lender/origination data?** | NO - focused on lease comps and sales comps, not lending/mortgage data. Has Trepp integration for CMBS data overlay. |
| **API available?** | YES - CompStak API for lease comps, sales data, and market rents. |
| **Pricing** | Exchange (free): contribute comps to access comps. Enterprise: paid subscription. API: enterprise pricing. |
| **Sign-up process** | Exchange is free for qualifying CRE professionals. Enterprise requires sales. |
| **Verdict** | **NOT A FIT for lending data.** Great for lease/sales comps (useful for CRE brokerage in general) but does not provide mortgage origination data. |

---

### 21. County Recorder Open Data Portals

| Attribute | Detail |
|---|---|
| **What they provide** | Direct deed and mortgage recordings at the county level. The raw source that ATTOM, CoreLogic, and others aggregate. |
| **CRE property types** | ALL types - deeds and mortgages are recorded for all property types. |
| **Lender/origination data?** | YES - the most granular source. Each recorded mortgage document shows lender, borrower, property, loan amount, recording date. |
| **API available?** | VARIES BY COUNTY. Some large counties (e.g., Cook County IL, Miami-Dade FL, LA County CA) have open data portals with API access. Most do not. No standardized national API. |
| **Pricing** | Generally FREE for online searches. Bulk downloads may require fees. Per-document fees ($0.50-$5) in some jurisdictions. |
| **Sign-up process** | Varies. Some require registration. Many allow anonymous searching. |
| **Verdict** | **FREE BUT NOT SCALABLE.** Perfect if you only work in 1-3 markets. Impractical for nationwide coverage because every county has different systems, formats, and access methods. This is why aggregators like ATTOM charge money - they standardize across 3,000+ counties. |

---

## RECOMMENDATION SUMMARY

### For CLS CRE Brokerage - Recommended Action Plan:

**Phase 1: Free enhancements (now)**
1. Add **NCUA credit union data** to your existing tool (free, fills credit union gap)
2. Add **FFIEC CRA data** as supplementary signal for active lenders by geography (free)
3. Expand **FDIC BankFind** queries to include CRE concentration sub-categories

**Phase 2: Paid API integration ($500-$800/month)**
4. **ATTOM Data** - Primary recommendation for all-CRE-type coverage via deed/mortgage records
   - OR **BatchData** - If you prefer more transparent pay-per-call pricing ($0.01/call)
   - OR **PropMix PubRec** - If you want the most budget-friendly option

**Phase 3: Premium upgrade (when budget allows)**
5. **Reonomy** - CRE-focused platform with excellent mortgage/lender data ($400+/month)
6. **MSCI Real Capital Analytics** - If you want the gold standard for CRE lending analytics (enterprise pricing)

### Cost Comparison for Phase 2:

| Provider | Est. Monthly Cost | CRE Mortgage Data? | Self-Service Signup? | Notes |
|---|---|---|---|---|
| ATTOM | ~$500/mo | Yes | Yes | Most established, broadest data |
| BatchData | ~$500/mo (20K calls) or $0.01/call | Yes | Yes | Most transparent pricing |
| PropMix PubRec | Pay-per-call (TBD) | Yes | Yes | Potentially cheapest |
| Reonomy | ~$400/mo (web) | Yes (CRE-focused) | Trial yes, API needs sales | Best CRE-specific UX |

---

## KEY INSIGHT

The core data you need (who lent money on what property, when, and how much) lives in **county recorder offices** as public deed/mortgage records. The question is just how you want to access it:

- **Free but manual:** Search individual county recorder websites
- **Free but limited:** HMDA (multifamily only), FDIC/NCUA (portfolio-level, no deal-level)
- **Paid and automated:** ATTOM, BatchData, PropMix, Reonomy, CoreLogic, First American (they all aggregate county recorder data into APIs)
- **Premium institutional:** MSCI RCA, Trepp, CoStar (gold standard but enterprise pricing)

For a small brokerage doing deal-by-deal lender searches, **ATTOM or BatchData at ~$500/month** gives you the best return on investment by letting you query deed/mortgage records across all U.S. counties, all property types, programmatically.
