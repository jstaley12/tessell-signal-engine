"""
collectors/discovery.py  —  Tessell Signal Engine  |  Phase 2
════════════════════════════════════════════════════════════════════
Autonomous territory discovery engine.

ARCHITECTURE: Structured APIs only. No HTML scraping. No guessing slugs.
No robots.txt risk. No JS-rendering required.

SOURCES (in reliability order):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Tier 1 — No API key, no auth, works from Streamlit Cloud (AWS IPs):
  ✅ SEC EDGAR full-text search API
       efts.sec.gov — public, no rate limit, returns entity_name+ticker+state
       Finds public companies that mentioned Oracle/database in 10-K/10-Q
       Company name is a DIRECT field — no extraction needed
       Coverage: all public companies that file with SEC

  ✅ GDELT DOC 2.0 API
       api.gdeltproject.org — research API, no auth, designed for bulk access
       Near real-time news index covering 65+ countries
       Queries: "Oracle database Texas", "CIO appointed Texas", etc.
       Company name extracted from news headlines (NLP patterns)
       Coverage: any company that appears in news

  ✅ State seed list
       Hardcoded known Fortune-tier enterprise anchors per state
       Guaranteed enterprise — used as baseline, never as primary signal
       Always included unless explicitly disabled

Tier 2 — Free API key (add to Streamlit secrets):
  ✅ NewsAPI.org  (NEWSAPI_KEY)
       Free: 100 req/day. Returns structured articles with company context.
       More reliable than GDELT for US business news.
       Sign up: newsapi.org

  ✅ USAJobs.gov  (no key needed)
       Federal job API — guaranteed enterprise (government contractors)
       Returns OrganizationName directly — no extraction
       Good for TX/OK/KS defense/energy contractors

Tier 3 — Paid keys (best quality):
  ✅ SerpAPI  (SERPAPI_KEY, $50/mo)
       Google Jobs structured results — company_name is a direct field
       Most reliable job-based company discovery
       serpapi.com

  ✅ Bing Search API  (BING_API_KEY, $3-7/1000 queries)
       Returns structured web results — good company name extraction
       Works perfectly from AWS (Microsoft's own cloud)
       azure.microsoft.com/cognitive-services/bing/web-search

NOT USED (removed per spec):
  ✗ Greenhouse/Lever slug guessing — removed
  ✗ Careers page HTML scraping — removed
  ✗ Newsroom HTML scraping — removed
  ✗ Google News RSS — blocked on AWS/Streamlit Cloud IPs
  ✗ Indeed RSS — rate-limited and blocked on data-center IPs
════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import re
import time
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set, Tuple
from urllib.parse import quote_plus, urlparse

from loguru import logger

from collectors.fetcher import fetch_json, fetch_html, FetchLog
from collectors.signal import (
    LiveSignal, is_relevant, extract_keywords,
    detect_state, detect_city, enterprise_relevance,
)


# ════════════════════════════════════════════════════════════════════
# DISCOVERY TYPE CLASSIFICATION
# ════════════════════════════════════════════════════════════════════

def classify_discovery_type(discovery_source: str) -> str:
    """
    live_discovered  — found entirely from live structured API calls
    fallback_seed    — only from the static state seed list
    mixed_source     — seed list company that also got live signals
    """
    sources = set(s.strip() for s in discovery_source.split("+") if s.strip())
    has_seed = "state_seed_list" in sources
    has_live = bool(sources - {"state_seed_list"})
    if has_seed and has_live:
        return "mixed_source"
    if has_seed:
        return "fallback_seed"
    return "live_discovered"


def why_discovered(discovery_source: str, signals: list) -> str:
    """Plain-English explanation of why this company was surfaced."""
    sources = set(s.strip() for s in discovery_source.split("+") if s.strip())
    reasons = []
    if "sec_edgar"       in sources: reasons.append("mentioned Oracle/database in SEC 10-K/10-Q filing")
    if "gdelt"           in sources: reasons.append("appeared in news: Oracle/cloud/transformation/leadership")
    if "newsapi"         in sources: reasons.append("featured in business news for DB/cloud/leadership signals")
    if "serpapi_jobs"    in sources: reasons.append("actively hiring Oracle/DBA/SRE roles (Google Jobs)")
    if "bing_search"     in sources: reasons.append("surfaced in enterprise tech search results")
    if "usajobs"         in sources: reasons.append("posting federal contractor roles in target territory")
    if "state_seed_list" in sources: reasons.append("known Fortune-tier enterprise anchor for this state")
    return "; ".join(reasons) if reasons else "discovered from live signals"


def tessell_relevance_reason(company_name: str, industry: str, signals: list) -> str:
    """
    Generate the 'Why Tessell Now' narrative.
    Uses inferred industry complexity + actual signals found.
    This is what a rep sees on the card to decide whether to call.
    """
    ind_lower = (industry or "").lower()
    name_lower = company_name.lower()

    # Industry-based narratives (inferred, no signals needed)
    INDUSTRY_NARRATIVES = {
        "airline":         "Airline ops require 24/7 Oracle uptime for reservations, loyalty, and revenue mgmt — known DB complexity",
        "airlines":        "Airline ops require 24/7 Oracle uptime for reservations, loyalty, and revenue mgmt — known DB complexity",
        "hospital":        "Hospital systems run Oracle/Epic EHR with strict HIPAA DR requirements — high DBA toil environment",
        "health system":   "Health system Oracle/EHR workloads + HIPAA compliance = strong Tessell fit",
        "healthcare":      "Healthcare Oracle footprint + DR/HA requirements — known DB complexity, strong Tessell fit",
        "banking":         "Core banking on Oracle/SQL Server, 24/7 uptime SLA, regulatory compliance — prime Tessell territory",
        "financial":       "Financial services Oracle dependency + audit/compliance pressure — strong DB automation need",
        "insurance":       "Insurance policy/claims systems on Oracle — DBA overhead and non-prod provisioning pain likely",
        "energy":          "Energy sector SAP/Oracle ERP + SCADA integration — multi-region ops and DR requirements",
        "oil":             "Upstream/midstream Oracle ERP — DBA toil and DR needs common in this space",
        "midstream":       "Midstream ops on Oracle ERP — pipeline management DB complexity, DR requirements",
        "telecom":         "Telco billing/BSS systems on Oracle — highest DB complexity, 24/7 uptime, provisioning pain",
        "telecommunications": "Telecom OSS/BSS Oracle stack — known DBA toil, automation needs, strong Tessell fit",
        "manufacturing":   "Manufacturing ERP on Oracle/SAP — non-prod provisioning delays, DBA overhead common",
        "automotive":      "Automotive manufacturer Oracle ERP + supply chain DB complexity — strong fit",
        "logistics":       "Logistics WMS/TMS Oracle stack — DR requirements, multi-region ops",
        "pharmaceutical":  "Pharma Oracle ERP + FDA compliance + DR requirements — strong Tessell fit",
        "pharma":          "Pharma Oracle footprint + GxP validation requirements — DB automation need",
        "defense":         "Defense Oracle ERP + FedRAMP/security compliance — mission-critical DB environment",
        "aerospace":       "Aerospace Oracle ERP + engineering DB complexity — DR and HA requirements",
        "retail":          "Retail Oracle/SAP ERP + seasonal scaling — provisioning and DR needs",
    }

    # Check for industry narrative
    base_narrative = None
    for ind_key, narrative in INDUSTRY_NARRATIVES.items():
        if ind_key in ind_lower:
            base_narrative = narrative
            break

    # Build signal-based additions
    if not signals:
        if base_narrative:
            return base_narrative
        return "Enterprise-qualified — no live signals yet. Run discovery scan for signal details."

    kws: set = set()
    signal_types: list = []
    for s in signals:
        extracted = (s.extracted_keywords if hasattr(s, "extracted_keywords")
                     else s.get("extracted_keywords", []))
        kws.update(k.lower() for k in extracted)
        sig_type = (s.signal_type if hasattr(s, "signal_type")
                    else s.get("signal_type", ""))
        if sig_type:
            signal_types.append(sig_type)

    signal_parts = []

    # Oracle-specific signals
    oracle_kws = {"oracle","oracle dba","oracle rac","oracle exadata","oracle licensing","oracle cost","oracle migration"}
    if hit := kws & oracle_kws:
        signal_parts.append(f"Oracle footprint confirmed ({', '.join(sorted(hit)[:2])})")

    # Hiring surge
    hiring_kws = {"oracle dba","database administrator","dba","database reliability","dbre",
                  "database engineer","platform engineer","sre","site reliability"}
    if hit := kws & hiring_kws:
        signal_parts.append(f"Active DB/SRE hiring ({', '.join(sorted(hit)[:2])})")

    # Cloud/migration
    migration_kws = {"cloud migration","database migration","oracle migration","modernization",
                     "data center exit","erp migration","sap migration"}
    if hit := kws & migration_kws:
        signal_parts.append(f"Migration/modernization in progress ({', '.join(sorted(hit)[:2])})")

    # DR/resilience
    dr_kws = {"disaster recovery","backup","high availability","failover","rpo","rto","downtime"}
    if hit := kws & dr_kws:
        signal_parts.append(f"DR/resilience signals ({', '.join(sorted(hit)[:2])})")

    # M&A / leadership
    timing_kws = {"acquisition","merger","new cio","new cto","appointed","leadership change"}
    if hit := kws & timing_kws:
        signal_parts.append(f"Timing trigger: {', '.join(sorted(hit)[:2])}")

    if signal_parts:
        result = "; ".join(signal_parts)
        if base_narrative:
            result = base_narrative.split("—")[0].strip() + " — " + result
        return result

    if base_narrative:
        return base_narrative

    return "Enterprise-qualified with signals collected — check score evidence for details"


def domain_filter(domain: str) -> str:
    """
    Returns one of:
      'skip'        — market research / SEO content farm — ignore completely
      'wire'        — press wire — extract real company FROM headline
      'media'       — news/investor blog — extract company, low confidence
      'company'     — domain looks like a real company website
    """
    if not domain:
        return 'company'
    d = domain.lower().split('.')[0].replace('-','').replace('_','')
    if any(s in d for s in SKIP_DOMAINS):
        return 'skip'
    if any(s in d for s in WIRE_SERVICE_DOMAINS):
        return 'wire'
    if any(s in d for s in INVESTOR_BLOG_DOMAINS | NEWS_MEDIA_DOMAINS):
        return 'media'
    return 'company'


NOT_COMPANY_WORDS = {
    "the","a","an","in","at","for","on","of","and","or","with","by",
    "texas","oklahoma","kansas","dallas","houston","tulsa","wichita",
    "new","report","says","announces","hires","names","joins","appoints",
    "database","cloud","digital","enterprise","technology","tech","it",
    "company","inc","corp","llc","ltd","group","holdings",
    "oracle","database","migration","transformation","modernization",
}

COMPANY_SUFFIXES = re.compile(
    r'\s*,?\s*(?:Inc\.?|Corp\.?|Corporation|LLC\.?|Ltd\.?|Limited|'
    r'Co\.?|Group|Holdings?|Technologies?|Technology|Systems?|'
    r'Solutions?|Services?|Enterprises?|Industries?|International|'
    r'Global|National|Partners?|Capital|Financial|Energy|Airlines?|'
    r'Healthcare|Health|Medical|Pharma|Defense|Aerospace)\.?\s*$',
    re.IGNORECASE
)


def extract_companies_from_text(text: str) -> List[str]:
    """
    Extract likely enterprise company names from a news headline or article snippet.
    Uses conservative patterns to minimize false positives.
    """
    candidates = []
    text = text.strip()

    # Pattern 1: "CompanyName [verb] [news action]"
    action_verbs = (
        r'(?:announced?|said|named?|hired?|appoints?|acquires?|launches?|'
        r'expands?|reports?|beats?|misses?|partners?|selects?|signs?|'
        r'completes?|closes?|raises?|invests?|upgrades?|migrates?|transforms?)'
    )
    p1 = re.findall(
        rf'([A-Z][A-Za-z&\-\.\']+(?:\s+[A-Z][A-Za-z&\-\.\']+){{0,3}})\s+{action_verbs}',
        text
    )
    candidates.extend(p1)

    # Pattern 2: "[verb] at/for CompanyName"
    p2 = re.findall(
        r'(?:at|for|with|join(?:ing|ed)?|from)\s+([A-Z][A-Za-z&\-\.\']+(?:\s+[A-Z][A-Za-z&\-\.\']+){0,3})',
        text
    )
    candidates.extend(p2)

    # Pattern 3: "CompanyName's" possessive
    p3 = re.findall(r"([A-Z][A-Za-z&\-\.\']+(?:\s+[A-Z][A-Za-z&\-\.\']+){0,2})'s\s", text)
    candidates.extend(p3)

    # Clean and filter
    result = []
    for name in candidates:
        name = name.strip().rstrip('.,;:')
        # Remove trailing suffix words
        name = COMPANY_SUFFIXES.sub('', name).strip()
        if len(name) < 3 or len(name) > 50:
            continue
        name_lower = name.lower()
        first_word = name_lower.split()[0] if name_lower.split() else ""
        if first_word in NOT_COMPANY_WORDS or name_lower in TECH_VENDORS:
            continue
        # Must start with capital letter
        if not name[0].isupper():
            continue
        result.append(name)

    return list(dict.fromkeys(result))  # dedup preserving order


def clean_company_name(name: str) -> str:
    """Normalize for dedup matching."""
    cleaned = COMPANY_SUFFIXES.sub('', name).strip()
    return ' '.join(cleaned.split()).lower()


# ════════════════════════════════════════════════════════════════════
# DISCOVERED COMPANY MODEL
# ════════════════════════════════════════════════════════════════════

@dataclass
class DiscoveredCompany:
    name:             str
    domain:           Optional[str]    = None
    hq_state:         Optional[str]    = None
    hq_city:          Optional[str]    = None
    industry:         Optional[str]    = None
    estimated_employees: Optional[int] = None
    is_public:        bool             = False
    ticker:           Optional[str]    = None
    fortune_rank:     Optional[int]    = None
    discovery_source: str              = ""
    signals:          List[LiveSignal] = field(default_factory=list)
    confidence:       float            = 0.5
    discovery_date:   str              = field(
        default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d")
    )

    def to_dict(self) -> dict:
        return {
            "name":               self.name,
            "domain":             self.domain,
            "hq_state":           self.hq_state,
            "hq_city":            self.hq_city,
            "industry":           self.industry,
            "estimated_employees":self.estimated_employees,
            "is_public":          self.is_public,
            "ticker":             self.ticker,
            "fortune_rank":       self.fortune_rank,
            "discovery_source":   self.discovery_source,
            "signal_count":       len(self.signals),
            "confidence":         self.confidence,
            "discovery_date":     self.discovery_date,
        }


# ════════════════════════════════════════════════════════════════════
# SOURCE 1: SEC EDGAR  ✅ No auth, government API, AWS-friendly
# ════════════════════════════════════════════════════════════════════

# Oracle/DB-related terms to search in 10-K/10-Q filings
EDGAR_SEARCH_TERMS = [
    '"Oracle database"',
    '"Oracle licensing"',
    '"database infrastructure"',
    '"database modernization"',
    '"Oracle ERP"',
]

# State code to SEC state filter mapping
EDGAR_STATE_CODES: Dict[str, str] = {
    "TX":"TX","OK":"OK","KS":"KS","AR":"AR","MO":"MO",
    "CO":"CO","NM":"NM","AZ":"AZ","LA":"LA","TN":"TN",
    "IN":"IN","MN":"MN","GA":"GA","FL":"FL","NC":"NC",
    "VA":"VA","IL":"IL","OH":"OH","MI":"MI","PA":"PA",
    "NY":"NY","CA":"CA","WA":"WA","MA":"MA","TX":"TX",
}


def discover_from_edgar(state: str, max_companies: int = 25) -> Tuple[List[DiscoveredCompany], dict]:
    """
    ✅ RELIABLE — SEC EDGAR full-text search.
    Endpoint: https://efts.sec.gov/LATEST/search-index
    No auth, no rate limit (use reasonably), AWS-friendly.

    Returns companies that mentioned Oracle/database keywords in recent
    10-K or 10-Q filings. entity_name field is direct — no extraction.
    Only covers public companies but all are Fortune-tier enterprises.
    """
    meta = {
        "source":      "sec_edgar",
        "queries_run": 0,
        "total_hits":  0,
        "accepted":    0,
        "blocked":     False,
    }
    discovered: Dict[str, DiscoveredCompany] = {}

    # Look back 18 months for filings
    cutoff = (datetime.utcnow() - timedelta(days=548)).strftime("%Y-%m-%d")

    for term in EDGAR_SEARCH_TERMS[:3]:  # 3 terms to stay well under rate limits
        url = (f"https://efts.sec.gov/LATEST/search-index"
               f"?q={quote_plus(term)}"
               f"&forms=10-K,10-Q"
               f"&dateRange=custom&startdt={cutoff}")

        data = fetch_json(url, source_name="sec_edgar")
        meta["queries_run"] += 1

        if not data:
            last = FetchLog.entries[-1] if FetchLog.entries else None
            if last and "403" in last.status:
                meta["blocked"] = True
            continue

        hits = (data.get("hits") or {}).get("hits", [])
        meta["total_hits"] += len(hits)
        logger.info(f"[EDGAR] '{term}': {len(hits)} hits")

        for hit in hits[:15]:
            src         = hit.get("_source", {})
            entity_name = (src.get("entity_name") or
                           (src.get("display_names") or [""])[0] or "").strip()
            ticker      = src.get("ticker", "")
            inc_states  = src.get("inc_states") or src.get("location") or ""
            form_type   = src.get("form_type", "10-K")
            file_date   = src.get("file_date", "")
            period      = src.get("period_of_report", "")

            if not entity_name or len(entity_name) < 3:
                continue

            # State filter — keep if incorporated/located in target state
            # Also keep if state unknown (EDGAR state data is inconsistent)
            # inc_states can be a list or string depending on EDGAR response
            if isinstance(inc_states, list):
                inc_states = " ".join(inc_states)
            filing_state = detect_state(str(inc_states)) if inc_states else None
            if filing_state and filing_state != state:
                continue  # Skip companies clearly HQ'd elsewhere

            term_clean = term.strip('"')
            kws = extract_keywords(f"{entity_name} {term_clean} oracle database")

            sig = LiveSignal(
                company_name=entity_name,
                source_url=(f"https://efts.sec.gov/LATEST/search-index?"
                            f"q={quote_plus(term)}&entity={quote_plus(entity_name)}"),
                source_type="sec_edgar",
                date_found=file_date or datetime.utcnow().strftime("%Y-%m-%d"),
                signal_type="transformation",
                raw_snippet=f"SEC {form_type} ({period}): mentions '{term_clean}' — {entity_name}",
                extracted_keywords=kws or ["oracle", "database"],
                confidence_score=0.82,
                state_detected=filing_state or state,
                enterprise_relevance_score=0.72,
                live_collected=True,
                source_access_status="success",
                parser_used="sec_efts_json",
                extraction_method="structured_api",
                data_source="LIVE",
            )

            key = clean_company_name(entity_name)
            if not key:
                continue

            if key not in discovered:
                discovered[key] = DiscoveredCompany(
                    name=entity_name,
                    ticker=ticker or None,
                    hq_state=filing_state or state,
                    is_public=True,
                    discovery_source="sec_edgar",
                    confidence=0.82,
                )
            discovered[key].signals.append(sig)
            if ticker:
                discovered[key].ticker = ticker

        if len(discovered) >= max_companies:
            break

        time.sleep(0.5)  # respectful rate limiting

    meta["accepted"] = len(discovered)
    logger.info(f"[EDGAR] {state}: {len(discovered)} companies from {meta['queries_run']} queries")
    return list(discovered.values())[:max_companies], meta


# ════════════════════════════════════════════════════════════════════
# SOURCE 2: GDELT DOC 2.0  ✅ No auth, research API, AWS-friendly
# ════════════════════════════════════════════════════════════════════

# State name expansions for GDELT queries
STATE_NAMES = {
    "TX":"Texas","OK":"Oklahoma","KS":"Kansas","AR":"Arkansas",
    "MO":"Missouri","CO":"Colorado","NM":"New Mexico","AZ":"Arizona",
    "LA":"Louisiana","TN":"Tennessee","IN":"Indiana","MN":"Minnesota",
    "GA":"Georgia","FL":"Florida","NC":"North Carolina","VA":"Virginia",
    "IL":"Illinois","OH":"Ohio","MI":"Michigan","PA":"Pennsylvania",
}

GDELT_QUERY_TEMPLATES = [
    '"{db_term}" "{state_name}" enterprise',
    '"new CIO" OR "new CTO" "{state_name}"',
    '"Oracle" "{state_name}" "database migration" OR "cloud migration"',
    '"database administrator" "{state_name}" hiring',
    '"digital transformation" "{state_name}" enterprise',
    '"ERP migration" OR "SAP migration" "{state_name}"',
]

DB_TERMS = ["Oracle database", "SQL Server database", "database modernization",
            "database migration", "Oracle DBA"]


def discover_from_gdelt(state: str, max_companies: int = 30) -> Tuple[List[DiscoveredCompany], dict]:
    """
    ✅ RELIABLE — GDELT DOC 2.0 API.
    Endpoint: https://api.gdeltproject.org/api/v2/doc/doc
    No auth, no API key, designed for research/programmatic access.
    Works from AWS IPs (Streamlit Cloud).

    Returns news articles; extracts company names from headlines using NLP.
    Covers enterprise news: leadership changes, transformation, Oracle/DB mentions.
    """
    meta = {
        "source":           "gdelt",
        "queries_run":      0,
        "raw_articles":     0,
        "relevant_articles":0,
        "companies_found":  0,
        "blocked":          False,
    }

    state_name = STATE_NAMES.get(state, state)
    discovered: Dict[str, DiscoveredCompany] = {}

    # Build targeted queries
    queries = []
    for db_term in DB_TERMS[:2]:
        queries.append(f'"{db_term}" "{state_name}"')
    for tmpl in GDELT_QUERY_TEMPLATES[:3]:
        queries.append(tmpl.format(state_name=state_name, db_term=DB_TERMS[0]))

    # GDELT date filter — last 90 days
    end_dt   = datetime.utcnow()
    start_dt = end_dt - timedelta(days=90)
    startdt  = start_dt.strftime("%Y%m%d%H%M%S")
    enddt    = end_dt.strftime("%Y%m%d%H%M%S")

    for query in queries[:4]:
        url = (f"https://api.gdeltproject.org/api/v2/doc/doc"
               f"?query={quote_plus(query)}"
               f"&mode=ArtList"
               f"&maxrecords=25"
               f"&startdatetime={startdt}"
               f"&enddatetime={enddt}"
               f"&sourcelang=english"
               f"&sourcecountry=US"
               f"&format=json")

        data = fetch_json(url, source_name="gdelt")
        meta["queries_run"] += 1

        if not data:
            last = FetchLog.entries[-1] if FetchLog.entries else None
            if last and "403" in last.status:
                meta["blocked"] = True
                logger.warning(f"[GDELT] Blocked (403) — likely sandbox network allowlist")
            else:
                logger.info(f"[GDELT] No response for query: {query[:60]}")
            continue

        articles = data.get("articles", [])
        meta["raw_articles"] += len(articles)
        logger.info(f"[GDELT] '{query[:50]}': {len(articles)} articles")

        for article in articles:
            title      = article.get("title", "")
            url_a      = article.get("url", "")
            seendate   = article.get("seendate", "")
            domain     = article.get("domain", "")

            if not title:
                continue

            # Filter by domain type
            dom_type = domain_filter(domain)
            if dom_type == 'skip':
                continue   # market research blog — ignore entirely

            full_text = title
            if not is_relevant(full_text):
                meta["relevant_articles"] += 0
                continue

            meta["relevant_articles"] += 1

            # Parse date from GDELT format: "20260419T143000Z"
            try:
                pub_date = datetime.strptime(seendate[:8], "%Y%m%d").strftime("%Y-%m-%d")
            except Exception:
                pub_date = datetime.utcnow().strftime("%Y-%m-%d")

            # Extract company names from headline
            companies = extract_companies_from_text(title)

            # Use domain as company name ONLY if it looks like a real company site
            # (not wire services, media, or market research blogs)
            if domain and "." in domain and dom_type == 'company':
                domain_co = domain.split(".")[0].replace("-"," ").title()
                if len(domain_co) > 2 and domain_co.lower() not in NOT_COMPANY_WORDS:
                    companies = [domain_co] + companies

            # Confidence penalty for non-company domains
            base_confidence = 0.68 if dom_type == 'company' else 0.52

            kws      = extract_keywords(full_text)
            sig_state = detect_state(full_text) or state

            # Signal type classification
            tl = title.lower()
            if any(x in tl for x in ["appoint","named","cio","cto","new vp","hire","joins"]):
                sig_type = "timing"
            elif any(x in tl for x in ["acqui","merger","partner"]):
                sig_type = "timing"
            elif any(x in tl for x in ["migrat","transform","modern","cloud","sap"]):
                sig_type = "transformation"
            else:
                sig_type = "pain"

            for co_name in companies[:2]:
                key = clean_company_name(co_name)
                if not key or len(key) < 2:
                    continue

                sig = LiveSignal(
                    company_name=co_name,
                    source_url=url_a,
                    source_type="gdelt",
                    date_found=pub_date,
                    signal_type=sig_type,
                    raw_snippet=f"{title} | {domain}",
                    extracted_keywords=kws,
                    confidence_score=base_confidence,
                    state_detected=sig_state,
                    city_detected=detect_city(title),
                    enterprise_relevance_score=enterprise_relevance(kws),
                    live_collected=True,
                    source_access_status="success",
                    parser_used="gdelt_json",
                    extraction_method="structured_api",
                    data_source="LIVE",
                )

                if key not in discovered:
                    discovered[key] = DiscoveredCompany(
                        name=co_name,
                        hq_state=sig_state,
                        hq_city=detect_city(title),
                        discovery_source="gdelt",
                        confidence=0.68,
                    )
                discovered[key].signals.append(sig)

            if len(discovered) >= max_companies:
                break

        if len(discovered) >= max_companies:
            break

        time.sleep(1.0)  # GDELT rate limiting

    meta["companies_found"] = len(discovered)
    logger.info(f"[GDELT] {state}: {len(discovered)} companies from "
                f"{meta['queries_run']} queries, {meta['raw_articles']} articles")
    return list(discovered.values())[:max_companies], meta


# ════════════════════════════════════════════════════════════════════
# SOURCE 3: NEWSAPI  ✅ Free key (100 req/day), AWS-friendly
# ════════════════════════════════════════════════════════════════════

def discover_from_newsapi(state: str, api_key: str,
                           max_companies: int = 25) -> Tuple[List[DiscoveredCompany], dict]:
    """
    ✅ RELIABLE WITH KEY — NewsAPI.org
    Free tier: 100 requests/day. Sign up at newsapi.org.
    Add as NEWSAPI_KEY in Streamlit secrets.
    Returns structured article data — more reliable than GDELT for US news.
    """
    meta = {
        "source":          "newsapi",
        "queries_run":     0,
        "raw_articles":    0,
        "companies_found": 0,
        "blocked":         False,
        "error":           None,
    }
    if not api_key:
        meta["error"] = "no_key"
        return [], meta

    state_name = STATE_NAMES.get(state, state)
    discovered: Dict[str, DiscoveredCompany] = {}

    queries = [
        f"Oracle database {state_name} enterprise",
        f"database administrator hiring {state_name}",
        f"CIO OR CTO appointed {state_name} company",
        f"cloud migration OR digital transformation {state_name}",
    ]

    cutoff = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")

    for query in queries[:3]:
        url = (f"https://newsapi.org/v2/everything"
               f"?q={quote_plus(query)}"
               f"&language=en"
               f"&sortBy=publishedAt"
               f"&from={cutoff}"
               f"&pageSize=20"
               f"&apiKey={api_key}")

        data = fetch_json(url, source_name="newsapi")
        meta["queries_run"] += 1

        if not data:
            meta["blocked"] = True
            continue
        if data.get("status") == "error":
            meta["error"] = data.get("message","unknown")
            logger.warning(f"[NewsAPI] Error: {meta['error']}")
            break

        articles = data.get("articles", [])
        meta["raw_articles"] += len(articles)
        logger.info(f"[NewsAPI] '{query[:50]}': {len(articles)} articles")

        for article in articles:
            title   = article.get("title","") or ""
            desc    = article.get("description","") or ""
            url_a   = article.get("url","")
            pub     = (article.get("publishedAt","") or "")[:10]
            source  = (article.get("source") or {}).get("name","")

            full_text = f"{title} {desc}"
            if not is_relevant(full_text):
                continue

            companies = extract_companies_from_text(full_text)
            # NewsAPI source.name is often the publisher company
            kws       = extract_keywords(full_text)
            sig_state = detect_state(full_text) or state

            tl = title.lower()
            if any(x in tl for x in ["appoint","named","cio","cto","new vp","hire"]):
                sig_type = "timing"
            elif any(x in tl for x in ["acqui","merger"]):
                sig_type = "timing"
            elif any(x in tl for x in ["migrat","transform","modern","cloud"]):
                sig_type = "transformation"
            else:
                sig_type = "pain"

            for co_name in companies[:2]:
                key = clean_company_name(co_name)
                if not key or len(key) < 2:
                    continue

                sig = LiveSignal(
                    company_name=co_name,
                    source_url=url_a,
                    source_type="newsapi",
                    date_found=pub or datetime.utcnow().strftime("%Y-%m-%d"),
                    signal_type=sig_type,
                    raw_snippet=f"{title} | {desc[:200]}",
                    extracted_keywords=kws,
                    confidence_score=0.72,
                    state_detected=sig_state,
                    city_detected=detect_city(full_text),
                    enterprise_relevance_score=enterprise_relevance(kws),
                    live_collected=True,
                    source_access_status="success",
                    parser_used="newsapi_json",
                    extraction_method="structured_api",
                    data_source="LIVE",
                )

                if key not in discovered:
                    discovered[key] = DiscoveredCompany(
                        name=co_name,
                        hq_state=sig_state,
                        discovery_source="newsapi",
                        confidence=0.72,
                    )
                discovered[key].signals.append(sig)

            if len(discovered) >= max_companies:
                break
        if len(discovered) >= max_companies:
            break
        time.sleep(0.5)

    meta["companies_found"] = len(discovered)
    logger.info(f"[NewsAPI] {state}: {len(discovered)} companies")
    return list(discovered.values())[:max_companies], meta


# ════════════════════════════════════════════════════════════════════
# SOURCE 4: SERPAPI  ✅ Paid key, best job discovery
# ════════════════════════════════════════════════════════════════════

SERP_JOB_QUERIES = [
    "Oracle DBA {state_name}",
    "database reliability engineer {state_name}",
    "SQL Server DBA {state_name}",
    "platform engineer database {state_name}",
    "cloud database engineer {state_name}",
    "database modernization {state_name}",
]


def discover_from_serpapi(state: str, api_key: str,
                           max_companies: int = 30) -> Tuple[List[DiscoveredCompany], dict]:
    """
    ✅ BEST JOB DISCOVERY — SerpAPI Google Jobs.
    $50/month. company_name is a direct structured field — no extraction.
    Sign up: serpapi.com. Add as SERPAPI_KEY in Streamlit secrets.
    """
    meta = {
        "source":          "serpapi_jobs",
        "queries_run":     0,
        "raw_jobs":        0,
        "companies_found": 0,
        "error":           None,
    }
    if not api_key:
        meta["error"] = "no_key"
        return [], meta

    state_name = STATE_NAMES.get(state, state)
    discovered: Dict[str, DiscoveredCompany] = {}

    for tmpl in SERP_JOB_QUERIES[:4]:
        query = tmpl.format(state_name=state_name)
        url   = (f"https://serpapi.com/search.json"
                 f"?engine=google_jobs"
                 f"&q={quote_plus(query)}"
                 f"&location={quote_plus(state_name)}"
                 f"&hl=en"
                 f"&api_key={api_key}")

        data = fetch_json(url, source_name="serpapi")
        meta["queries_run"] += 1

        if not data:
            continue
        if data.get("error"):
            meta["error"] = data["error"]
            logger.warning(f"[SerpAPI] Error: {meta['error']}")
            break

        jobs = data.get("jobs_results", [])
        meta["raw_jobs"] += len(jobs)
        logger.info(f"[SerpAPI] '{query}': {len(jobs)} jobs")

        for job in jobs:
            co_name  = job.get("company_name","").strip()
            location = job.get("location","")
            title    = job.get("title","")
            desc     = (job.get("description","") or "")[:400]

            if not co_name or len(co_name) < 2:
                continue

            # State filter
            sig_state = detect_state(location) or state
            if sig_state != state:
                continue

            kws = extract_keywords(f"{title} {desc}")
            key = clean_company_name(co_name)
            if not key:
                continue

            sig = LiveSignal(
                company_name=co_name,
                source_url=f"https://www.google.com/search?q={quote_plus(co_name+' jobs')}",
                source_type="serpapi_jobs",
                date_found=datetime.utcnow().strftime("%Y-%m-%d"),
                signal_type="hiring",
                raw_snippet=f"{title} | {co_name} | {location}",
                extracted_keywords=kws or [query.split()[0].lower()],
                confidence_score=0.88,
                state_detected=sig_state,
                city_detected=detect_city(location),
                likely_buyer_function="DBA/DBRE",
                enterprise_relevance_score=enterprise_relevance(kws),
                live_collected=True,
                source_access_status="success",
                parser_used="serpapi_json",
                extraction_method="structured_api",
                data_source="LIVE",
            )

            if key not in discovered:
                discovered[key] = DiscoveredCompany(
                    name=co_name,
                    hq_state=sig_state,
                    hq_city=detect_city(location),
                    discovery_source="serpapi_jobs",
                    confidence=0.88,
                )
            discovered[key].signals.append(sig)

            if len(discovered) >= max_companies:
                break

        if len(discovered) >= max_companies:
            break
        time.sleep(0.5)

    meta["companies_found"] = len(discovered)
    logger.info(f"[SerpAPI] {state}: {len(discovered)} companies")
    return list(discovered.values())[:max_companies], meta


# ════════════════════════════════════════════════════════════════════
# SOURCE 5: BING SEARCH  ✅ Paid key, very AWS-friendly
# ════════════════════════════════════════════════════════════════════

BING_QUERY_TEMPLATES = [
    'site:linkedin.com OR site:glassdoor.com "Oracle DBA" "{state_name}"',
    '"Oracle database" "{state_name}" enterprise company',
    '"CIO appointed" OR "CTO named" "{state_name}" 2025 OR 2026',
    '"database migration" "{state_name}" enterprise',
]


def discover_from_bing(state: str, api_key: str,
                        max_companies: int = 25) -> Tuple[List[DiscoveredCompany], dict]:
    """
    ✅ RELIABLE WITH KEY — Bing Web Search API.
    ~$3-7/1000 queries. Returns structured web results.
    Works perfectly from AWS (Microsoft cloud).
    Add as BING_API_KEY in Streamlit secrets.
    Azure portal: cognitive-services/bing/web-search
    """
    meta = {
        "source":          "bing_search",
        "queries_run":     0,
        "raw_results":     0,
        "companies_found": 0,
        "error":           None,
    }
    if not api_key:
        meta["error"] = "no_key"
        return [], meta

    state_name = STATE_NAMES.get(state, state)
    discovered: Dict[str, DiscoveredCompany] = {}

    for tmpl in BING_QUERY_TEMPLATES[:3]:
        query = tmpl.format(state_name=state_name)
        url   = f"https://api.bing.microsoft.com/v7.0/search?q={quote_plus(query)}&count=20&mkt=en-US"

        data = fetch_json(
            url, source_name="bing_search",
            # Bing requires the key in a header, not query param
            # fetch_json doesn't support custom headers — use requests directly
        )

        # Since fetch_json doesn't pass headers, use direct requests for Bing
        import requests as _req
        meta["queries_run"] += 1
        try:
            r = _req.get(
                url,
                headers={
                    "Ocp-Apim-Subscription-Key": api_key,
                    "User-Agent": "TessellSignalEngine/2.0",
                },
                timeout=10,
            )
            if r.status_code != 200:
                meta["error"] = f"HTTP {r.status_code}"
                logger.warning(f"[Bing] HTTP {r.status_code}: {r.text[:100]}")
                continue
            data = r.json()
        except Exception as e:
            meta["error"] = str(e)
            logger.warning(f"[Bing] Request error: {e}")
            continue

        results = (data.get("webPages") or {}).get("value", [])
        meta["raw_results"] += len(results)
        logger.info(f"[Bing] '{query[:50]}': {len(results)} results")

        for result in results:
            name_r  = result.get("name","")
            snippet = result.get("snippet","")
            url_r   = result.get("url","")
            full    = f"{name_r} {snippet}"

            if not is_relevant(full):
                continue

            companies = extract_companies_from_text(full)
            kws       = extract_keywords(full)
            sig_state = detect_state(full) or state

            tl = full.lower()
            if any(x in tl for x in ["appoint","named","cio","cto","hire"]):
                sig_type = "timing"
            elif any(x in tl for x in ["migrat","transform","modern"]):
                sig_type = "transformation"
            else:
                sig_type = "pain"

            for co_name in companies[:2]:
                key = clean_company_name(co_name)
                if not key or len(key) < 2:
                    continue

                sig = LiveSignal(
                    company_name=co_name,
                    source_url=url_r,
                    source_type="bing_search",
                    date_found=datetime.utcnow().strftime("%Y-%m-%d"),
                    signal_type=sig_type,
                    raw_snippet=f"{name_r} | {snippet[:200]}",
                    extracted_keywords=kws,
                    confidence_score=0.70,
                    state_detected=sig_state,
                    enterprise_relevance_score=enterprise_relevance(kws),
                    live_collected=True,
                    source_access_status="success",
                    parser_used="bing_json",
                    extraction_method="structured_api",
                    data_source="LIVE",
                )

                if key not in discovered:
                    discovered[key] = DiscoveredCompany(
                        name=co_name,
                        hq_state=sig_state,
                        discovery_source="bing_search",
                        confidence=0.70,
                    )
                discovered[key].signals.append(sig)
                if len(discovered) >= max_companies:
                    break
        if len(discovered) >= max_companies:
            break
        time.sleep(0.5)

    meta["companies_found"] = len(discovered)
    return list(discovered.values())[:max_companies], meta


# ════════════════════════════════════════════════════════════════════
# SOURCE 6: STATE SEED LIST  — guaranteed enterprise anchors
# ════════════════════════════════════════════════════════════════════

STATE_ENTERPRISE_SEEDS: Dict[str, List[str]] = {
    "TX": [
        "AT&T","ExxonMobil","McKesson","American Airlines","Southwest Airlines",
        "Texas Instruments","Dell Technologies","Energy Transfer","Kimberly-Clark",
        "ConocoPhillips","Phillips 66","Tenet Healthcare","Celanese","Fluor",
        "Vistra","NRG Energy","Flowserve","HollyFrontier","USAA",
        "Whole Foods Market","7-Eleven","GameStop","iHeartMedia","Match Group",
        "Toyota Motor North America","Jacobs Engineering","Neiman Marcus",
        "Commercial Metals","Pioneer Natural Resources","Atmos Energy",
    ],
    "OK": [
        "ONEOK","Devon Energy","Chesapeake Energy","Williams Companies",
        "BOK Financial","Mach Natural Resources","Alliance Resource Partners",
        "OGE Energy","Helmerich & Payne","Unit Corp","Matrix Service",
        "American Fidelity","Vast Bank","Enable Midstream","SemGroup",
    ],
    "KS": [
        "Spirit AeroSystems","Evergy","Garmin","Security Benefit",
        "Kansas City Life Insurance","Seaboard","Cerner","H&R Block",
        "YRC Worldwide","INTRUST Bank","CoreFirst Bank","CommunityAmerica",
        "Westar Energy","Payless ShoeSource",
    ],
    "AR": [
        "Walmart","J.B. Hunt","Dillard's","Murphy Oil","Axiom",
        "Stephens Inc","ArcBest","USA Truck","Acxiom","Hunt Oil",
    ],
    "MO": [
        "Emerson Electric","Edward Jones","Centene","Peabody Energy",
        "Graybar Electric","Stifel Financial","World Wide Technology",
        "Mastercard","Leidos Holdings","Maritz Holdings","Ameren",
    ],
    "CO": [
        "Newmont","DaVita","Arrow Electronics","Ball Corp",
        "Dish Network","Centura Health","Level 3 Communications",
        "Envision Healthcare","ReadyTalk","DigitalBridge",
    ],
    "TN": [
        "HCA Healthcare","FedEx","Dollar General","AutoZone","Unum Group",
        "Bridgestone Americas","Community Health Systems","Tractor Supply",
        "Thomas & Betts","Genesco",
    ],
    "IN": [
        "Cummins","Eli Lilly","Indiana University Health","Salesforce",
        "OneAmerica","Anthem","Roche Diagnostics","Rolls-Royce Americas",
        "Steel Technologies","Simon Property Group",
    ],
    "MN": [
        "UnitedHealth Group","Target","Best Buy","3M","General Mills",
        "US Bancorp","Xcel Energy","Ameriprise Financial","Toro",
        "Ecolab","Nuveen","Land O Lakes",
    ],
    "LA": [
        "Entergy","CenturyLink","Ochsner Health","Turner Industries",
        "Lamar Advertising","IberiaBank","Stone Energy","Tidewater",
    ],
    "AZ": [
        "Avnet","Microchip Technology","Freeport-McMoRan","Insight Direct",
        "PetSmart","Banner Health","Viad","Republic Services","ON Semiconductor",
    ],
    "NM": [
        "PNM Resources","Presbyterian Healthcare","Intel New Mexico",
        "Sandia National Laboratories","Lovelace Health System",
    ],
}


def discover_from_seeds(state: str) -> List[DiscoveredCompany]:
    seeds = STATE_ENTERPRISE_SEEDS.get(state, [])
    result = []
    for name in seeds:
        result.append(DiscoveredCompany(
            name=name,
            hq_state=state,
            is_public=True,
            discovery_source="state_seed_list",
            confidence=0.95,
        ))
    logger.info(f"[Seeds] {state}: {len(result)} known enterprise anchors")
    return result


# ════════════════════════════════════════════════════════════════════
# DEDUPLICATION
# ════════════════════════════════════════════════════════════════════

def _fuzzy_dedup(discovered: Dict[str, DiscoveredCompany]) -> List[DiscoveredCompany]:
    """
    Merge entries that look like the same company.
    'ONEOK Inc' + 'ONEOK' → keep the one with more signals, merge signals.
    """
    values = list(discovered.values())
    merged: List[DiscoveredCompany] = []
    used:   Set[int] = set()

    for i, co in enumerate(values):
        if i in used:
            continue
        base  = clean_company_name(co.name)
        group = [co]
        for j, other in enumerate(values):
            if j <= i or j in used:
                continue
            other_base = clean_company_name(other.name)
            if ((base in other_base or other_base in base)
                    and abs(len(base) - len(other_base)) <= 10):
                group.append(other)
                used.add(j)
        used.add(i)

        best = max(group, key=lambda c: len(c.signals))
        for other in group:
            if other is not best:
                best.signals.extend(other.signals)
                if not best.ticker and other.ticker:
                    best.ticker = other.ticker
                if not best.is_public and other.is_public:
                    best.is_public = other.is_public
                if other.discovery_source not in best.discovery_source:
                    best.discovery_source += f"+{other.discovery_source}"
        merged.append(best)

    return merged


# ════════════════════════════════════════════════════════════════════
# MASTER DISCOVERY FUNCTION
# ════════════════════════════════════════════════════════════════════

def discover_territory(
    state:           str,
    max_companies:   int            = 50,
    include_seeds:   bool           = True,
    newsapi_key:     Optional[str]  = None,
    serpapi_key:     Optional[str]  = None,
    bing_api_key:    Optional[str]  = None,
) -> dict:
    """
    Main entry point. Runs all available structured sources for a state.
    Returns deduplicated DiscoveredCompany list with signals attached.

    Sources run in reliability order. Paid sources (SerpAPI, Bing) run
    last so free sources always run even if keys are missing.
    """
    all_discovered:    Dict[str, DiscoveredCompany] = {}
    source_counts:     Dict[str, int]               = {}
    source_meta:       Dict[str, dict]               = {}

    # 1. Seeds — always first, guaranteed baseline
    if include_seeds:
        seeds = discover_from_seeds(state)
        for co in seeds:
            key = clean_company_name(co.name)
            if key:
                all_discovered[key] = co
        source_counts["state_seeds"] = len(seeds)
        source_meta["state_seeds"]   = {"source":"state_seed_list","accepted":len(seeds)}

    # 2. SEC EDGAR — public companies, direct company names
    edgar_cos, edgar_meta = discover_from_edgar(state, max_companies=25)
    for co in edgar_cos:
        key = clean_company_name(co.name)
        if key:
            if key in all_discovered:
                all_discovered[key].signals.extend(co.signals)
                all_discovered[key].is_public = True
                if co.ticker and not all_discovered[key].ticker:
                    all_discovered[key].ticker = co.ticker
                if "sec_edgar" not in all_discovered[key].discovery_source:
                    all_discovered[key].discovery_source += "+sec_edgar"
            else:
                all_discovered[key] = co
    source_counts["sec_edgar"] = edgar_meta["accepted"]
    source_meta["sec_edgar"]   = edgar_meta

    # 3. GDELT — news-based, no auth
    gdelt_cos, gdelt_meta = discover_from_gdelt(state, max_companies=30)
    for co in gdelt_cos:
        key = clean_company_name(co.name)
        if key:
            if key in all_discovered:
                all_discovered[key].signals.extend(co.signals)
                if "gdelt" not in all_discovered[key].discovery_source:
                    all_discovered[key].discovery_source += "+gdelt"
            else:
                all_discovered[key] = co
    source_counts["gdelt"] = gdelt_meta["companies_found"]
    source_meta["gdelt"]   = gdelt_meta

    # 4. NewsAPI — free key
    if newsapi_key:
        newsapi_cos, newsapi_meta = discover_from_newsapi(state, newsapi_key, 25)
        for co in newsapi_cos:
            key = clean_company_name(co.name)
            if key:
                if key in all_discovered:
                    all_discovered[key].signals.extend(co.signals)
                    if "newsapi" not in all_discovered[key].discovery_source:
                        all_discovered[key].discovery_source += "+newsapi"
                else:
                    all_discovered[key] = co
        source_counts["newsapi"] = newsapi_meta["companies_found"]
        source_meta["newsapi"]   = newsapi_meta

    # 5. SerpAPI — paid, best job coverage
    if serpapi_key:
        serp_cos, serp_meta = discover_from_serpapi(state, serpapi_key, 30)
        for co in serp_cos:
            key = clean_company_name(co.name)
            if key:
                if key in all_discovered:
                    all_discovered[key].signals.extend(co.signals)
                    if "serpapi_jobs" not in all_discovered[key].discovery_source:
                        all_discovered[key].discovery_source += "+serpapi_jobs"
                else:
                    all_discovered[key] = co
        source_counts["serpapi_jobs"] = serp_meta["companies_found"]
        source_meta["serpapi_jobs"]   = serp_meta

    # 6. Bing Search — paid
    if bing_api_key:
        bing_cos, bing_meta = discover_from_bing(state, bing_api_key, 25)
        for co in bing_cos:
            key = clean_company_name(co.name)
            if key:
                if key in all_discovered:
                    all_discovered[key].signals.extend(co.signals)
                    if "bing_search" not in all_discovered[key].discovery_source:
                        all_discovered[key].discovery_source += "+bing_search"
                else:
                    all_discovered[key] = co
        source_counts["bing_search"] = bing_meta["companies_found"]
        source_meta["bing_search"]   = bing_meta

    # Dedup
    before_dedup       = len(all_discovered)
    deduped            = _fuzzy_dedup(all_discovered)
    duplicates_removed = before_dedup - len(deduped)

    logger.info(
        f"[Discovery] {state}: {before_dedup} raw → "
        f"{duplicates_removed} deduped → {len(deduped)} unique | "
        f"sources: {source_counts}"
    )

    return {
        "state":             state,
        "total_found":       len(deduped),
        "before_dedup":      before_dedup,
        "duplicates_removed":duplicates_removed,
        "source_counts":     source_counts,
        "source_meta":       source_meta,
        "companies":         deduped,
    }
