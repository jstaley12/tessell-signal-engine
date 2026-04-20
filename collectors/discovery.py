"""
collectors/discovery.py  —  Tessell Signal Engine  |  Phase 1
════════════════════════════════════════════════════════════════════
Standardized signal ingestion — spec-compliant feed hierarchy.

PRIMARY SOURCES (Phase 1):
  1. SEC EDGAR full-text search
       Oracle/DB mentions, M&A complexity, cloud modernization spend,
       legacy infrastructure risk disclosures.
       No auth. Government API. Reliable on any hosting environment.

  2. NewsAPI.org
       CIO/CTO leadership changes, acquisitions, modernization
       announcements, partnerships, outages/incidents.
       Free key: 100 req/day. Add NEWSAPI_KEY to Streamlit secrets.

  3. Major RSS feeds (Reuters, MarketWatch, BusinessWire, PRNewswire,
                      GlobeNewswire IT feed)
       Company announcements, expansions, earnings commentary,
       cloud/digital transformation press releases.
       No auth. Reliable on AWS.

SECONDARY SOURCES (Phase 2 - lower signal weight):
  4. Careers/jobs pages — Oracle DBA hiring signals (future).

REMOVED (not in spec):
  - GDELT
  - SerpAPI
  - Bing Search API
  - Google News RSS (blocked on AWS IPs)
  - Greenhouse/Lever slug guessing

RULES:
  - No account ranks in Discovery mode without source evidence.
  - Show source + date + signal on each card.
  - Fresh signals outrank static seed accounts.
  - Seed/named accounts in a SEPARATE LIST, never mixed into live rankings.
"""
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Set, Tuple
from urllib.parse import quote_plus

from loguru import logger

from collectors.fetcher import fetch_json, fetch_html, FetchLog
from collectors.signal import (
    LiveSignal, is_relevant, extract_keywords,
    detect_state, detect_city,
    CITY_STATE, STATE_NAMES,
)

# ════════════════════════════════════════════════════════════════════
# FEED CONFIGURATION
# ════════════════════════════════════════════════════════════════════

# EDGAR: spec topics → search term → signal label
EDGAR_TERMS = {
    '"Oracle database"':        "oracle_mention",
    '"Oracle licensing"':       "oracle_cost",
    '"database modernization"': "modernization",
    '"cloud migration"':        "cloud_migration",
    '"legacy database"':        "legacy_risk",
}

# NewsAPI: spec topics (state_name injected at runtime)
NEWSAPI_QUERIES = {
    'new CIO OR new CTO OR "appointed CIO" OR "appointed CTO" {state_name}': "leadership_change",
    'acquisition OR merger {state_name} enterprise technology':               "ma_event",
    '"cloud migration" OR "digital transformation" {state_name} enterprise':  "modernization",
    '"Oracle database" OR "Oracle DBA" {state_name}':                        "oracle_pain",
    'outage OR incident OR downtime {state_name} technology enterprise':      "outage",
}

# RSS feeds per spec
RSS_FEEDS = [
    {"url": "https://feeds.reuters.com/reuters/businessNews",
     "name": "Reuters Business",     "type": "reuters_rss"},
    {"url": "https://feeds.marketwatch.com/marketwatch/topstories/",
     "name": "MarketWatch",          "type": "marketwatch_rss"},
    {"url": "https://www.prnewswire.com/rss/technology-latest-news.rss",
     "name": "PR Newswire Tech",     "type": "prnewswire_rss"},
    {"url": "https://feed.businesswire.com/rss/home/?rss=G22",
     "name": "BusinessWire",         "type": "businesswire_rss"},
    {"url": "https://www.globenewswire.com/RssFeed/subjectcode/28-Information+Technology",
     "name": "GlobeNewswire IT",     "type": "globenewswire_rss"},
]

STATE_FULL_NAMES = {v: k.title() for k, v in STATE_NAMES.items()}

# ════════════════════════════════════════════════════════════════════
# COMPANY MODEL
# ════════════════════════════════════════════════════════════════════

@dataclass
class DiscoveredCompany:
    name:             str
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
            "name":             self.name,
            "hq_state":         self.hq_state,
            "hq_city":          self.hq_city,
            "industry":         self.industry,
            "is_public":        self.is_public,
            "ticker":           self.ticker,
            "fortune_rank":     self.fortune_rank,
            "discovery_source": self.discovery_source,
            "signal_count":     len(self.signals),
            "confidence":       self.confidence,
            "discovery_date":   self.discovery_date,
        }


# ════════════════════════════════════════════════════════════════════
# COMPANY NAME EXTRACTION
# ════════════════════════════════════════════════════════════════════

NOT_COMPANY_WORDS = {
    "the","a","an","in","at","for","on","of","and","or","with","by","from","to",
    "new","report","says","announced","announces","hires","names","joins","appoints",
    "database","cloud","digital","enterprise","technology","tech","it","company",
    "oracle","sql","microsoft","amazon","google","ibm","sap","data","platform",
    "texas","oklahoma","kansas","dallas","houston","tulsa","wichita","austin",
    "inc","corp","llc","ltd","group","holdings","markets","research","global",
    "news","media","press","wire","daily","weekly","report","blog","analysis",
}

DB_VENDOR_NAMES = {
    "oracle","oracle corp","oracle corporation","snowflake","mongodb","databricks",
    "cloudera","teradata","couchbase","datastax","cockroachdb","neo4j","redis",
    "influxdata","yugabyte","memsql","singlestore","dremio","starburst","percona",
    "amazon","amazon.com","microsoft","google","alphabet","ibm","vmware","nutanix",
}

MEDIA_PUBLISHER_NAMES = {
    "reuters","bloomberg","wsj","cnbc","cnn","bbc","marketwatch","techcrunch",
    "businessinsider","venturebeat","wired","axios","theverge","zdnet",
    "computerworld","informationweek","seekingalpha","benzinga","motleyfool",
    "prnewswire","businesswire","globenewswire","accesswire","apnews",
}


def _clean_name(name: str) -> str:
    cleaned = re.sub(
        r'\s*,?\s*(?:Inc\.?|Corp\.?|Corporation|LLC\.?|Ltd\.?|Limited|'
        r'Co\.?|Group|Holdings?|Technologies?|Systems?|Solutions?|'
        r'Services?|Enterprises?|Industries?|International|Global|'
        r'National|Partners?|Capital|Financial|Energy|Airlines?|'
        r'Healthcare|Health|Medical)\.?\s*$',
        '', name, flags=re.IGNORECASE
    ).strip()
    return ' '.join(cleaned.split()).lower()


def _is_valid_company(name: str) -> bool:
    if len(name) < 3 or len(name) > 60:
        return False
    if not name[0].isupper():
        return False
    name_lower = name.lower()
    first_word = name_lower.split()[0]
    if first_word in NOT_COMPANY_WORDS:
        return False
    if name_lower in DB_VENDOR_NAMES:
        return False
    if any(p in name_lower for p in MEDIA_PUBLISHER_NAMES):
        return False
    return True


def _extract_companies(text: str) -> List[str]:
    candidates = []
    action = (r'(?:announced?|said|named?|hired?|appoints?|acquires?|'
              r'launches?|expands?|reports?|completes?|selects?|signs?|'
              r'merges?|migrates?|transforms?|upgrades?|partners?)')
    for pat in [
        rf'([A-Z][A-Za-z&\-\.\']+(?:\s+[A-Z][A-Za-z&\-\.\']+){{0,3}})\s+{action}',
        r'(?:at|for|with|join(?:ing|ed)?)\s+([A-Z][A-Za-z&\-\.\']+(?:\s+[A-Z][A-Za-z&\-\.\']+){0,3})',
        r"([A-Z][A-Za-z&\-\.\']+(?:\s+[A-Z][A-Za-z&\-\.\']+){0,2})'s\s",
    ]:
        for m in re.finditer(pat, text):
            name = m.group(1).strip().rstrip('.,;:')
            if _is_valid_company(name):
                candidates.append(name)
    return list(dict.fromkeys(candidates))


# ════════════════════════════════════════════════════════════════════
# SOURCE 1: SEC EDGAR
# ════════════════════════════════════════════════════════════════════

def discover_from_edgar(state: str, max_companies: int = 30) -> Tuple[List[DiscoveredCompany], dict]:
    """
    PRIMARY SOURCE 1 — SEC EDGAR full-text search.
    No auth required. Finds public companies disclosing Oracle/DB/M&A/cloud topics.
    Returns Tier 3 signals — EDGAR filings establish presence, not urgency.
    """
    meta = {"source":"sec_edgar","queries_run":0,"total_hits":0,"accepted":0,
            "blocked":False,"error":None}
    discovered: Dict[str, DiscoveredCompany] = {}
    cutoff = (datetime.utcnow() - timedelta(days=365)).strftime("%Y-%m-%d")

    for term, signal_topic in list(EDGAR_TERMS.items())[:4]:
        url = (f"https://efts.sec.gov/LATEST/search-index"
               f"?q={quote_plus(term)}&forms=10-K,10-Q"
               f"&dateRange=custom&startdt={cutoff}")
        data = fetch_json(url, source_name="sec_edgar")
        meta["queries_run"] += 1

        if not data:
            last = FetchLog.entries[-1] if FetchLog.entries else None
            if last and "403" in last.status:
                meta["blocked"] = True
                meta["error"]   = f"HTTP 403 on {term}"
            continue

        hits = (data.get("hits") or {}).get("hits", [])
        meta["total_hits"] += len(hits)
        logger.info(f"[EDGAR] '{term}': {len(hits)} hits")

        for hit in hits[:10]:
            src        = hit.get("_source", {})
            name       = (src.get("entity_name") or
                         (src.get("display_names") or [""])[0] or "").strip()
            ticker     = src.get("ticker","")
            form_type  = src.get("form_type","10-K")
            file_date  = src.get("file_date","")
            period     = src.get("period_of_report","")

            inc_states = src.get("inc_states") or src.get("location") or ""
            if isinstance(inc_states, list):
                inc_states = " ".join(inc_states)
            filing_state = detect_state(str(inc_states)) if inc_states else None

            if not name or len(name) < 3 or not _is_valid_company(name):
                continue
            if filing_state and filing_state != state:
                continue

            term_clean = term.strip('"')
            snippet    = f"SEC {form_type} ({period}): mentions '{term_clean}' — {name}"
            kws        = extract_keywords(f"{snippet} {term_clean}")

            sig = LiveSignal(
                company_name=name,
                source_url=f"https://efts.sec.gov/LATEST/search-index?q={quote_plus(term)}&entity={quote_plus(name)}",
                source_type="sec_edgar",
                date_found=file_date or datetime.utcnow().strftime("%Y-%m-%d"),
                signal_type="transformation",
                raw_snippet=snippet,
                extracted_keywords=kws or ["oracle","database"],
                confidence_score=0.65,
                state_detected=filing_state or state,
                enterprise_relevance_score=0.60,
                live_collected=True,
                source_access_status="success",
                parser_used="sec_efts_json",
                extraction_method="structured_api",
                data_source="LIVE",
            )

            key = _clean_name(name)
            if not key:
                continue
            if key not in discovered:
                discovered[key] = DiscoveredCompany(
                    name=name, ticker=ticker or None,
                    hq_state=filing_state or state, is_public=True,
                    discovery_source="sec_edgar", confidence=0.80,
                )
            discovered[key].signals.append(sig)
            if ticker:
                discovered[key].ticker = ticker

        if len(discovered) >= max_companies:
            break
        time.sleep(0.4)

    meta["accepted"] = len(discovered)
    logger.info(f"[EDGAR] {state}: {len(discovered)} companies, blocked={meta['blocked']}")
    return list(discovered.values())[:max_companies], meta


# ════════════════════════════════════════════════════════════════════
# SOURCE 2: NEWSAPI
# ════════════════════════════════════════════════════════════════════

def discover_from_newsapi(state: str, api_key: str,
                           max_companies: int = 30) -> Tuple[List[DiscoveredCompany], dict]:
    """
    PRIMARY SOURCE 2 — NewsAPI.org
    Free key at newsapi.org (100 req/day). Add as NEWSAPI_KEY in Streamlit secrets.
    Spec topics: CIO/CTO changes, M&A, modernization, Oracle pain, outages.
    Signals from NewsAPI are Tier 1/2 depending on content.
    """
    meta = {"source":"newsapi","queries_run":0,"raw_articles":0,
            "companies_found":0,"blocked":False,"error":None}
    if not api_key:
        meta["error"] = "no_key — add free key at newsapi.org to Streamlit secrets"
        return [], meta

    state_name = STATE_FULL_NAMES.get(state, state)
    discovered: Dict[str, DiscoveredCompany] = {}
    cutoff = (datetime.utcnow() - timedelta(days=30)).strftime("%Y-%m-%d")

    for query_tmpl, signal_topic in list(NEWSAPI_QUERIES.items())[:4]:
        query = query_tmpl.format(state_name=state_name)
        url   = (f"https://newsapi.org/v2/everything"
                 f"?q={quote_plus(query)}&language=en&sortBy=publishedAt"
                 f"&from={cutoff}&pageSize=20&apiKey={api_key}")

        data = fetch_json(url, source_name="newsapi")
        meta["queries_run"] += 1

        if not data:
            meta["blocked"] = True
            continue
        if data.get("status") == "error":
            meta["error"] = data.get("message","unknown")
            logger.warning(f"[NewsAPI] Error: {meta['error']}")
            break

        articles = data.get("articles",[])
        meta["raw_articles"] += len(articles)
        logger.info(f"[NewsAPI] '{query[:60]}': {len(articles)} articles")

        for article in articles:
            title = (article.get("title")       or "").strip()
            desc  = (article.get("description") or "").strip()
            url_a = (article.get("url")         or "")
            pub   = (article.get("publishedAt") or "")[:10]

            full = f"{title} {desc}"
            if not is_relevant(full):
                continue

            companies = _extract_companies(full)
            if not companies:
                continue

            kws       = extract_keywords(full)
            sig_state = detect_state(full) or state

            for co_name in companies[:2]:
                key = _clean_name(co_name)
                if not key:
                    continue

                sig = LiveSignal(
                    company_name=co_name,
                    source_url=url_a,
                    source_type="newsapi",
                    date_found=pub or datetime.utcnow().strftime("%Y-%m-%d"),
                    signal_type=signal_topic,
                    raw_snippet=f"{title} | {desc[:200]}",
                    extracted_keywords=kws,
                    confidence_score=0.78,
                    state_detected=sig_state,
                    city_detected=detect_city(full),
                    enterprise_relevance_score=0.68,
                    live_collected=True,
                    source_access_status="success",
                    parser_used="newsapi_json",
                    extraction_method="structured_api",
                    data_source="LIVE",
                )

                if key not in discovered:
                    discovered[key] = DiscoveredCompany(
                        name=co_name, hq_state=sig_state,
                        hq_city=detect_city(full),
                        discovery_source="newsapi", confidence=0.78,
                    )
                discovered[key].signals.append(sig)

            if len(discovered) >= max_companies:
                break
        if len(discovered) >= max_companies:
            break
        time.sleep(0.3)

    meta["companies_found"] = len(discovered)
    logger.info(f"[NewsAPI] {state}: {len(discovered)} companies")
    return list(discovered.values())[:max_companies], meta


# ════════════════════════════════════════════════════════════════════
# SOURCE 3: MAJOR RSS FEEDS
# ════════════════════════════════════════════════════════════════════

def _parse_rss_date(raw: str) -> str:
    if not raw:
        return datetime.utcnow().strftime("%Y-%m-%d")
    try:
        from email.utils import parsedate_to_datetime
        return parsedate_to_datetime(raw).strftime("%Y-%m-%d")
    except Exception:
        pass
    m = re.search(r'(\d{4}-\d{2}-\d{2})', raw)
    return m.group(1) if m else datetime.utcnow().strftime("%Y-%m-%d")


def discover_from_rss(state: str, max_companies: int = 30) -> Tuple[List[DiscoveredCompany], dict]:
    """
    PRIMARY SOURCE 3 — Reuters, MarketWatch, BusinessWire, PRNewswire, GlobeNewswire IT.
    No auth required. Parses company announcements relevant to target state.
    """
    meta = {"source":"rss_feeds","feeds_tried":0,"feeds_ok":0,"raw_items":0,
            "relevant_items":0,"companies_found":0,"blocked_feeds":[],"errors":[]}
    discovered: Dict[str, DiscoveredCompany] = {}
    state_name = (STATE_FULL_NAMES.get(state, state) or state).lower()

    for feed in RSS_FEEDS:
        feed_url  = feed["url"]
        feed_name = feed["name"]
        feed_type = feed["type"]
        meta["feeds_tried"] += 1

        html = fetch_html(feed_url, source_name=feed_type)
        if not html:
            last = FetchLog.entries[-1] if FetchLog.entries else None
            if last and ("403" in last.status or "blocked" in last.status):
                meta["blocked_feeds"].append(feed_name)
            else:
                meta["errors"].append(f"{feed_name}: no response")
            continue

        meta["feeds_ok"] += 1
        try:
            from bs4 import BeautifulSoup
            soup  = BeautifulSoup(html, "lxml-xml")
            items = soup.find_all("item")
            if not items:
                soup  = BeautifulSoup(html, "lxml")
                items = soup.find_all("item")

            meta["raw_items"] += len(items)
            logger.info(f"[RSS/{feed_name}] {len(items)} items")

            for item in items:
                title_el = item.find("title")
                desc_el  = item.find("description")
                link_el  = item.find("link")
                date_el  = item.find("pubDate") or item.find("dc:date")

                title    = title_el.get_text(strip=True) if title_el else ""
                desc     = desc_el.get_text(strip=True)  if desc_el  else ""
                link     = link_el.get_text(strip=True)  if link_el  else feed_url
                pub_date = _parse_rss_date(date_el.get_text(strip=True) if date_el else "")

                full = f"{title} {desc}"
                if state_name not in full.lower() and state not in full:
                    continue
                if not is_relevant(full):
                    continue

                meta["relevant_items"] += 1
                companies = _extract_companies(full)
                if not companies:
                    continue

                kws       = extract_keywords(full)
                sig_state = detect_state(full) or state

                tl = title.lower()
                if any(x in tl for x in ["cio","cto","appoint","named","hired as","joins as"]):
                    sig_type = "leadership_change"
                elif any(x in tl for x in ["acqui","merger","merges","acquires"]):
                    sig_type = "ma_event"
                elif any(x in tl for x in ["migrat","transform","modern","cloud"]):
                    sig_type = "modernization"
                elif any(x in tl for x in ["outage","incident","downtime","fail"]):
                    sig_type = "outage"
                elif any(x in tl for x in ["oracle","database","dba"]):
                    sig_type = "oracle_pain"
                else:
                    sig_type = "announcement"

                for co_name in companies[:2]:
                    key = _clean_name(co_name)
                    if not key:
                        continue

                    sig = LiveSignal(
                        company_name=co_name,
                        source_url=link,
                        source_type=feed_type,
                        date_found=pub_date,
                        signal_type=sig_type,
                        raw_snippet=f"{title} | {desc[:200]}",
                        extracted_keywords=kws,
                        confidence_score=0.72,
                        state_detected=sig_state,
                        city_detected=detect_city(full),
                        enterprise_relevance_score=0.65,
                        live_collected=True,
                        source_access_status="success",
                        parser_used="rss_xml",
                        extraction_method="rss_feed",
                        data_source="LIVE",
                    )

                    if key not in discovered:
                        discovered[key] = DiscoveredCompany(
                            name=co_name, hq_state=sig_state,
                            hq_city=detect_city(full),
                            discovery_source=feed_type, confidence=0.72,
                        )
                    discovered[key].signals.append(sig)

                    if len(discovered) >= max_companies:
                        break

        except Exception as e:
            meta["errors"].append(f"{feed_name}: {str(e)[:60]}")
            logger.warning(f"[RSS/{feed_name}] Parse error: {e}")

        if len(discovered) >= max_companies:
            break
        time.sleep(0.3)

    meta["companies_found"] = len(discovered)
    logger.info(f"[RSS] {state}: {len(discovered)} companies from "
               f"{meta['feeds_ok']}/{meta['feeds_tried']} feeds")
    return list(discovered.values())[:max_companies], meta


# ════════════════════════════════════════════════════════════════════
# SIGNAL ENRICHMENT — targeted NewsAPI fetch per discovered company
# ════════════════════════════════════════════════════════════════════

def enrich_company_signals(company_name: str, newsapi_key: str) -> List[LiveSignal]:
    """Fetch targeted Oracle/CIO/M&A signals for a specific company via NewsAPI."""
    if not newsapi_key:
        return []

    additional = []
    for query in [
        f'"{company_name}" Oracle database OR "database migration"',
        f'"{company_name}" CIO OR CTO OR "cloud migration" OR acquisition',
    ][:2]:
        url  = (f"https://newsapi.org/v2/everything"
                f"?q={quote_plus(query)}&language=en"
                f"&sortBy=publishedAt&pageSize=5&apiKey={newsapi_key}")
        data = fetch_json(url, source_name="newsapi_enrichment")
        if not data or data.get("status") == "error":
            continue

        for article in data.get("articles",[])[:3]:
            title = (article.get("title")       or "").strip()
            desc  = (article.get("description") or "").strip()
            url_a = (article.get("url")         or "")
            pub   = (article.get("publishedAt") or "")[:10]

            full  = f"{title} {desc}"
            if not is_relevant(full):
                continue

            kws   = extract_keywords(full)
            tl    = title.lower()
            if any(x in tl for x in ["cio","cto","appoint","named"]):
                sig_type = "leadership_change"
            elif any(x in tl for x in ["acqui","merger"]):
                sig_type = "ma_event"
            else:
                sig_type = "oracle_pain"

            sig = LiveSignal(
                company_name=company_name,
                source_url=url_a,
                source_type="newsapi",
                date_found=pub or datetime.utcnow().strftime("%Y-%m-%d"),
                signal_type=sig_type,
                raw_snippet=f"{title} | {desc[:200]}",
                extracted_keywords=kws,
                confidence_score=0.80,
                state_detected=None,
                enterprise_relevance_score=0.72,
                live_collected=True,
                source_access_status="success",
                parser_used="newsapi_json",
                extraction_method="structured_api",
                data_source="LIVE",
            )
            additional.append(sig)
        time.sleep(0.2)

    return additional


# ════════════════════════════════════════════════════════════════════
# SEED LIST — separate from live rankings per spec
# ════════════════════════════════════════════════════════════════════

STATE_SEED_LIST: Dict[str, List[str]] = {
    "TX": [
        "AT&T","ExxonMobil","McKesson","American Airlines","Southwest Airlines",
        "Texas Instruments","Dell Technologies","Energy Transfer","Kimberly-Clark",
        "ConocoPhillips","Phillips 66","Tenet Healthcare","Fluor","Vistra",
        "NRG Energy","USAA","Whole Foods Market","7-Eleven","iHeartMedia",
        "Toyota Motor North America","Jacobs Engineering","Atmos Energy",
    ],
    "OK": ["ONEOK","Devon Energy","Chesapeake Energy","Williams Companies",
           "BOK Financial","OGE Energy","Helmerich & Payne"],
    "KS": ["Spirit AeroSystems","Evergy","Garmin","Seaboard","INTRUST Bank"],
    "AR": ["Walmart","J.B. Hunt","Dillard's","Murphy Oil","ArcBest"],
    "MO": ["Emerson Electric","Edward Jones","Centene","Mastercard"],
    "TN": ["HCA Healthcare","FedEx","Dollar General","AutoZone","Unum Group"],
    "IN": ["Cummins","Eli Lilly","Salesforce","OneAmerica"],
    "MN": ["UnitedHealth Group","Target","Best Buy","3M","General Mills","US Bancorp"],
    "CO": ["Newmont","DaVita","Arrow Electronics","Ball Corp"],
    "LA": ["Entergy","Ochsner Health"],
    "AZ": ["Avnet","Microchip Technology","Freeport-McMoRan"],
    "NM": ["PNM Resources","Intel New Mexico"],
}


def get_seed_list(state: str) -> List[DiscoveredCompany]:
    """
    Returns known Fortune-tier anchors. Per spec: SEPARATE LIST from live rankings.
    Seeds have discovery_source='seed_list' for clear labeling.
    They never rank against live-signal companies.
    """
    seeds = []
    for name in STATE_SEED_LIST.get(state, []):
        seeds.append(DiscoveredCompany(
            name=name, hq_state=state, is_public=True,
            discovery_source="seed_list", confidence=0.95,
        ))
    logger.info(f"[Seeds] {state}: {len(seeds)} known enterprise anchors")
    return seeds


# ════════════════════════════════════════════════════════════════════
# DEDUP
# ════════════════════════════════════════════════════════════════════

def _fuzzy_dedup(disc: Dict[str, DiscoveredCompany]) -> List[DiscoveredCompany]:
    values  = list(disc.values())
    merged: List[DiscoveredCompany] = []
    used:   Set[int] = set()
    for i, co in enumerate(values):
        if i in used:
            continue
        base  = _clean_name(co.name)
        group = [co]
        for j, other in enumerate(values):
            if j <= i or j in used:
                continue
            ob = _clean_name(other.name)
            if (base in ob or ob in base) and abs(len(base) - len(ob)) <= 10:
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
# CLASSIFICATION HELPERS (used by streamlit_app.py)
# ════════════════════════════════════════════════════════════════════

def classify_discovery_type(discovery_source: str) -> str:
    sources  = set(s.strip() for s in discovery_source.split("+") if s.strip())
    has_seed = "seed_list" in sources
    has_live = bool(sources - {"seed_list"})
    if has_seed and has_live:
        return "mixed_source"
    if has_seed:
        return "seed_list"
    return "live_discovered"


def why_discovered(discovery_source: str, signals: list) -> str:
    sources = set(s.strip() for s in discovery_source.split("+") if s.strip())
    reasons = []
    if "sec_edgar"         in sources: reasons.append("SEC 10-K/10-Q: Oracle/DB mention")
    if "newsapi"           in sources: reasons.append("NewsAPI: leadership/M&A/modernization")
    if "reuters_rss"       in sources: reasons.append("Reuters business news")
    if "marketwatch_rss"   in sources: reasons.append("MarketWatch")
    if "prnewswire_rss"    in sources: reasons.append("PR Newswire")
    if "businesswire_rss"  in sources: reasons.append("BusinessWire")
    if "globenewswire_rss" in sources: reasons.append("GlobeNewswire IT")
    if "seed_list"         in sources: reasons.append("Known enterprise anchor (seed)")
    return "; ".join(reasons) if reasons else "live discovery"


def tessell_relevance_reason(company_name: str, industry: str, signals: list) -> str:
    ind_lower = (industry or "").lower()

    INDUSTRY_NARRATIVES = {
        "airline":        "24/7 Oracle reservation + loyalty systems — known DB complexity",
        "airlines":       "24/7 Oracle reservation + loyalty systems — known DB complexity",
        "hospital":       "Oracle/EHR systems with HIPAA DR requirements — high DBA toil",
        "healthcare":     "Healthcare Oracle footprint + DR/HA compliance requirements",
        "banking":        "Core banking Oracle/SQL, 24/7 SLA, regulatory compliance",
        "financial":      "Financial Oracle dependency + audit/compliance pressure",
        "insurance":      "Policy/claims Oracle — DBA overhead and provisioning pain",
        "energy":         "SAP/Oracle ERP + SCADA + multi-region DR",
        "oil":            "Oracle ERP — DBA toil and DR needs",
        "midstream":      "Oracle ERP — pipeline management DB complexity",
        "telecom":        "Billing/BSS on Oracle — highest DB complexity",
        "telecommunications": "OSS/BSS Oracle stack — DBA toil, strong Tessell fit",
        "manufacturing":  "Oracle/SAP ERP — non-prod provisioning delays",
        "automotive":     "Oracle ERP + supply chain DB complexity",
        "logistics":      "WMS/TMS Oracle stack — DR requirements, multi-region",
        "pharmaceutical": "Oracle ERP + GxP validation + DR requirements",
        "defense":        "Oracle ERP + FedRAMP/security compliance",
        "retail":         "Oracle/SAP ERP + seasonal scaling + DR",
    }

    base = next(
        (v for k, v in INDUSTRY_NARRATIVES.items() if k in ind_lower), None
    )

    if not signals:
        return base or "Enterprise-qualified — no live signals yet"

    kws: Set[str] = set()
    for s in signals:
        extracted = (s.extracted_keywords if hasattr(s, "extracted_keywords")
                     else s.get("extracted_keywords", []))
        kws.update(k.lower() for k in extracted)

    parts = []
    if kws & {"oracle","oracle dba","oracle rac","oracle exadata","oracle licensing"}:
        hit = kws & {"oracle dba","oracle rac","oracle exadata","oracle licensing","oracle"}
        parts.append(f"Oracle footprint: {', '.join(sorted(hit)[:2])}")
    if kws & {"database administrator","dba","database reliability","dbre","oracle dba"}:
        hit = kws & {"oracle dba","database administrator","dba","dbre"}
        parts.append(f"DB/SRE hiring: {', '.join(sorted(hit)[:2])}")
    if kws & {"cloud migration","oracle migration","database migration","data center exit"}:
        hit = kws & {"oracle migration","cloud migration","database migration","data center exit"}
        parts.append(f"Migration: {', '.join(sorted(hit)[:2])}")
    if kws & {"acquisition","merger","new cio","new cto"}:
        hit = kws & {"acquisition","merger","new cio","new cto"}
        parts.append(f"Timing trigger: {', '.join(sorted(hit)[:2])}")

    if parts:
        prefix = (base.split("—")[0].strip() + " — ") if base else ""
        return prefix + "; ".join(parts)

    return base or "Enterprise signals detected"


# ════════════════════════════════════════════════════════════════════
# MASTER DISCOVERY FUNCTION
# ════════════════════════════════════════════════════════════════════

def discover_territory(
    state:        str,
    max_companies: int           = 50,
    include_seeds: bool          = True,
    newsapi_key:   Optional[str] = None,
    serpapi_key:   Optional[str] = None,   # not used (not in spec)
    bing_api_key:  Optional[str] = None,   # not used (not in spec)
) -> dict:
    """
    Main entry point. Runs all Phase 1 primary sources.
    Returns live-discovered companies and seed list separately.
    """
    live:         Dict[str, DiscoveredCompany] = {}
    source_counts: Dict[str, int]              = {}
    source_meta:   Dict[str, dict]             = {}

    # SOURCE 1: SEC EDGAR
    edgar_cos, edgar_meta = discover_from_edgar(state, max_companies=25)
    for co in edgar_cos:
        key = _clean_name(co.name)
        if key:
            if key in live:
                live[key].signals.extend(co.signals)
                live[key].is_public = True
                if "sec_edgar" not in live[key].discovery_source:
                    live[key].discovery_source += "+sec_edgar"
            else:
                live[key] = co
    source_counts["sec_edgar"] = edgar_meta["accepted"]
    source_meta["sec_edgar"]   = edgar_meta

    # SOURCE 2: NEWSAPI
    if newsapi_key:
        newsapi_cos, newsapi_meta = discover_from_newsapi(state, newsapi_key, 25)
        for co in newsapi_cos:
            key = _clean_name(co.name)
            if key:
                if key in live:
                    live[key].signals.extend(co.signals)
                    if "newsapi" not in live[key].discovery_source:
                        live[key].discovery_source += "+newsapi"
                else:
                    live[key] = co
        source_counts["newsapi"] = newsapi_meta["companies_found"]
        source_meta["newsapi"]   = newsapi_meta
    else:
        source_counts["newsapi"] = 0
        source_meta["newsapi"]   = {
            "source": "newsapi",
            "error":  "no_key — add NEWSAPI_KEY to Streamlit secrets (free at newsapi.org)",
            "accepted": 0,
        }

    # SOURCE 3: RSS FEEDS
    rss_cos, rss_meta = discover_from_rss(state, max_companies=25)
    for co in rss_cos:
        key = _clean_name(co.name)
        if key:
            if key in live:
                live[key].signals.extend(co.signals)
                if co.discovery_source not in live[key].discovery_source:
                    live[key].discovery_source += f"+{co.discovery_source}"
            else:
                live[key] = co
    source_counts["rss_feeds"] = rss_meta["companies_found"]
    source_meta["rss_feeds"]   = rss_meta

    # SIGNAL ENRICHMENT — targeted NewsAPI per discovered company
    if newsapi_key:
        for key, co in list(live.items())[:15]:
            extra = enrich_company_signals(co.name, newsapi_key)
            if extra:
                co.signals.extend(extra)

    # DEDUP
    before_dedup       = len(live)
    deduped            = _fuzzy_dedup(live)
    duplicates_removed = before_dedup - len(deduped)

    # SEEDS — separate list per spec
    seeds = get_seed_list(state) if include_seeds else []

    logger.info(
        f"[Discovery] {state}: {len(deduped)} live + {len(seeds)} seeds | "
        f"sources={source_counts}"
    )

    return {
        "state":              state,
        "companies":          deduped,    # live-discovered only
        "seed_companies":     seeds,      # separate per spec
        "total_live":         len(deduped),
        "total_seeds":        len(seeds),
        "before_dedup":       before_dedup,
        "duplicates_removed": duplicates_removed,
        "source_counts":      source_counts,
        "source_meta":        source_meta,
    }
