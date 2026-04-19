"""
collectors/live_collectors.py
All live source collectors. Every signal carries explicit labeling:
  live_collected, source_access_status, parser_used, extraction_method.

Source status (production, from normal laptop/server):
  ✅ GREENHOUSE   — public JSON API, no auth, ~25% F1000 coverage
  ✅ LEVER        — public JSON API, no auth, ~15% F1000 coverage
  ✅ GOOGLE NEWS  — public RSS feed, no auth
  ✅ CAREERS PAGE — static HTML scraper, 3-strategy extraction
  ✅ NEWSROOM     — static HTML scraper
  ⚠️  WORKDAY     — static HTML only; JS listings need Playwright (Phase 3)
  ⚠️  ICIMS       — client-side rendered; Playwright needed (Phase 3)
  ⚠️  SEC EDGAR   — filing index only; full text = Phase 3
  ❌ IR PAGES     — variable layout; per-company adapters needed
  ❌ CONFERENCE   — no standard format; Phase 3
"""
from __future__ import annotations

import json, re, sys, os, time
from datetime import datetime
from typing import List, Optional, Tuple, Dict
from urllib.parse import quote_plus, urljoin

from bs4 import BeautifulSoup
from loguru import logger

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from collectors.fetcher import (
    fetch_html, fetch_json, parse_html, dedup_hash, FetchLog, FetchEntry,
)
from collectors.signal import (
    LiveSignal, is_relevant, is_relevant_title,
    extract_keywords, detect_state, detect_city,
    infer_buyer, enterprise_relevance,
)


# ════════════════════════════════════════════════════════════════════
# GREENHOUSE  ✅ public JSON API
# ════════════════════════════════════════════════════════════════════

GREENHOUSE_SLUGS: Dict[str, str] = {
    "southwest airlines":    "southwestairlines",
    "oneok":                 "oneok",
    "cummins":               "cummins",
    "mckesson":              "mckesson",
    "j.b. hunt":             "jbhunt",
    "jb hunt":               "jbhunt",
    "humana":                "humana",
    "dollar general":        "dollargeneral",
    "cognizant":             "cognizant",
    "american airlines":     "aa",
    "at&t":                  "att",
    "kimberly-clark":        "kimberlyclark",
    "spirit aerosystems":    "spiritaero",
    "devon energy":          "devonenergy",
    "hca healthcare":        "hcahealthcare",
    "fedex":                 "fedex",
    "eli lilly":             "lilly",
    "conocophillips":        "conocophillips",
    "phillips 66":           "phillips66",
    "jpmorgan chase":        "jpmorgan",
    "citigroup":             "citi",
    "boeing":                "boeing",
    "general motors":        "gm",
    "unitedhealth group":    "unitedhealthgroup",
    "manpowergroup":         "manpower",
    "hp inc":                "hpinc",
}


def collect_greenhouse(company: str, slug: Optional[str] = None,
                       max_signals: int = 40) -> Tuple[List[LiveSignal], dict]:
    board_slug = slug or GREENHOUSE_SLUGS.get(company.lower())
    meta: dict = {
        "source": "greenhouse_api",
        "slug": board_slug,
        "endpoint": None,
        "total_jobs_fetched": 0,
        "relevant_signals": 0,
        "access_status": "no_slug" if not board_slug else None,
    }
    if not board_slug:
        FetchLog.record(FetchEntry(
            url=f"greenhouse/{company}", domain="boards-api.greenhouse.io",
            status="no_slug", http_code=None, elapsed_ms=0, source_name="greenhouse",
            note=f"No Greenhouse slug configured for: {company}",
        ))
        return [], meta

    url = f"https://boards-api.greenhouse.io/v1/boards/{board_slug}/jobs?content=true"
    meta["endpoint"] = url
    data = fetch_json(url, source_name="greenhouse")

    if not data:
        meta["access_status"] = FetchLog.entries[-1].status if FetchLog.entries else "error"
        return [], meta

    meta["access_status"] = "success"
    jobs = data.get("jobs", []) if isinstance(data, dict) else []
    meta["total_jobs_fetched"] = len(jobs)
    logger.info(f"[Greenhouse] {company}: {len(jobs)} total jobs fetched")

    signals, seen = [], set()
    for job in jobs:
        title    = job.get("title", "")
        location = job.get("location", {}).get("name", "")
        job_url  = job.get("absolute_url", url)
        content  = BeautifulSoup(job.get("content", ""), "lxml").get_text(" ", strip=True)[:500]

        if not is_relevant_title(title): continue
        full = f"{title} {location} {content}"
        if not is_relevant(full): continue

        h = dedup_hash(job_url, title)
        if h in seen: continue
        seen.add(h)

        kws = extract_keywords(full)
        sig = LiveSignal(
            company_name=company, source_url=job_url,
            source_type="greenhouse_api",
            date_found=datetime.utcnow().strftime("%Y-%m-%d"),
            signal_type="hiring",
            raw_snippet=f"{title} | {location} | {content[:200]}",
            extracted_keywords=kws, confidence_score=0.88,
            state_detected=detect_state(location) or detect_state(title),
            city_detected=detect_city(location) or detect_city(title),
            likely_buyer_function=infer_buyer(title),
            enterprise_relevance_score=enterprise_relevance(kws),
            live_collected=True,
            source_access_status="success",
            parser_used="greenhouse_json",
            extraction_method="structured_api",
            data_source="LIVE", dedup_id=h,
        )
        signals.append(sig)
        if len(signals) >= max_signals: break

    meta["relevant_signals"] = len(signals)
    logger.info(f"[Greenhouse] {company}: {len(signals)} relevant signals after noise filter")
    return signals, meta


# ════════════════════════════════════════════════════════════════════
# LEVER  ✅ public JSON API
# ════════════════════════════════════════════════════════════════════

LEVER_SLUGS: Dict[str, str] = {
    "netflix":      "netflix",
    "stripe":       "stripe",
    "figma":        "figma",
    "cloudflare":   "cloudflare",
    "hashicorp":    "hashicorp",
    "databricks":   "databricks",
    "elastic":      "elastic",
    "grafana labs": "grafana",
    "mongodb":      "mongodb",
    "dbt labs":     "dbtlabs",
    "samsara":      "samsara",
    "fastly":       "fastly",
    "sumo logic":   "sumologic",
}


def collect_lever(company: str, slug: Optional[str] = None,
                  max_signals: int = 30) -> Tuple[List[LiveSignal], dict]:
    co_slug = slug or LEVER_SLUGS.get(company.lower())
    meta: dict = {
        "source": "lever_api",
        "slug": co_slug,
        "endpoint": None,
        "total_jobs_fetched": 0,
        "relevant_signals": 0,
        "access_status": "no_slug" if not co_slug else None,
    }
    if not co_slug:
        FetchLog.record(FetchEntry(
            url=f"lever/{company}", domain="api.lever.co",
            status="no_slug", http_code=None, elapsed_ms=0, source_name="lever",
            note=f"No Lever slug configured for: {company}",
        ))
        return [], meta

    url  = f"https://api.lever.co/v0/postings/{co_slug}?mode=json"
    meta["endpoint"] = url
    data = fetch_json(url, source_name="lever")

    if not data:
        meta["access_status"] = FetchLog.entries[-1].status if FetchLog.entries else "error"
        return [], meta

    meta["access_status"] = "success"
    jobs = data if isinstance(data, list) else data.get("data", [])
    meta["total_jobs_fetched"] = len(jobs)
    logger.info(f"[Lever] {company}: {len(jobs)} total jobs fetched")

    signals, seen = [], set()
    for job in jobs:
        title    = job.get("text", "")
        location = (job.get("categories") or {}).get("location", "") or ""
        job_url  = job.get("hostedUrl", url)
        lists    = job.get("lists") or []
        desc     = " ".join(
            BeautifulSoup(itm.get("content",""), "lxml").get_text(" ")
            for itm in lists
        )[:400]

        if not is_relevant_title(title): continue
        full = f"{title} {location} {desc}"
        if not is_relevant(full): continue

        h = dedup_hash(job_url, title)
        if h in seen: continue
        seen.add(h)

        kws = extract_keywords(full)
        sig = LiveSignal(
            company_name=company, source_url=job_url,
            source_type="lever_api",
            date_found=datetime.utcnow().strftime("%Y-%m-%d"),
            signal_type="hiring",
            raw_snippet=f"{title} | {location} | {desc[:200]}",
            extracted_keywords=kws, confidence_score=0.85,
            state_detected=detect_state(location) or detect_state(title),
            city_detected=detect_city(location) or detect_city(title),
            likely_buyer_function=infer_buyer(title),
            enterprise_relevance_score=enterprise_relevance(kws),
            live_collected=True,
            source_access_status="success",
            parser_used="lever_json",
            extraction_method="structured_api",
            data_source="LIVE", dedup_id=h,
        )
        signals.append(sig)
        if len(signals) >= max_signals: break

    meta["relevant_signals"] = len(signals)
    logger.info(f"[Lever] {company}: {len(signals)} relevant signals")
    return signals, meta


# ════════════════════════════════════════════════════════════════════
# CAREERS PAGE  ✅ 3-strategy HTML extraction
# ════════════════════════════════════════════════════════════════════

def collect_careers_page(company: str, careers_url: str,
                         max_signals: int = 30) -> Tuple[List[LiveSignal], dict]:
    meta: dict = {
        "source": "careers_page",
        "url": careers_url,
        "strategy_used": None,
        "raw_candidates": 0,
        "noise_filtered": 0,
        "relevant_signals": 0,
    }
    html = fetch_html(careers_url, source_name="careers_page")
    if not html:
        status = FetchLog.entries[-1].status if FetchLog.entries else "error"
        meta["access_status"] = status
        return [], meta

    meta["access_status"] = "success"
    soup = parse_html(html)
    signals, seen = [], set()

    # Strategy 1: JSON-LD
    for script in soup.find_all("script", {"type": "application/ld+json"}):
        try:
            ld = json.loads(script.string or "[]")
            items = ld if isinstance(ld, list) else [ld]
            for item in items:
                if item.get("@type") != "JobPosting": continue
                title = item.get("title", "")
                desc  = BeautifulSoup(item.get("description",""), "lxml").get_text(" ")[:400]
                loc   = item.get("jobLocation", {})
                if isinstance(loc, list): loc = loc[0] if loc else {}
                city  = loc.get("address", {}).get("addressLocality", "")
                state = loc.get("address", {}).get("addressRegion", "")
                job_url = item.get("url", careers_url)
                location = f"{city}, {state}".strip(", ")
                meta["raw_candidates"] += 1
                if not is_relevant_title(title) or not is_relevant(f"{title} {desc}"):
                    meta["noise_filtered"] += 1; continue
                h = dedup_hash(job_url, title)
                if h in seen: continue
                seen.add(h)
                kws = extract_keywords(f"{title} {location} {desc}")
                sig = LiveSignal(
                    company_name=company, source_url=job_url,
                    source_type="careers_page",
                    date_found=datetime.utcnow().strftime("%Y-%m-%d"),
                    signal_type="hiring",
                    raw_snippet=f"{title} | {location} | {desc[:200]}",
                    extracted_keywords=kws, confidence_score=0.83,
                    state_detected=detect_state(location) or detect_state(title),
                    city_detected=detect_city(location) or detect_city(title),
                    likely_buyer_function=infer_buyer(title),
                    enterprise_relevance_score=enterprise_relevance(kws),
                    live_collected=True, source_access_status="success",
                    parser_used="jsonld", extraction_method="jsonld_schema",
                    data_source="LIVE", dedup_id=h,
                )
                signals.append(sig)
        except Exception: pass

    if signals:
        meta["strategy_used"] = "jsonld"
        meta["relevant_signals"] = len(signals)
        return signals[:max_signals], meta

    # Strategy 2: HTML job elements
    els = (
        soup.find_all("li",  class_=re.compile(r"job|position|posting|opening", re.I)) or
        soup.find_all("div", class_=re.compile(r"job-card|job_card|job-item|jobCard|position-item", re.I)) or
        soup.find_all("article", class_=re.compile(r"job|position", re.I))
    )
    for el in els[:60]:
        meta["raw_candidates"] += 1
        title_el = (
            el.find(["h2","h3","h4","a","span"], class_=re.compile(r"title|job-title|position", re.I))
            or el.find(["h2","h3","h4","a"])
        )
        if not title_el: continue
        title = title_el.get_text(strip=True)
        if len(title) < 8 or len(title) > 150: continue
        if not is_relevant_title(title): meta["noise_filtered"] += 1; continue
        href = el.find("a", href=True)
        job_url = urljoin(careers_url, href["href"]) if href else careers_url
        loc_el  = el.find(class_=re.compile(r"location|loc|city", re.I))
        location = loc_el.get_text(strip=True) if loc_el else ""
        h = dedup_hash(job_url, title)
        if h in seen: continue
        seen.add(h)
        kws = extract_keywords(f"{title} {location}")
        if not kws: continue
        sig = LiveSignal(
            company_name=company, source_url=job_url,
            source_type="careers_page",
            date_found=datetime.utcnow().strftime("%Y-%m-%d"),
            signal_type="hiring",
            raw_snippet=f"{title} | {location}",
            extracted_keywords=kws, confidence_score=0.70,
            state_detected=detect_state(location) or detect_state(title),
            city_detected=detect_city(location) or detect_city(title),
            likely_buyer_function=infer_buyer(title),
            enterprise_relevance_score=enterprise_relevance(kws),
            live_collected=True, source_access_status="success",
            parser_used="html_elements", extraction_method="html_selector",
            data_source="LIVE", dedup_id=h,
        )
        signals.append(sig)
        if len(signals) >= max_signals: break

    if signals:
        meta["strategy_used"] = "html_elements"
        meta["relevant_signals"] = len(signals)
        return signals[:max_signals], meta

    # Strategy 3: text scan
    for line in soup.get_text("\n", strip=True).split("\n"):
        line = line.strip()
        meta["raw_candidates"] += 1
        if 15 < len(line) < 120 and is_relevant_title(line):
            kws = extract_keywords(line)
            if not kws: continue
            h = dedup_hash(careers_url, line)
            if h in seen: continue
            seen.add(h)
            sig = LiveSignal(
                company_name=company, source_url=careers_url,
                source_type="careers_page",
                date_found=datetime.utcnow().strftime("%Y-%m-%d"),
                signal_type="hiring", raw_snippet=line,
                extracted_keywords=kws, confidence_score=0.45,
                likely_buyer_function=infer_buyer(line),
                enterprise_relevance_score=enterprise_relevance(kws),
                live_collected=True, source_access_status="success",
                parser_used="text_scan", extraction_method="text_regex",
                data_source="LIVE", dedup_id=h,
            )
            signals.append(sig)
            if len(signals) >= max_signals: break

    meta["strategy_used"] = "text_scan" if signals else "none_matched"
    meta["relevant_signals"] = len(signals)
    logger.info(f"[Careers] {company}: strategy={meta['strategy_used']} "
                f"raw={meta['raw_candidates']} filtered={meta['noise_filtered']} "
                f"relevant={meta['relevant_signals']}")
    return signals[:max_signals], meta


# ════════════════════════════════════════════════════════════════════
# WORKDAY  ⚠️ partial — static HTML only
# ════════════════════════════════════════════════════════════════════

def collect_workday(company: str, workday_url: str,
                    max_signals: int = 20) -> Tuple[List[LiveSignal], dict]:
    sigs, m = collect_careers_page(company, workday_url, max_signals)
    for s in sigs:
        s.source_type      = "workday_partial"
        s.confidence_score = min(s.confidence_score, 0.58)
        s.parser_used      = "workday_static_html"
        s.extraction_method= "html_selector_partial"
    return sigs, {**m, "source": "workday_partial",
                  "limitation": "JS-rendered listings not accessible without Playwright"}


# ════════════════════════════════════════════════════════════════════
# iCIMS  ⚠️ partial
# ════════════════════════════════════════════════════════════════════

def collect_icims(company: str, icims_url: str,
                  max_signals: int = 15) -> Tuple[List[LiveSignal], dict]:
    sigs, m = collect_careers_page(company, icims_url, max_signals)
    for s in sigs:
        s.source_type      = "icims_partial"
        s.confidence_score = min(s.confidence_score, 0.52)
        s.parser_used      = "icims_static_html"
        s.extraction_method= "html_selector_partial"
    return sigs, {**m, "source": "icims_partial",
                  "limitation": "Client-side rendered; Playwright needed for full listings"}


# ════════════════════════════════════════════════════════════════════
# NEWSROOM  ✅ static HTML
# ════════════════════════════════════════════════════════════════════

def collect_newsroom(company: str, newsroom_url: str,
                     max_signals: int = 12) -> Tuple[List[LiveSignal], dict]:
    meta: dict = {
        "source": "newsroom",
        "url": newsroom_url,
        "raw_candidates": 0,
        "noise_filtered": 0,
        "relevant_signals": 0,
    }
    html = fetch_html(newsroom_url, source_name="newsroom")
    if not html:
        meta["access_status"] = FetchLog.entries[-1].status if FetchLog.entries else "error"
        return [], meta

    meta["access_status"] = "success"
    soup = parse_html(html)
    signals, seen = [], set()

    candidates = (
        soup.find_all("article") or
        soup.find_all("div", class_=re.compile(r"news|press|article|release|story", re.I)) or
        soup.find_all("li",  class_=re.compile(r"news|press|release", re.I))
    )
    meta["raw_candidates"] = len(candidates)

    for item in candidates[:35]:
        title_el = item.find(["h1","h2","h3","h4","a"])
        if not title_el: continue
        title = title_el.get_text(strip=True)
        if len(title) < 15 or len(title) > 220: continue

        excerpt_el = item.find(["p","div"], class_=re.compile(r"excerpt|summary|desc|body|text", re.I))
        if not excerpt_el: excerpt_el = item.find("p")
        excerpt = excerpt_el.get_text(strip=True)[:300] if excerpt_el else ""

        href = item.find("a", href=True)
        article_url = urljoin(newsroom_url, href["href"]) if href else newsroom_url
        full = f"{title} {excerpt}"

        if not is_relevant(full): meta["noise_filtered"] += 1; continue
        h = dedup_hash(article_url, title)
        if h in seen: continue
        seen.add(h)

        kws = extract_keywords(full)
        tl  = title.lower()
        if any(x in tl for x in ["appoint","hire","join","named","cio","cto","new vp"]):
            sig_type = "timing"
        elif any(x in tl for x in ["acqui","merger","partner"]): sig_type = "timing"
        elif any(x in tl for x in ["migrat","transform","modern","cloud"]): sig_type = "transformation"
        else: sig_type = "pain"

        sig = LiveSignal(
            company_name=company, source_url=article_url,
            source_type="newsroom",
            date_found=datetime.utcnow().strftime("%Y-%m-%d"),
            signal_type=sig_type,
            raw_snippet=f"{title} | {excerpt[:250]}",
            extracted_keywords=kws, confidence_score=0.72,
            state_detected=detect_state(full),
            city_detected=detect_city(full),
            enterprise_relevance_score=enterprise_relevance(kws),
            live_collected=True, source_access_status="success",
            parser_used="html_scrape", extraction_method="html_selector",
            data_source="LIVE", dedup_id=h,
        )
        signals.append(sig)
        if len(signals) >= max_signals: break

    meta["relevant_signals"] = len(signals)
    logger.info(f"[Newsroom] {company}: {meta['raw_candidates']} candidates → "
                f"{meta['noise_filtered']} filtered → {len(signals)} relevant")
    return signals, meta


# ════════════════════════════════════════════════════════════════════
# GOOGLE NEWS RSS  ✅ public RSS feed
# ════════════════════════════════════════════════════════════════════

def collect_google_news(company: str, news_terms: Optional[List[str]] = None,
                        max_signals: int = 18) -> Tuple[List[LiveSignal], dict]:
    import re as _re
    from email.utils import parsedate_to_datetime

    terms = news_terms or ["Oracle database migration", "database modernization", "cloud transformation CIO"]
    meta: dict = {
        "source": "google_news_rss",
        "queries": [],
        "raw_items_fetched": 0,
        "noise_filtered": 0,
        "relevant_signals": 0,
    }
    signals, seen = [], set()

    def _parse_date(raw: str) -> str:
        if not raw: return datetime.utcnow().strftime("%Y-%m-%d")
        try: return parsedate_to_datetime(raw).strftime("%Y-%m-%d")
        except Exception: pass
        m = _re.search(r'(\d{4}-\d{2}-\d{2})', raw)
        return m.group(1) if m else datetime.utcnow().strftime("%Y-%m-%d")

    for term in terms[:3]:
        query = f'"{company}" ({term})'
        url   = f"https://news.google.com/rss/search?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
        meta["queries"].append(url)

        html = fetch_html(url, source_name="google_news_rss", timeout=10)
        if not html:
            logger.debug(f"[GoogleNews] No response: {company} + {term}")
            continue

        try:
            soup = BeautifulSoup(html, "lxml-xml")
            items = soup.find_all("item")
            if not items:
                soup = BeautifulSoup(html, "lxml")
                items = soup.find_all("item")

            meta["raw_items_fetched"] += len(items)
            logger.info(f"[GoogleNews] {company}+'{term}': {len(items)} items")

            for item in items[:6]:
                title   = (item.find("title")  or type('x', (), {'get_text': lambda s,**k: ''})()).get_text(strip=True)
                desc    = (item.find("description") or type('x', (), {'get_text': lambda s,**k: ''})()).get_text(strip=True)
                link    = (item.find("link")    or type('x', (), {'get_text': lambda s,**k: url})()).get_text(strip=True)
                pub_raw = (item.find("pubDate") or type('x', (), {'get_text': lambda s,**k: ''})()).get_text(strip=True)
                pub_date = _parse_date(pub_raw)
                full = f"{title} {desc}"
                if not is_relevant(full): meta["noise_filtered"] += 1; continue
                h = dedup_hash(link, title)
                if h in seen: continue
                seen.add(h)
                kws  = extract_keywords(full)
                tl   = title.lower()
                if any(x in tl for x in ["appoint","named","cio","cto","new vp","hire"]): stype = "timing"
                elif any(x in tl for x in ["acqui","merger"]): stype = "timing"
                elif any(x in tl for x in ["migrat","transform","modern","cloud"]): stype = "transformation"
                else: stype = "news"
                sig = LiveSignal(
                    company_name=company, source_url=link,
                    source_type="google_news_rss", date_found=pub_date,
                    signal_type=stype,
                    raw_snippet=f"{title} | {desc[:250]}",
                    extracted_keywords=kws, confidence_score=0.67,
                    state_detected=detect_state(full),
                    city_detected=detect_city(full),
                    enterprise_relevance_score=enterprise_relevance(kws),
                    live_collected=True, source_access_status="success",
                    parser_used="rss_xml", extraction_method="rss_feed",
                    data_source="LIVE", dedup_id=h,
                )
                signals.append(sig)
        except Exception as e:
            logger.warning(f"[GoogleNews] Parse error for {company}+{term}: {e}")

        if len(signals) >= max_signals: break

    meta["relevant_signals"] = len(signals)
    logger.info(f"[GoogleNews] {company}: {meta['raw_items_fetched']} raw → "
                f"{meta['noise_filtered']} filtered → {len(signals)} relevant")
    return signals, meta


# ════════════════════════════════════════════════════════════════════
# IR PAGE  ❌ variable layout
# ════════════════════════════════════════════════════════════════════

def collect_ir_page(company: str, ir_url: str,
                    max_signals: int = 8) -> Tuple[List[LiveSignal], dict]:
    sigs, m = collect_newsroom(company, ir_url, max_signals)
    for s in sigs:
        s.source_type = "ir_page"
        s.parser_used = "ir_html_scrape"
    return sigs, {**m, "source": "ir_page",
                  "limitation": "variable layout — best-effort newsroom scraper"}


# ════════════════════════════════════════════════════════════════════
# SEC EDGAR  ⚠️ partial
# ════════════════════════════════════════════════════════════════════

def collect_sec_edgar(company: str, ticker: Optional[str] = None,
                      max_signals: int = 5) -> Tuple[List[LiveSignal], dict]:
    meta: dict = {
        "source": "sec_edgar",
        "ticker": ticker,
        "relevant_signals": 0,
        "limitation": "filing index only; full 10-K/10-Q text parsing = Phase 3",
    }
    if not ticker:
        meta["access_status"] = "no_ticker"
        return [], meta

    url  = (f"https://efts.sec.gov/LATEST/search-index?q=%22Oracle+database%22"
            f"&forms=10-K,10-Q&dateRange=custom&startdt=2024-01-01"
            f"&entity={quote_plus(company)}")
    meta["endpoint"] = url
    data = fetch_json(url, source_name="sec_edgar")
    if not data:
        meta["access_status"] = FetchLog.entries[-1].status if FetchLog.entries else "error"
        return [], meta

    meta["access_status"] = "success"
    signals, seen = [], set()
    hits = (data.get("hits") or {}).get("hits", [])
    for hit in hits[:max_signals]:
        src      = hit.get("_source", {})
        file_url = f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={src.get('entity_id','')}"
        title    = f"SEC {src.get('form_type','10-K')}: {src.get('period_of_report','')} — {company}"
        excerpt  = src.get("description","")[:200]
        kws      = extract_keywords(f"{title} {excerpt} oracle database")
        h        = dedup_hash(file_url, title)
        if h in seen: continue
        seen.add(h)
        sig = LiveSignal(
            company_name=company, source_url=file_url,
            source_type="sec_edgar",
            date_found=src.get("file_date", datetime.utcnow().strftime("%Y-%m-%d")),
            signal_type="transformation",
            raw_snippet=f"{title} | {excerpt}",
            extracted_keywords=kws, confidence_score=0.55,
            enterprise_relevance_score=enterprise_relevance(kws),
            live_collected=True, source_access_status="success",
            parser_used="sec_json_index", extraction_method="structured_api",
            data_source="LIVE", dedup_id=h,
        )
        signals.append(sig)

    meta["relevant_signals"] = len(signals)
    logger.info(f"[SEC] {company}: {len(signals)} filing signals")
    return signals, meta


# ════════════════════════════════════════════════════════════════════
# CONFERENCE SPEAKERS  ❌ Phase 3
# ════════════════════════════════════════════════════════════════════

def collect_conference_speakers(company: str) -> Tuple[List[LiveSignal], dict]:
    return [], {"source": "conference_speakers", "relevant_signals": 0,
                "status": "not_implemented", "phase": 3}


# ════════════════════════════════════════════════════════════════════
# MASTER COLLECTOR
# ════════════════════════════════════════════════════════════════════

def collect_all(
    company:         str,
    greenhouse_slug: Optional[str]       = None,
    lever_slug:      Optional[str]       = None,
    careers_url:     Optional[str]       = None,
    workday_url:     Optional[str]       = None,
    icims_url:       Optional[str]       = None,
    newsroom_url:    Optional[str]       = None,
    ir_url:          Optional[str]       = None,
    ticker:          Optional[str]       = None,
    news_terms:      Optional[List[str]] = None,
) -> dict:
    all_signals: List[LiveSignal] = []
    per_source:  dict             = {}

    # All 10 sources in priority order from the spec
    if careers_url:
        s, m = collect_careers_page(company, careers_url); all_signals.extend(s); per_source["careers_page"] = m
    if workday_url:
        s, m = collect_workday(company, workday_url);      all_signals.extend(s); per_source["workday"]      = m
    s, m = collect_greenhouse(company, greenhouse_slug);   all_signals.extend(s); per_source["greenhouse"]   = m
    s, m = collect_lever(company, lever_slug);             all_signals.extend(s); per_source["lever"]        = m
    if icims_url:
        s, m = collect_icims(company, icims_url);          all_signals.extend(s); per_source["icims"]        = m
    if newsroom_url:
        s, m = collect_newsroom(company, newsroom_url);    all_signals.extend(s); per_source["newsroom"]     = m
    s, m = collect_google_news(company, news_terms);       all_signals.extend(s); per_source["google_news"]  = m
    if ir_url:
        s, m = collect_ir_page(company, ir_url);           all_signals.extend(s); per_source["ir_page"]      = m
    if ticker:
        s, m = collect_sec_edgar(company, ticker);         all_signals.extend(s); per_source["sec_edgar"]    = m
    s, m = collect_conference_speakers(company);           all_signals.extend(s); per_source["conference"]   = m

    # Dedup
    seen, deduped = set(), []
    for sig in all_signals:
        if sig.dedup_id not in seen:
            seen.add(sig.dedup_id); deduped.append(sig)

    total_raw           = len(all_signals)
    false_positives     = total_raw - len(deduped)   # from dedup only (noise already filtered per-collector)

    return {
        "company":                company,
        "total_raw":              total_raw,
        "false_positives_removed":false_positives,
        "total_after_dedup":      len(deduped),
        "per_source":             per_source,
        "signals":                deduped,
    }
