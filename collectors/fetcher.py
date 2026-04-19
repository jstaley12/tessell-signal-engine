"""
collectors/fetcher.py
Shared HTTP layer. Every request is logged with URL, result, and timing.
Call log is accessible via FetchLog.entries for the post-run source quality report.
"""
import re, time, hashlib
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup
from loguru import logger

try:
    from fake_useragent import UserAgent as _UA
    _ua_pool = _UA()
    def _ua() -> str:
        try:    return _ua_pool.random
        except: return "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
except Exception:
    def _ua() -> str:
        return "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"


# ── Fetch log — every URL attempted is recorded ──────────────────────────────

@dataclass
class FetchEntry:
    url:         str
    domain:      str
    status:      str          # success | robots_blocked | http_403 | http_404 | http_NNN | timeout | error | no_slug
    http_code:   Optional[int]
    elapsed_ms:  int
    source_name: str          # which collector called this
    content_len: int = 0
    note:        str = ""

class FetchLog:
    entries: List[FetchEntry] = []

    @classmethod
    def record(cls, entry: FetchEntry):
        cls.entries.append(entry)

    @classmethod
    def reset(cls):
        cls.entries = []

    @classmethod
    def summary(cls) -> dict:
        if not cls.entries:
            return {}
        by_status: Dict[str, int] = {}
        by_source: Dict[str, Dict] = {}
        for e in cls.entries:
            by_status[e.status] = by_status.get(e.status, 0) + 1
            if e.source_name not in by_source:
                by_source[e.source_name] = {"tried":0,"success":0,"blocked":0,"failed":0}
            by_source[e.source_name]["tried"] += 1
            if e.status == "success":       by_source[e.source_name]["success"] += 1
            elif "blocked" in e.status:     by_source[e.source_name]["blocked"] += 1
            else:                           by_source[e.source_name]["failed"]  += 1
        return {
            "total_urls_tried":   len(cls.entries),
            "by_status":          by_status,
            "by_source":          by_source,
            "success_rate":       round(by_status.get("success",0)/len(cls.entries)*100, 1) if cls.entries else 0,
        }

    @classmethod
    def failed_urls(cls) -> List[dict]:
        return [
            {"url":e.url,"status":e.status,"source":e.source_name,"note":e.note}
            for e in cls.entries if e.status != "success"
        ]

    @classmethod
    def successful_urls(cls) -> List[dict]:
        return [
            {"url":e.url,"source":e.source_name,"content_bytes":e.content_len,"elapsed_ms":e.elapsed_ms}
            for e in cls.entries if e.status == "success"
        ]


# ── robots.txt cache ──────────────────────────────────────────────────────────

_robots_cache: Dict[str, Optional[RobotFileParser]] = {}

def _robots_ok(url: str) -> bool:
    domain = urlparse(url).netloc
    if domain not in _robots_cache:
        rp = RobotFileParser()
        try:
            rp.set_url(f"https://{domain}/robots.txt")
            rp.read()
            _robots_cache[domain] = rp
            logger.debug(f"robots.txt loaded for {domain}")
        except Exception:
            _robots_cache[domain] = None
    rp = _robots_cache[domain]
    return True if rp is None else rp.can_fetch("*", url)


# ── Rate limiter ──────────────────────────────────────────────────────────────

_last_request: Dict[str, float] = {}

def _rate_limit(domain: str, delay: float = 2.5):
    wait = delay - (time.time() - _last_request.get(domain, 0))
    if wait > 0:
        time.sleep(wait)
    _last_request[domain] = time.time()


# ── Core fetch ────────────────────────────────────────────────────────────────

def fetch_html(url: str, source_name: str = "unknown",
               timeout: int = 15, retries: int = 3,
               respect_robots: bool = True) -> Optional[str]:
    """
    Fetch URL. Logs every attempt to FetchLog. Returns HTML or None.
    Checks robots.txt first. Rate-limits per domain. Retries on 429/timeout.
    """
    domain = urlparse(url).netloc

    if respect_robots and not _robots_ok(url):
        FetchLog.record(FetchEntry(
            url=url, domain=domain, status="robots_blocked",
            http_code=None, elapsed_ms=0, source_name=source_name,
            note="robots.txt disallows this path",
        ))
        logger.debug(f"[{source_name}] robots_blocked: {url}")
        return None

    _rate_limit(domain)

    headers = {
        "User-Agent":      _ua(),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection":      "keep-alive",
    }

    for attempt in range(retries):
        t0 = time.time()
        try:
            r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
            elapsed = int((time.time() - t0) * 1000)
            if r.status_code == 200:
                FetchLog.record(FetchEntry(
                    url=url, domain=domain, status="success",
                    http_code=200, elapsed_ms=elapsed, source_name=source_name,
                    content_len=len(r.text),
                ))
                logger.debug(f"[{source_name}] 200 OK {url} ({elapsed}ms, {len(r.text):,} chars)")
                return r.text

            if r.status_code == 429:
                wait = 4 ** (attempt + 1)
                logger.warning(f"[{source_name}] 429 rate-limit on {domain}, waiting {wait}s")
                time.sleep(wait)
                continue

            status_key = f"http_{r.status_code}"
            FetchLog.record(FetchEntry(
                url=url, domain=domain, status=status_key,
                http_code=r.status_code, elapsed_ms=elapsed, source_name=source_name,
                note=r.text[:120].strip() if r.status_code == 403 else "",
            ))
            logger.info(f"[{source_name}] HTTP {r.status_code}: {url}")
            return None

        except requests.exceptions.Timeout:
            elapsed = int((time.time() - t0) * 1000)
            logger.warning(f"[{source_name}] Timeout attempt {attempt+1}: {url}")
            if attempt == retries - 1:
                FetchLog.record(FetchEntry(
                    url=url, domain=domain, status="timeout",
                    http_code=None, elapsed_ms=elapsed, source_name=source_name,
                ))
            time.sleep(2 ** attempt)

        except requests.exceptions.ConnectionError as e:
            elapsed = int((time.time() - t0) * 1000)
            FetchLog.record(FetchEntry(
                url=url, domain=domain, status="connection_error",
                http_code=None, elapsed_ms=elapsed, source_name=source_name,
                note=str(e)[:100],
            ))
            logger.warning(f"[{source_name}] Connection error: {url} — {e}")
            return None

        except Exception as e:
            elapsed = int((time.time() - t0) * 1000)
            if attempt == retries - 1:
                FetchLog.record(FetchEntry(
                    url=url, domain=domain, status="error",
                    http_code=None, elapsed_ms=elapsed, source_name=source_name,
                    note=str(e)[:100],
                ))
            logger.warning(f"[{source_name}] Error attempt {attempt+1}: {url} — {e}")
            time.sleep(2 ** attempt)

    return None


def fetch_json(url: str, source_name: str = "unknown",
               timeout: int = 12) -> Optional[dict]:
    """Fetch JSON endpoint. Logs to FetchLog."""
    domain = urlparse(url).netloc
    _rate_limit(domain)
    headers = {
        "User-Agent": _ua(),
        "Accept":     "application/json, */*;q=0.8",
    }
    t0 = time.time()
    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        elapsed = int((time.time() - t0) * 1000)
        if r.status_code == 200:
            data = r.json()
            FetchLog.record(FetchEntry(
                url=url, domain=domain, status="success",
                http_code=200, elapsed_ms=elapsed, source_name=source_name,
                content_len=len(r.text),
            ))
            logger.debug(f"[{source_name}] JSON 200 OK {url} ({elapsed}ms)")
            return data
        FetchLog.record(FetchEntry(
            url=url, domain=domain, status=f"http_{r.status_code}",
            http_code=r.status_code, elapsed_ms=elapsed, source_name=source_name,
            note=r.text[:120].strip(),
        ))
        logger.info(f"[{source_name}] JSON HTTP {r.status_code}: {url}")
    except Exception as e:
        elapsed = int((time.time() - t0) * 1000)
        FetchLog.record(FetchEntry(
            url=url, domain=domain, status="error",
            http_code=None, elapsed_ms=elapsed, source_name=source_name,
            note=str(e)[:100],
        ))
        logger.warning(f"[{source_name}] JSON error: {url} — {e}")
    return None


def parse_html(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


def dedup_hash(url: str, title: str) -> str:
    return hashlib.sha256(f"{url}||{title}".encode()).hexdigest()[:20]
