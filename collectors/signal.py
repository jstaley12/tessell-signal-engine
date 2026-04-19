"""
collectors/signal.py
Defines LiveSignal with all Phase 2 explicit labeling fields.
Every signal tells you exactly where it came from and how confident to be.
"""
from __future__ import annotations
import re, hashlib
from dataclasses import dataclass, field
from typing import List, Optional

# ── Keyword pools ─────────────────────────────────────────────────────────────

TESSELL_KEYWORDS = [
    "oracle","sql server","mssql","postgresql","postgres","mysql","mongodb",
    "mariadb","aurora","rds","db2","cassandra","teradata","azure sql",
    "database administrator","dba","database engineer","database reliability",
    "dbre","database platform","database operations","database architect",
    "database devops","database automation","database cloning",
    "database provisioning","database migration","database modernization",
    "database-as-a-service","dbaas","copy data","non-production database",
    "non-prod","dev/test environment","environment refresh",
    "cloud migration","cloud transformation","data center exit",
    "data center migration","platform engineering","site reliability",
    "sre","infrastructure engineer","devops engineer",
    "backup","disaster recovery","high availability","failover","rpo","rto",
    "dba toil","manual dba","dba overhead","oracle cost","oracle licensing",
    "license cost","cost optimization","oracle exadata","oracle rac",
    "new cio","new cto","new vp infrastructure","acquisition","merger",
    "digital transformation","erp migration","sap migration",
]

RELEVANT_TITLE_WORDS = [
    "database","dba","dbre","oracle","sql server","postgres","mysql","mongodb",
    "platform engineer","site reliability","sre","cloud engineer",
    "infrastructure engineer","data platform","database architect",
    "database reliability","database operations","database platform",
]

HIGH_VALUE_KWS = {
    "oracle","oracle dba","oracle rac","oracle exadata","oracle licensing",
    "oracle cost","dbre","database reliability","database platform",
    "database-as-a-service","dbaas","non-production database","dba toil",
    "database cloning","database automation","copy data",
}

NOISE_PHRASES = [
    "restaurant","cashier","retail associate","delivery driver","forklift",
    "customer service representative","sales associate","hair stylist",
    "dental assistant","small business","no experience required",
    "work from home data entry","call center agent",
]

def is_relevant(text: str) -> bool:
    t = text.lower()
    if any(n in t for n in NOISE_PHRASES): return False
    return any(k in t for k in TESSELL_KEYWORDS)

def is_relevant_title(title: str) -> bool:
    return any(k in title.lower() for k in RELEVANT_TITLE_WORDS)

def extract_keywords(text: str) -> List[str]:
    t = text.lower()
    return list(dict.fromkeys(k for k in TESSELL_KEYWORDS if k in t))

def enterprise_relevance(kws: List[str]) -> float:
    h = sum(1 for k in kws if k in HIGH_VALUE_KWS)
    m = sum(1 for k in kws if k not in HIGH_VALUE_KWS)
    return round(min(1.0, h * 0.15 + m * 0.06 + len(kws) * 0.02), 2)

# ── Geography ─────────────────────────────────────────────────────────────────

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}
STATE_NAMES = {
    "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR","california":"CA",
    "colorado":"CO","connecticut":"CT","delaware":"DE","florida":"FL","georgia":"GA",
    "hawaii":"HI","idaho":"ID","illinois":"IL","indiana":"IN","iowa":"IA",
    "kansas":"KS","kentucky":"KY","louisiana":"LA","maine":"ME","maryland":"MD",
    "massachusetts":"MA","michigan":"MI","minnesota":"MN","mississippi":"MS",
    "missouri":"MO","montana":"MT","nebraska":"NE","nevada":"NV",
    "new hampshire":"NH","new jersey":"NJ","new mexico":"NM","new york":"NY",
    "north carolina":"NC","north dakota":"ND","ohio":"OH","oklahoma":"OK",
    "oregon":"OR","pennsylvania":"PA","rhode island":"RI","south carolina":"SC",
    "south dakota":"SD","tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT",
    "virginia":"VA","washington":"WA","west virginia":"WV","wisconsin":"WI",
    "wyoming":"WY","district of columbia":"DC",
}
CITY_STATE = {
    "dallas":"TX","fort worth":"TX","houston":"TX","austin":"TX","san antonio":"TX",
    "irving":"TX","plano":"TX","frisco":"TX","richardson":"TX","arlington":"TX",
    "mckinney":"TX","garland":"TX","mesquite":"TX","grand prairie":"TX",
    "lewisville":"TX","denton":"TX","carrollton":"TX","sugar land":"TX",
    "the woodlands":"TX","katy":"TX","pearland":"TX","round rock":"TX",
    "tulsa":"OK","oklahoma city":"OK","edmond":"OK","norman":"OK","broken arrow":"OK",
    "wichita":"KS","overland park":"KS","kansas city":"KS","topeka":"KS","olathe":"KS",
    "chicago":"IL","new york":"NY","los angeles":"CA","san francisco":"CA",
    "seattle":"WA","atlanta":"GA","miami":"FL","boston":"MA","denver":"CO",
    "phoenix":"AZ","minneapolis":"MN","detroit":"MI","columbus":"OH",
    "indianapolis":"IN","nashville":"TN","charlotte":"NC","raleigh":"NC",
    "memphis":"TN","louisville":"KY","new orleans":"LA","omaha":"NE",
    "little rock":"AR","lowell":"AR","richmond":"VA","washington":"DC",
    "baltimore":"MD","pittsburgh":"PA","philadelphia":"PA","cincinnati":"OH",
    "cleveland":"OH","milwaukee":"WI","st. louis":"MO","st louis":"MO",
    "salt lake city":"UT","albuquerque":"NM","portland":"OR","las vegas":"NV",
    "sacramento":"CA","san jose":"CA","san diego":"CA","el paso":"TX",
    "corpus christi":"TX","lubbock":"TX","amarillo":"TX","waco":"TX",
    "goodlettsville":"TN","teaneck":"NJ","minnetonka":"MN","eden prairie":"MN",
    "palo alto":"CA","menlo park":"CA","mountain view":"CA","sunnyvale":"CA",
    "brentwood":"TN","franklin":"TN","parsippany":"NJ",
}

def detect_state(text: str) -> Optional[str]:
    t = " " + text + " "
    m = re.search(r'\b([A-Za-z][a-z]+(?:\s[A-Za-z][a-z]+)?),\s*([A-Z]{2})\b', t)
    if m and m.group(2) in US_STATES: return m.group(2)
    tl = t.lower()
    for name, abbr in STATE_NAMES.items():
        if re.search(r'\b' + re.escape(name) + r'\b', tl): return abbr
    for city, state in CITY_STATE.items():
        if city in tl: return state
    for m2 in re.finditer(r'(?<![A-Za-z])([A-Z]{2})(?![A-Za-z])', t):
        if m2.group(1) in US_STATES: return m2.group(1)
    return None

def detect_city(text: str) -> Optional[str]:
    tl = text.lower()
    for city in CITY_STATE:
        if city in tl: return city.title()
    m = re.search(r'\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)?),\s*[A-Z]{2}\b', text)
    return m.group(1) if m else None

def infer_buyer(title: str) -> str:
    t = title.lower()
    if any(x in t for x in ["cio","cto","chief information","chief technology"]): return "C-Suite"
    if any(x in t for x in ["vp ","vice president"]): return "VP"
    if "director" in t: return "Director"
    if any(x in t for x in ["manager","head of"]): return "Manager"
    if any(x in t for x in ["dba","database admin","database engineer","dbre"]): return "DBA/DBRE"
    if any(x in t for x in ["platform engineer","site reliability","sre"]): return "Platform/SRE"
    if "architect" in t: return "Architect"
    if any(x in t for x in ["infrastructure","cloud engineer"]): return "Infrastructure"
    return "Technical IC"

def dedup_hash(url: str, title: str) -> str:
    return hashlib.sha256(f"{url}||{title}".encode()).hexdigest()[:20]


# ── LiveSignal — all Phase 2 explicit labeling fields ────────────────────────

@dataclass
class LiveSignal:
    # Core content
    company_name:               str
    source_url:                 str
    source_type:                str      # greenhouse_api | lever_api | careers_page_html | newsroom | google_news_rss | ...
    date_found:                 str      # YYYY-MM-DD
    signal_type:                str      # hiring | transformation | timing | pain | news
    raw_snippet:                str      # title + location + excerpt, max 400 chars

    # Extracted intelligence
    extracted_keywords:         List[str] = field(default_factory=list)
    confidence_score:           float = 0.7
    state_detected:             Optional[str] = None
    city_detected:              Optional[str] = None
    likely_buyer_function:      str = ""
    enterprise_relevance_score: float = 0.0

    # ── Phase 2 explicit labeling ─────────────────────────────────
    live_collected:       bool  = True   # True = came from real HTTP; False = fixture/demo
    source_access_status: str  = ""      # "success" | "robots_blocked" | "http_403" | "http_404" | "timeout" | "no_slug" | "parse_empty"
    parser_used:          str  = ""      # "greenhouse_json" | "lever_json" | "jsonld" | "html_elements" | "text_scan" | "rss_xml" | "html_scrape"
    extraction_method:    str  = ""      # "structured_api" | "jsonld_schema" | "html_selector" | "text_regex" | "rss_feed"

    # Dedup
    data_source:  str = "LIVE"           # Always LIVE in Phase 2; "MANUAL_TEST_FIXTURE" in Phase 1
    dedup_id:     str = ""

    def to_dict(self) -> dict:
        return {
            "company_name":               self.company_name,
            "source_url":                 self.source_url,
            "source_type":                self.source_type,
            "date_found":                 self.date_found,
            "signal_type":                self.signal_type,
            "raw_snippet":                self.raw_snippet[:400],
            "extracted_keywords":         self.extracted_keywords,
            "confidence_score":           round(self.confidence_score, 2),
            "state_detected":             self.state_detected,
            "city_detected":              self.city_detected,
            "likely_buyer_function":      self.likely_buyer_function,
            "enterprise_relevance_score": self.enterprise_relevance_score,
            # Phase 2 explicit labels
            "live_collected":             self.live_collected,
            "source_access_status":       self.source_access_status,
            "parser_used":                self.parser_used,
            "extraction_method":          self.extraction_method,
            "data_source":                self.data_source,
            "dedup_id":                   self.dedup_id,
        }
