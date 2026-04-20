"""
collectors/signal.py  — v3  Signal Taxonomy Engine
════════════════════════════════════════════════════════════════════
Every signal is classified into a tier that directly drives scoring weight.

Tier 1 — Immediate Buyer Motion  (15 pts each, max 45)
  A rep should call this week. Direct evidence of active DB pain or spend.

Tier 2 — Contextual Pain  (5 pts each, max 20)
  Confirms operational complexity. Supports Tier 1 signals.

Tier 3 — Minor Context Only  (1 pt each, max 5)
  Background info. Cannot push a company into top 10 alone.

Rule: 1 Tier 1 signal outweighs 15 Tier 3 signals.
No company should rank top 10 from Tier 3 alone.
"""
from __future__ import annotations
import re
import hashlib
from dataclasses import dataclass, field
from typing import List, Optional
from datetime import datetime, timedelta

# ════════════════════════════════════════════════════════════════════
# TIER 1 — Immediate Buyer Motion
# ════════════════════════════════════════════════════════════════════

# Job title patterns that indicate active DB pain (Oracle/SQL/Postgres DBA hiring)
T1_DB_HIRE_PATTERNS = [
    r'oracle\s+dba',
    r'oracle\s+database\s+admin',
    r'sr\.?\s+oracle',
    r'lead\s+oracle',
    r'principal\s+oracle',
    r'sql\s+server\s+dba',
    r'sql\s+server\s+database\s+admin',
    r'postgres(?:ql)?\s+dba',
    r'postgres(?:ql)?\s+architect',
    r'database\s+reliability\s+engineer',
    r'\bdbre\b',
    r'database\s+platform\s+engineer',
    r'database\s+architect',
    r'database\s+devops',
    r'database\s+automation\s+engineer',
    r'senior\s+dba',
    r'staff\s+dba',
    r'principal\s+dba',
    r'lead\s+dba',
]

# Leadership change — new CIO/CTO creates budget + mandate window
T1_LEADERSHIP_PATTERNS = [
    r'new\s+cio\b',
    r'new\s+cto\b',
    r'new\s+vp\s+(?:of\s+)?(?:infrastructure|engineering|technology|it\b)',
    r'new\s+chief\s+(?:information|technology)',
    r'named\s+(?:as\s+)?cio\b',
    r'named\s+(?:as\s+)?cto\b',
    r'appointed\s+(?:as\s+)?cio\b',
    r'appointed\s+(?:as\s+)?cto\b',
    r'joins\s+as\s+cio\b',
    r'joins\s+as\s+cto\b',
    r'hired\s+as\s+cio\b',
    r'new\s+head\s+of\s+(?:infrastructure|engineering|technology)',
    r'svp\s+(?:of\s+)?(?:technology|infrastructure|it)\b',
]

# Cloud migration — active initiative = active spend
T1_CLOUD_MIGRATION_PATTERNS = [
    r'cloud\s+migration\s+(?:program|initiative|project|strategy)',
    r'migrating\s+(?:to\s+)?(?:aws|azure|gcp|cloud)',
    r'data\s+center\s+(?:exit|migration|consolidation|closure)',
    r'move\s+to\s+(?:aws|azure|gcp|cloud)',
    r'oracle\s+(?:to\s+)?cloud\s+migration',
    r'oracle\s+migration\s+(?:project|program|initiative)',
    r'legacy\s+(?:system|database|app)\s+(?:migration|modernization)',
    r'erp\s+(?:migration|modernization|upgrade)',
    r'sap\s+(?:migration|s\/4hana)',
    r'platform\s+modernization\s+(?:initiative|program)',
    r'infrastructure\s+modernization',
]

# Outage / DR — pain is real and recent
T1_DR_OUTAGE_PATTERNS = [
    r'(?:system|database|service|it)\s+outage',
    r'(?:data\s+loss|data\s+corruption)',
    r'(?:recovery\s+time|rto|rpo)\s+(?:exceeded|missed|failed)',
    r'(?:disaster\s+recovery|failover)\s+(?:failed|tested|drill)',
    r'downtime\s+(?:incident|event|cost)',
    r'production\s+(?:outage|incident|failure)',
    r'database\s+(?:failure|crash|corruption)',
    r'backup\s+(?:failed|failure|restore)',
    r'high\s+availability\s+(?:failure|incident)',
]

# M&A — integration creates DB sprawl
T1_MA_PATTERNS = [
    r'(?:completes?|closes?)\s+acquisition',
    r'acquires?\s+',
    r'merger\s+(?:complete|close|integration)',
    r'post[\-\s]merger\s+integration',
    r'database\s+consolidation',
    r'it\s+integration\s+(?:post|following|after)',
    r'systems?\s+integration\s+(?:post|following)',
    r'carve[\-\s]out',
    r'divest(?:iture)?',
    r'spin[\-\s]off',
]

# Oracle cost/licensing pain — direct spend signal
T1_ORACLE_COST_PATTERNS = [
    r'oracle\s+licens(?:ing|e)\s+(?:cost|fee|renewal|audit)',
    r'oracle\s+(?:cost|spend)\s+(?:reduction|optimization|review)',
    r'reducing\s+oracle\s+(?:costs?|spend|licenses?)',
    r'oracle\s+audit',
    r'oracle\s+compliance',
    r'oracle\s+true[\-\s]up',
    r'oracle\s+(?:price|pricing)\s+increase',
    r'database\s+cost\s+(?:optimization|reduction)',
    r'cost\s+(?:optimization|reduction)\s+.*\bdatabase\b',
]

# Cost optimization mandate
T1_COST_MANDATE_PATTERNS = [
    r'cost\s+(?:reduction|optimization|cutting)\s+(?:program|initiative|mandate|target)',
    r'(?:reduce|cut|optimize)\s+it\s+(?:costs?|spend|budget)',
    r'finops\s+(?:initiative|program)',
    r'cloud\s+cost\s+(?:optimization|reduction)',
    r'technology\s+cost\s+(?:reduction|optimization)',
]

# ════════════════════════════════════════════════════════════════════
# TIER 2 — Contextual Pain
# ════════════════════════════════════════════════════════════════════

T2_INFRA_HIRE_PATTERNS = [
    r'platform\s+engineer(?:ing)?',
    r'site\s+reliability\s+engineer',
    r'\bsre\b',
    r'infrastructure\s+engineer',
    r'cloud\s+(?:engineer|architect|ops)',
    r'devops\s+engineer',
    r'database\s+(?:engineer|operations)',
    r'data\s+(?:infrastructure|platform)',
]

T2_MULTIREGION_PATTERNS = [
    r'multi[\-\s]region',
    r'multi[\-\s](?:cloud|dc|datacenter)',
    r'global\s+(?:operations|expansion|infrastructure)',
    r'(?:expan(?:d|sion)|grow(?:ing|th))\s+.*\b(?:state|region|market|location)s?\b',
    r'(?:new|additional)\s+(?:data\s+center|office|location|facility)',
    r'international\s+expansion',
    r'(?:opens?|opening)\s+(?:new\s+)?(?:office|facility|center)',
]

T2_REGULATED_PATTERNS = [
    r'(?:hipaa|hitech|ehr|emr)\s+(?:compliance|requirement)',
    r'(?:pci[\-\s]dss|pci\s+compliance)',
    r'(?:soc\s+2|fedramp|fisma)',
    r'(?:sox|sarbanes)',
    r'(?:gdpr|ccpa|data\s+privacy)\s+compliance',
    r'(?:fda|gxp|21\s+cfr)\s+(?:compliance|validation)',
    r'(?:regulatory|compliance)\s+(?:audit|requirement|mandate)',
    r'24[\s\/]7\s+(?:uptime|availability|operations)',
    r'(?:99\.9|99\.99|five\s+nines)',
    r'mission[\-\s]critical\s+(?:system|database|application)',
]

T2_BACKUP_DR_PATTERNS = [
    r'(?:disaster\s+recovery|dr)\s+(?:plan|strategy|capability)',
    r'backup\s+(?:strategy|solution|policy)',
    r'(?:high\s+availability|ha)\s+(?:solution|setup|requirement)',
    r'(?:rpo|rto)\s+(?:requirement|target|sla)',
    r'data\s+(?:protection|resilience)',
    r'business\s+continuity',
]

T2_MULTICLOUD_PATTERNS = [
    r'multi[\-\s]cloud\s+(?:strategy|approach|environment)',
    r'hybrid\s+cloud',
    r'(?:aws|azure|gcp)\s+and\s+(?:aws|azure|gcp)',
    r'cloud[\-\s]agnostic',
    r'cloud\s+portability',
]

# ════════════════════════════════════════════════════════════════════
# TIER 3 — Minor Context Only
# ════════════════════════════════════════════════════════════════════

# Generic DB mentions — interesting but not action-worthy
T3_GENERIC_DB_PATTERNS = [
    r'\boracle\b(?!\s+dba|\s+rac|\s+exadata|\s+licens|\s+cost|\s+migrat)',
    r'\bsql\s+server\b',
    r'\bpostgresql?\b',
    r'\bmysql\b',
    r'\bdatabase\b',
    r'\bdb2\b',
]

# Stale signal threshold
T3_STALE_DAYS = 60

# Sources that are inherently Tier 3
T3_SOURCES = {"sec_edgar", "state_seed_list"}

# ════════════════════════════════════════════════════════════════════
# TIER CLASSIFIER
# ════════════════════════════════════════════════════════════════════

SIGNAL_TIER_WEIGHTS = {1: 15, 2: 5, 3: 1}
SIGNAL_TIER_MAX_PTS = {1: 45, 2: 20, 3: 5}  # max contribution per tier


def _days_ago(date_str: str) -> int:
    """Returns how many days ago a date string was. -1 if unknown."""
    if not date_str:
        return -1
    try:
        d = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        delta = datetime.utcnow() - d.replace(tzinfo=None)
        return max(0, delta.days)
    except Exception:
        return -1


def _human_age(date_str: str) -> str:
    """Returns human-readable age like '4 days ago', '2 weeks ago'."""
    days = _days_ago(date_str)
    if days < 0:
        return "date unknown"
    if days == 0:
        return "today"
    if days == 1:
        return "1 day ago"
    if days < 7:
        return f"{days} days ago"
    if days < 14:
        return "1 week ago"
    if days < 30:
        return f"{days // 7} weeks ago"
    if days < 60:
        return "1 month ago"
    if days < 365:
        return f"{days // 30} months ago"
    return f"{days // 365} year{'s' if days > 730 else ''} ago"


def classify_signal_tier(
    source_type: str,
    signal_type: str,
    snippet: str,
    date_str: str,
    keywords: List[str],
) -> tuple:
    """
    Returns (tier: int, label: str, human_reason: str)

    tier:         1, 2, or 3
    label:        TIER_1_DB_HIRE, TIER_2_MULTIREGION, TIER_3_SEC, etc.
    human_reason: "Hiring Oracle DBA in Dallas (4 days ago)"
    """
    text = snippet.lower()
    days = _days_ago(date_str)
    age  = _human_age(date_str)

    # ── Tier 3 by source (SEC, seeds) — always Tier 3 regardless of content ──
    if source_type in T3_SOURCES:
        if source_type == "sec_edgar":
            return 3, "TIER_3_SEC", f"SEC filing mention ({age})"
        return 3, "TIER_3_SEED", f"Known enterprise anchor — no live signals yet"

    # ── Stale → Tier 3 ──────────────────────────────────────────────
    if days > T3_STALE_DAYS and days >= 0:
        return 3, "TIER_3_STALE", f"Signal older than {T3_STALE_DAYS} days ({age})"

    # ── TIER 1 checks (order = priority) ────────────────────────────

    # T1: Oracle/SQL/Postgres DBA hiring
    for pat in T1_DB_HIRE_PATTERNS:
        if re.search(pat, text):
            role = _extract_job_role(text)
            loc  = _extract_location(snippet)
            loc_str = f" in {loc}" if loc else ""
            return 1, "TIER_1_DB_HIRE", f"Hiring {role}{loc_str} ({age})"

    # T1: Leadership change
    for pat in T1_LEADERSHIP_PATTERNS:
        if re.search(pat, text):
            role = _extract_leadership_role(text)
            return 1, "TIER_1_LEADERSHIP", f"New {role} announced ({age})"

    # T1: Cloud migration
    for pat in T1_CLOUD_MIGRATION_PATTERNS:
        if re.search(pat, text):
            detail = _extract_migration_detail(text)
            return 1, "TIER_1_CLOUD_MIGRATION", f"Cloud/migration initiative: {detail} ({age})"

    # T1: Outage / DR event
    for pat in T1_DR_OUTAGE_PATTERNS:
        if re.search(pat, text):
            return 1, "TIER_1_DR_OUTAGE", f"DR/outage event reported ({age})"

    # T1: M&A
    for pat in T1_MA_PATTERNS:
        if re.search(pat, text):
            return 1, "TIER_1_MA", f"M&A activity — DB integration complexity ({age})"

    # T1: Oracle licensing/cost
    for pat in T1_ORACLE_COST_PATTERNS:
        if re.search(pat, text):
            return 1, "TIER_1_ORACLE_COST", f"Oracle licensing/cost pressure ({age})"

    # T1: Cost mandate
    for pat in T1_COST_MANDATE_PATTERNS:
        if re.search(pat, text):
            return 1, "TIER_1_COST_MANDATE", f"Cost optimization mandate ({age})"

    # ── TIER 2 checks ────────────────────────────────────────────────

    # T2: Platform/SRE/infra hiring (not specific DB but still relevant)
    for pat in T2_INFRA_HIRE_PATTERNS:
        if re.search(pat, text):
            role = _extract_job_role(text)
            loc  = _extract_location(snippet)
            loc_str = f" in {loc}" if loc else ""
            return 2, "TIER_2_INFRA_HIRE", f"Platform/SRE hiring: {role}{loc_str} ({age})"

    # T2: Multi-region expansion
    for pat in T2_MULTIREGION_PATTERNS:
        if re.search(pat, text):
            return 2, "TIER_2_MULTIREGION", f"Multi-region/expansion operations ({age})"

    # T2: Regulated compliance
    for pat in T2_REGULATED_PATTERNS:
        if re.search(pat, text):
            return 2, "TIER_2_REGULATED", f"Regulated/mission-critical ops ({age})"

    # T2: Backup/DR language
    for pat in T2_BACKUP_DR_PATTERNS:
        if re.search(pat, text):
            return 2, "TIER_2_DR_LANGUAGE", f"DR/resilience language ({age})"

    # T2: Multi-cloud
    for pat in T2_MULTICLOUD_PATTERNS:
        if re.search(pat, text):
            return 2, "TIER_2_MULTICLOUD", f"Multi-cloud strategy referenced ({age})"

    # ── TIER 3 default ───────────────────────────────────────────────
    # Has some keywords but nothing action-worthy
    kw_hit = next((k for k in keywords
                   if any(db in k.lower() for db in ["oracle","sql","postgres","database","dba"])),
                  None)
    if kw_hit:
        return 3, "TIER_3_GENERIC_MENTION", f"Generic DB mention: '{kw_hit}' ({age})"

    return 3, "TIER_3_NEWS_MENTION", f"News mention — no specific DB pain signal ({age})"


# ════════════════════════════════════════════════════════════════════
# EXTRACTION HELPERS
# ════════════════════════════════════════════════════════════════════

def _extract_job_role(text: str) -> str:
    """Extract a clean job role name from snippet text."""
    patterns = [
        r'((?:senior|sr\.?|lead|staff|principal|manager of)\s+(?:oracle\s+)?dba)',
        r'(oracle\s+database\s+administrator)',
        r'(oracle\s+dba)',
        r'(sql\s+server\s+dba)',
        r'(postgres(?:ql)?\s+dba)',
        r'(database\s+reliability\s+engineer)',
        r'(database\s+platform\s+engineer)',
        r'(database\s+architect)',
        r'(platform\s+engineer)',
        r'(site\s+reliability\s+engineer)',
        r'(\bsre\b)',
        r'(\bdba\b)',
    ]
    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            return m.group(1).strip().title()
    return "Database Engineer"


def _extract_location(snippet: str) -> Optional[str]:
    """Extract city, state from job snippet if present."""
    # "City, ST" pattern
    m = re.search(r'\b([A-Z][a-z]+(?:\s[A-Z][a-z]+)?),\s*([A-Z]{2})\b', snippet)
    if m:
        return f"{m.group(1)}, {m.group(2)}"
    # Known cities
    text_l = snippet.lower()
    from collectors.signal import CITY_STATE
    for city, state in CITY_STATE.items():
        if city in text_l:
            return f"{city.title()}, {state}"
    return None


def _extract_leadership_role(text: str) -> str:
    """Extract the leadership role title from snippet."""
    for role in ["CIO", "CTO", "SVP Technology", "VP Infrastructure",
                 "VP Engineering", "VP Technology", "Chief Information Officer",
                 "Chief Technology Officer", "Head of Infrastructure"]:
        if role.lower() in text:
            return role
    return "Technology Leader"


def _extract_migration_detail(text: str) -> str:
    """Extract migration target/type from snippet."""
    if "oracle" in text:     return "Oracle migration"
    if "erp" in text:        return "ERP migration"
    if "sap" in text:        return "SAP migration"
    if "data center" in text:return "data center exit"
    if "aws" in text:        return "AWS migration"
    if "azure" in text:      return "Azure migration"
    if "gcp" in text:        return "GCP migration"
    return "cloud migration"


# ════════════════════════════════════════════════════════════════════
# KEYWORD POOLS (unchanged — used by is_relevant, extract_keywords)
# ════════════════════════════════════════════════════════════════════

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


# ════════════════════════════════════════════════════════════════════
# GEOGRAPHY (unchanged)
# ════════════════════════════════════════════════════════════════════

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


# ════════════════════════════════════════════════════════════════════
# LiveSignal dataclass — now includes tier fields
# ════════════════════════════════════════════════════════════════════

@dataclass
class LiveSignal:
    company_name:               str
    source_url:                 str
    source_type:                str
    date_found:                 str
    signal_type:                str
    raw_snippet:                str

    extracted_keywords:         List[str] = field(default_factory=list)
    confidence_score:           float = 0.7
    state_detected:             Optional[str] = None
    city_detected:              Optional[str] = None
    likely_buyer_function:      str = ""
    enterprise_relevance_score: float = 0.0

    # Signal taxonomy — populated by classify_signal_tier()
    signal_tier:        int  = 3          # 1, 2, or 3
    tier_label:         str  = ""         # TIER_1_DB_HIRE, TIER_2_MULTIREGION, etc.
    human_reason:       str  = ""         # "Hiring Oracle DBA in Dallas (4 days ago)"

    # Phase 2 labels
    live_collected:       bool = True
    source_access_status: str  = ""
    parser_used:          str  = ""
    extraction_method:    str  = ""
    data_source:          str  = "LIVE"
    dedup_id:             str  = ""

    def __post_init__(self):
        """Auto-classify tier if not already set."""
        if not self.tier_label:
            tier, label, reason = classify_signal_tier(
                source_type=self.source_type,
                signal_type=self.signal_type,
                snippet=self.raw_snippet,
                date_str=self.date_found,
                keywords=self.extracted_keywords,
            )
            self.signal_tier   = tier
            self.tier_label    = label
            self.human_reason  = reason

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
            "signal_tier":                self.signal_tier,
            "tier_label":                 self.tier_label,
            "human_reason":               self.human_reason,
            "live_collected":             self.live_collected,
            "source_access_status":       self.source_access_status,
            "parser_used":                self.parser_used,
            "extraction_method":          self.extraction_method,
            "data_source":                self.data_source,
            "dedup_id":                   self.dedup_id,
        }
