"""
scoring/scorer.py  — v3  (Tessell buying signals first)
════════════════════════════════════════════════════════════════════
SCORING MODEL — rebuilt to rank by likelihood to buy Tessell NOW.

Score Architecture:
  Tier 1 — Immediate Buying Signals  (0-55)
    Oracle/DB operational pain:          0-20
    DBA / platform / SRE hiring surge:   0-15
    Backup / DR / automation signals:    0-10
    Cloud migration / modernization:     0-10

  Tier 2 — Urgency Multipliers  (0-25)
    CIO/CTO/VP Infra leadership change:  0-10
    M&A / acquisition complexity:        0-8
    Multi-region / rapid expansion:      0-4
    Security / compliance pressure:      0-3

  Tier 3 — Context Boosters  (0-20)
    Enterprise size / tier:              0-8   (gating signal, not primary)
    Industry fit:                        0-7
    Mission critical environment:        0-5

  Territory  (0-15)
    HQ in territory:                     0-6
    Office / hiring / signal in terr:    0-9

  Penalties (new)
    Stale signals only (>90d):           -10 max
    Fortune rank only, no signals:       -5
    DB technology vendor / seller:       -15

Heat Levels:
  HOT       ≥ 80   Call this week — strong buying signals
  WARM      ≥ 60   Prioritize — good signals, some timing
  WATCHLIST ≥ 40   Monitor — qualifies but weak signals
  COLD       < 40  Deprioritize
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta

# ════════════════════════════════════════════════════════════════════
# TIER 1 KEYWORD POOLS — Tessell buying signals
# ════════════════════════════════════════════════════════════════════

# Oracle/DB pain — weighted by how directly they indicate Tessell need
DB_PAIN_KEYWORDS = {
    # Direct Oracle signals (highest weight)
    "oracle dba":               8,
    "oracle rac":               8,
    "oracle exadata":           8,
    "oracle licensing":         7,
    "oracle cost":              7,
    "oracle migration":         7,
    "oracle database":          5,
    "oracle":                   4,
    # SQL Server signals
    "sql server dba":           6,
    "sql server":               4,
    "mssql":                    4,
    # PostgreSQL / MySQL
    "postgres dba":             5,
    "postgresql":               4,
    "postgres":                 4,
    "mysql dba":                4,
    "mysql":                    3,
    # Database operations pain
    "dba toil":                 8,
    "manual dba":               7,
    "database toil":            6,
    "dba overhead":             6,
    "dba backlog":              6,
    "database sprawl":          7,
    "provisioning delay":       6,
    "slow provisioning":        6,
    "environment refresh":      5,
    "non-production database":  5,
    "non-prod":                 4,
    "database clone":           6,
    "database cloning":         6,
    "copy data":                5,
    # Other DB technologies (signal presence, not Oracle-specific)
    "mongodb":                  3,
    "mariadb":                  3,
    "aurora":                   3,
    "rds":                      3,
    "db2":                      3,
    "azure sql":                4,
    "database infrastructure":  3,
}

# Hiring keywords — active DBA/platform hiring = active pain
DB_HIRING_KEYWORDS = {
    "oracle dba":               8,
    "senior oracle dba":        8,
    "oracle database administrator": 7,
    "sql server dba":           7,
    "database reliability engineer": 8,
    "dbre":                     8,
    "database platform engineer": 7,
    "database engineer":        5,
    "database administrator":   5,
    "dba":                      5,
    "database architect":       5,
    "database devops":          6,
    "database automation":      6,
    "platform engineer":        4,
    "site reliability engineer": 4,
    "sre":                      4,
    "database operations":      5,
    "cloud database":           4,
    "data infrastructure":      4,
}

# Backup/DR/automation keywords
OPERATIONAL_KEYWORDS = {
    "disaster recovery":        5,
    "backup and recovery":      5,
    "backup":                   3,
    "high availability":        4,
    "failover":                 4,
    "rpo":                      4,
    "rto":                      4,
    "data loss":                4,
    "downtime":                 3,
    "database automation":      6,
    "automated database":       5,
    "database-as-a-service":    7,
    "dbaas":                    7,
    "self-service database":    6,
    "database provisioning":    5,
    "resilience":               3,
    "zero downtime":            4,
    "always-on":                4,
}

# Cloud/modernization keywords
MODERNIZATION_KEYWORDS = {
    "oracle migration":         8,
    "database migration":       6,
    "database modernization":   6,
    "cloud migration":          5,
    "cloud transformation":     5,
    "data center exit":         7,
    "data center migration":    6,
    "legacy modernization":     5,
    "erp modernization":        5,
    "erp migration":            5,
    "sap migration":            4,
    "platform engineering":     4,
    "devops transformation":    4,
    "infrastructure modernization": 4,
    "digital transformation":   3,
    "cloud first":              4,
}

# ════════════════════════════════════════════════════════════════════
# TIER 2 KEYWORD POOLS — Urgency multipliers
# ════════════════════════════════════════════════════════════════════

LEADERSHIP_TIMING_KEYWORDS = {
    "new cio":                  8,
    "new cto":                  8,
    "new vp infrastructure":    7,
    "new vp engineering":       7,
    "new vp technology":        7,
    "new chief information":    8,
    "new chief technology":     8,
    "named cio":                8,
    "named cto":                8,
    "appointed cio":            8,
    "appointed cto":            8,
    "joins as cio":             7,
    "joins as cto":             7,
    "hired as cio":             7,
    "new head of infrastructure": 6,
    "new head of engineering":  6,
    "new head of technology":   6,
}

MA_KEYWORDS = {
    "acquisition":              5,
    "acquired":                 5,
    "merger":                   5,
    "merges":                   5,
    "acquires":                 5,
    "divest":                   3,
    "divestiture":              3,
    "carve-out":                4,
    "spin-off":                 3,
    "integration":              3,
    "post-merger":              5,
    "database consolidation":   6,
}

EXPANSION_KEYWORDS = {
    "rapid expansion":          3,
    "multi-region":             4,
    "global expansion":         3,
    "new data center":          4,
    "expanding operations":     3,
    "scaling infrastructure":   3,
    "hypergrowth":              3,
    "aggressive growth":        3,
}

COMPLIANCE_KEYWORDS = {
    "hipaa":                    3,
    "pci dss":                  3,
    "pci":                      2,
    "soc 2":                    3,
    "fedramp":                  3,
    "gdpr":                     2,
    "data sovereignty":         3,
    "audit":                    2,
    "regulatory compliance":    2,
    "data governance":          2,
}

# ════════════════════════════════════════════════════════════════════
# TIER 3 — Context / qualification
# ════════════════════════════════════════════════════════════════════

ENTERPRISE_CONTEXT_KEYWORDS = {
    "fortune":                  4,
    "global operations":        3,
    "mission critical":         4,
    "24/7":                     3,
    "uptime sla":               3,
    "production systems":       3,
    "enterprise":               2,
    "data center":              2,
}

# Industry fit — industries with heaviest Oracle / legacy DB footprint
HIGH_FIT_INDUSTRIES  = [
    "financial","banking","insurance","healthcare","hospital","pharma",
    "government","defense","federal","energy","utility","airline","aviation",
]
MED_FIT_INDUSTRIES   = [
    "manufacturing","automotive","aerospace","logistics","transportation",
    "telecom","telecommunications","retail","distribution","supply chain",
]

# Penalty: DB vendors / technology sellers
DB_VENDOR_SIGNALS = [
    "database software", "database vendor", "database platform provider",
    "oracle partner", "sql server partner", "selling database",
    "database product", "we sell", "our database product",
]

# Penalty: stale signal age threshold
STALE_SIGNAL_DAYS = 90

# Things that suggest SMB — reduce score or exclude
SMB_SIGNALS = [
    "small business",
    "startup",
    "solopreneur",
    "solo founder",
    "local business",
    "family-owned",
    "boutique",
    "1-10 employees",
    "11-50 employees",
    "51-200 employees",
]

# ════════════════════════════════════════════════════════════════════
# ENTERPRISE GATE
# ════════════════════════════════════════════════════════════════════

@dataclass
class EnterpriseGateResult:
    passes: bool
    reason: str
    tier: str             # fortune_500 / fortune_1000 / large_enterprise / large_private / upper_midmarket / excluded
    estimated_employees: Optional[int]
    confidence: float     # 0–1: how confident is this classification


def enterprise_gate(
    company_name: str,
    estimated_employees: Optional[int],
    all_text: str,
    known_public: bool = False,
    known_fortune_rank: Optional[int] = None,
) -> EnterpriseGateResult:
    """
    Hard gate applied BEFORE scoring.
    Returns (passes=False) for any company that doesn't meet
    enterprise criteria. Called once per company.

    Rules (applied in order, first match wins):
      1. Known Fortune 500  → PASS  (fortune_500)
      2. Known Fortune 1000 → PASS  (fortune_1000)
      3. Employees ≥ 10,000 → PASS  (fortune_1000)
      4. Employees ≥  2,000 → PASS  (large_enterprise)
      5. Employees ≥  1,000 → PASS  (large_private)
      6. Employees ≥    500 → PASS  (upper_midmarket) IF complexity signals present
      7. Employees unknown  → PASS  IF strong enterprise text signals (confidence 0.5)
      8. Employees   < 500  → FAIL  (excluded)
      9. SMB signals present→ FAIL  regardless of stated size

    Complexity signals required for rule 6:
      - 2+ infrastructure/enterprise keywords in text
      - OR known regulated industry language
    """
    text_lower = all_text.lower()
    name_lower = company_name.lower()

    # DB vendor hard exclude — check FIRST
    name_clean = re.sub("[^a-z0-9 ]", "", name_lower).strip()
    for vendor in DB_VENDOR_COMPANIES:
        if name_lower == vendor or name_lower.startswith(vendor + " ") or name_clean == vendor:
            return EnterpriseGateResult(
                passes=False,
                reason=f"Database vendor/technology seller: '{company_name}' — not a Tessell buyer",
                tier="db_vendor",
                estimated_employees=estimated_employees,
                confidence=0.99,
            )

    # Media company exclude
    for media_pat in MEDIA_COMPANY_PATTERNS:
        if media_pat in name_lower:
            return EnterpriseGateResult(
                passes=False,
                reason=f"Media/research company: '{company_name}'",
                tier="excluded",
                estimated_employees=estimated_employees,
                confidence=0.90,
            )

    # SMB hard exclude — check before anything else
    for smb_sig in SMB_SIGNALS:
        if smb_sig in text_lower:
            return EnterpriseGateResult(
                passes=False,
                reason=f"SMB signal detected: '{smb_sig}'",
                tier="excluded",
                estimated_employees=estimated_employees,
                confidence=0.9,
            )

    # Known Fortune rank
    if known_fortune_rank is not None:
        if known_fortune_rank <= 500:
            return EnterpriseGateResult(
                passes=True, reason="Fortune 500 company",
                tier="fortune_500", estimated_employees=estimated_employees, confidence=0.99,
            )
        elif known_fortune_rank <= 1000:
            return EnterpriseGateResult(
                passes=True, reason="Fortune 1000 company",
                tier="fortune_1000", estimated_employees=estimated_employees, confidence=0.99,
            )

    # Employee count rules
    if estimated_employees is not None:
        if estimated_employees < 500:
            return EnterpriseGateResult(
                passes=False,
                reason=f"Employee count {estimated_employees:,} < 500 minimum",
                tier="excluded",
                estimated_employees=estimated_employees,
                confidence=0.95,
            )
        elif estimated_employees >= 10_000:
            tier = "fortune_500" if estimated_employees >= 50_000 else "fortune_1000"
            return EnterpriseGateResult(
                passes=True, reason=f"{estimated_employees:,} employees — large enterprise",
                tier=tier, estimated_employees=estimated_employees, confidence=0.9,
            )
        elif estimated_employees >= 2_000:
            return EnterpriseGateResult(
                passes=True, reason=f"{estimated_employees:,} employees — large enterprise",
                tier="large_enterprise", estimated_employees=estimated_employees, confidence=0.85,
            )
        elif estimated_employees >= 1_000:
            return EnterpriseGateResult(
                passes=True, reason=f"{estimated_employees:,} employees — large private",
                tier="large_private", estimated_employees=estimated_employees, confidence=0.8,
            )
        elif estimated_employees >= 500:
            # Need complexity signals to pass
            infra_hits = sum(1 for kw in ENTERPRISE_POSITIVE if kw in text_lower)
            if infra_hits >= 2:
                return EnterpriseGateResult(
                    passes=True,
                    reason=f"{estimated_employees:,} employees with {infra_hits} enterprise complexity signals",
                    tier="upper_midmarket", estimated_employees=estimated_employees, confidence=0.65,
                )
            else:
                return EnterpriseGateResult(
                    passes=False,
                    reason=f"{estimated_employees:,} employees but insufficient complexity signals ({infra_hits}/2 needed)",
                    tier="excluded", estimated_employees=estimated_employees, confidence=0.7,
                )

    # Unknown employee count — use text signals
    enterprise_hits = sum(w for kw, w in ENTERPRISE_POSITIVE.items() if kw in text_lower)
    db_hits = sum(1 for kw in DB_TECHNOLOGIES if kw in text_lower)
    infra_hits = sum(1 for kw in TRANSFORMATION_KEYWORDS if kw in text_lower)

    if enterprise_hits >= 10 or (db_hits >= 2 and infra_hits >= 1):
        return EnterpriseGateResult(
            passes=True,
            reason=f"Enterprise signals: enterprise_score={enterprise_hits}, db_techs={db_hits}, infra_signals={infra_hits}",
            tier="large_enterprise",
            estimated_employees=None,
            confidence=0.5,   # Low confidence — no employee count
        )

    # Not enough signal to qualify
    return EnterpriseGateResult(
        passes=False,
        reason=f"Insufficient enterprise signals (enterprise_score={enterprise_hits}, need ≥ 10)",
        tier="excluded",
        estimated_employees=None,
        confidence=0.4,
    )


# ════════════════════════════════════════════════════════════════════
# SCORE RESULT
# ════════════════════════════════════════════════════════════════════

@dataclass
class ScoreBreakdown:
    """Detailed score for one dimension with per-rule evidence."""
    raw: float
    capped: float
    max_possible: float
    rules_fired: List[Dict]   # [{"rule": str, "points": float, "evidence": str}]


@dataclass
class FullScoreResult:
    fit: ScoreBreakdown
    pain: ScoreBreakdown
    timing: ScoreBreakdown
    territory: ScoreBreakdown
    meeting_propensity: float   # 0–100
    total_score: float
    heat_level: str             # HOT / WARM / WATCHLIST / COLD
    confidence: float           # 0–1
    surfaced: bool              # Did it pass the min threshold?
    surface_reason: str
    score_notes: List[str]      # Human-readable evidence bullets


# ════════════════════════════════════════════════════════════════════
# TERRITORY DETECTION
# ════════════════════════════════════════════════════════════════════

US_STATES = {
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA",
    "KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN","TX","UT","VT",
    "VA","WA","WV","WI","WY","DC",
}

STATE_FULL_NAMES = {
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

# Known major city → state mapping (prevents false positives)
CITY_STATE_MAP = {
    "dallas":"TX","fort worth":"TX","houston":"TX","austin":"TX","san antonio":"TX",
    "oklahoma city":"OK","tulsa":"OK","edmond":"OK","norman":"OK",
    "wichita":"KS","overland park":"KS","kansas city":"KS","topeka":"KS",
    "chicago":"IL","new york":"NY","los angeles":"CA","san francisco":"CA",
    "seattle":"WA","atlanta":"GA","miami":"FL","boston":"MA","denver":"CO",
    "phoenix":"AZ","minneapolis":"MN","detroit":"MI","columbus":"OH",
    "indianapolis":"IN","memphis":"TN","nashville":"TN","charlotte":"NC",
    "raleigh":"NC","richmond":"VA","washington":"DC","baltimore":"MD",
    "jacksonville":"FL","orlando":"FL","tampa":"FL","salt lake city":"UT",
    "albuquerque":"NM","tucson":"AZ","sacramento":"CA","portland":"OR",
    "las vegas":"NV","reno":"NV","boise":"ID","omaha":"NE","lincoln":"NE",
    "little rock":"AR","jackson":"MS","birmingham":"AL","montgomery":"AL",
    "louisville":"KY","lexington":"KY","new orleans":"LA","baton rouge":"LA",
    "fargo":"ND","sioux falls":"SD","cheyenne":"WY","billings":"MT",
    "anchorage":"AK","honolulu":"HI","wilmington":"DE","providence":"RI",
    "burlington":"VT","manchester":"NH","hartford":"CT","springfield":"MA",
    "buffalo":"NY","albany":"NY","newark":"NJ","trenton":"NJ",
    "pittsburgh":"PA","philadelphia":"PA","cincinnati":"OH","cleveland":"OH",
    "milwaukee":"WI","madison":"WI","st louis":"MO","springfield":"MO",
    "des moines":"IA","cedar rapids":"IA","rapid city":"SD","helena":"MT",
    "concord":"NH","montpelier":"VT","dover":"DE","annapolis":"MD",
    "columbia":"SC","charleston":"SC","chattanooga":"TN","knoxville":"TN",
    "huntsville":"AL","mobile":"AL","shreveport":"LA","el paso":"TX",
    "corpus christi":"TX","lubbock":"TX","amarillo":"TX","laredo":"TX",
    "midland":"TX","odessa":"TX","abilene":"TX","waco":"TX","beaumont":"TX",
    "tulsa":"OK","broken arrow":"OK","lawton":"OK","moore":"OK",
    "enid":"OK","stillwater":"OK","muskogee":"OK",
}

# Metro area definitions — city names that belong to a metro
METRO_DEFINITIONS = {
    "dfw":         ["dallas","fort worth","plano","irving","frisco","mckinney","arlington","garland","richardson","lewisville","allen","carrollton","denton","mesquite","grand prairie","flower mound","rowlett","mansfield","coppell","grapevine"],
    "houston":     ["houston","sugar land","the woodlands","katy","pasadena","pearland","baytown","league city","conroe","spring","friendswood"],
    "austin":      ["austin","round rock","cedar park","georgetown","pflugerville","kyle","buda","san marcos","leander","hutto"],
    "oklahoma_city": ["oklahoma city","edmond","norman","moore","midwest city","yukon","mustang","shawnee","stillwater"],
    "tulsa":       ["tulsa","broken arrow","owasso","bixby","jenks","sapulpa","sand springs","claremore"],
    "kansas_city": ["kansas city","overland park","olathe","lenexa","shawnee","merriam","leawood","prairie village","mission"],
    "chicago":     ["chicago","naperville","aurora","joliet","elgin","waukegan","schaumburg","evanston","arlington heights","bolingbrook"],
    "nyc":         ["new york","brooklyn","queens","bronx","staten island","jersey city","newark","hoboken","yonkers","stamford","bridgeport"],
    "atlanta":     ["atlanta","alpharetta","marietta","sandy springs","roswell","johns creek","dunwoody","smyrna","peachtree city"],
    "miami":       ["miami","fort lauderdale","boca raton","west palm beach","pompano beach","hollywood","coral springs","deerfield beach"],
}


def extract_states_from_text(text: str) -> List[str]:
    """
    Extract US state codes from free text.
    Returns deduplicated list of 2-letter codes.

    False positive prevention:
      - Only accepts 2-letter sequences that are in US_STATES
      - Requires word boundary on both sides
      - Does not match ambiguous abbreviations (IN, OR, OK) without context
    """
    found = set()
    text_normalized = " " + text + " "

    # Pattern 1: "City, ST" — most reliable
    city_state_re = re.compile(
        r'\b([A-Za-z][a-z]+(?:\s[A-Za-z][a-z]+)?),\s*([A-Z]{2})\b'
    )
    for m in city_state_re.finditer(text):
        state = m.group(2)
        if state in US_STATES:
            found.add(state)

    # Pattern 2: Known city names → map to state
    text_lower = text.lower()
    for city, state in CITY_STATE_MAP.items():
        if city in text_lower:
            found.add(state)

    # Pattern 3: Full state names
    for full_name, abbrev in STATE_FULL_NAMES.items():
        # Use word boundary matching
        if re.search(r'\b' + full_name + r'\b', text_lower):
            found.add(abbrev)

    # Pattern 4: Standalone "TX" style — only if preceded/followed by non-alpha
    standalone_re = re.compile(r'(?<![A-Za-z])([A-Z]{2})(?![A-Za-z])')
    for m in standalone_re.finditer(text):
        code = m.group(1)
        if code in US_STATES:
            # Extra check: avoid ambiguous matches in the middle of words
            pos = m.start()
            before = text[max(0, pos-10):pos]
            after = text[pos+2:pos+12]
            # Only trust if surrounded by space/punctuation, not letters
            if not re.search(r'[A-Za-z]$', before) and not re.search(r'^[A-Za-z]', after):
                found.add(code)

    return list(found)


def detect_hiring_states(signals: List[Dict]) -> List[str]:
    """
    Extract states where the company is actively hiring.
    Only uses signals with source_type == 'job_posting'.
    """
    states = set()
    for sig in signals:
        if sig.get("source_type") != "job_posting":
            continue
        # Prefer explicit signal_state if set
        if sig.get("signal_state") and sig["signal_state"] in US_STATES:
            states.add(sig["signal_state"])
            continue
        # Fall back to text extraction
        text = f"{sig.get('raw_title','')} {sig.get('raw_excerpt','')}"
        for s in extract_states_from_text(text):
            states.add(s)
    return list(states)


def detect_office_states(signals: List[Dict], hq_state: Optional[str]) -> List[str]:
    """
    Detect states where the company has offices beyond HQ.
    Uses all signal types. Excludes HQ state to avoid redundancy.
    """
    states = set()
    for sig in signals:
        text = f"{sig.get('raw_title','')} {sig.get('raw_excerpt','')} {sig.get('raw_evidence','')}"
        # Look for explicit "office in X", "operations in X" patterns
        office_patterns = [
            r'office[s]?\s+in\s+([A-Za-z\s]+(?:,\s*[A-Z]{2})?)',
            r'headquartered\s+in\s+([A-Za-z\s]+)',
            r'operations\s+in\s+([A-Za-z\s,]+)',
            r'locations?\s+in\s+([A-Za-z\s,]+)',
        ]
        for pattern in office_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            for match in matches:
                for s in extract_states_from_text(match):
                    states.add(s)
        # Also extract all states from text
        for s in extract_states_from_text(text):
            states.add(s)

    # Remove HQ state from offices (to keep them semantically distinct)
    if hq_state and hq_state in states:
        states.discard(hq_state)

    return list(states)


# ════════════════════════════════════════════════════════════════════
# TERRITORY BOOST
# ════════════════════════════════════════════════════════════════════
# Max total territory boost = 20 points

TERRITORY_BOOST_RULES = {
    "hq_in_territory":        6,   # Company HQ is in seller's state(s)
    "major_office_in_territory": 5, # Non-HQ major office in territory
    "active_hiring_in_territory": 4, # Job postings in territory (last 30d)
    "recent_signal_in_territory": 3, # News/press signal from territory
}


def score_territory(
    hq_state: Optional[str],
    office_states: List[str],
    hiring_states: List[str],
    signal_states: List[str],
    target_states: List[str],
) -> Tuple[ScoreBreakdown, List[str]]:
    """
    Calculate territory boost.
    target_states: seller's territory (e.g. ["TX", "OK", "KS"])
    Returns (ScoreBreakdown, notes)
    """
    if not target_states:
        bd = ScoreBreakdown(raw=0, capped=0, max_possible=20, rules_fired=[])
        return bd, []

    target_set = {s.upper() for s in target_states}
    rules_fired = []
    notes = []
    total = 0.0

    # Rule 1: HQ in territory
    if hq_state and hq_state.upper() in target_set:
        pts = TERRITORY_BOOST_RULES["hq_in_territory"]
        rules_fired.append({"rule": "hq_in_territory", "points": pts,
                             "evidence": f"HQ state {hq_state} is in target territory"})
        notes.append(f"HQ in target territory ({hq_state}) +{pts}pts")
        total += pts

    # Rule 2: Major office in territory (only if not already counted as HQ)
    office_matches = [s for s in office_states if s.upper() in target_set
                      and s != hq_state]
    if office_matches:
        pts = TERRITORY_BOOST_RULES["major_office_in_territory"]
        rules_fired.append({"rule": "major_office_in_territory", "points": pts,
                             "evidence": f"Office presence in {office_matches}"})
        notes.append(f"Office presence in territory ({', '.join(office_matches)}) +{pts}pts")
        total += pts

    # Rule 3: Active hiring in territory
    hiring_matches = [s for s in hiring_states if s.upper() in target_set]
    if hiring_matches:
        pts = TERRITORY_BOOST_RULES["active_hiring_in_territory"]
        rules_fired.append({"rule": "active_hiring_in_territory", "points": pts,
                             "evidence": f"Hiring activity in {hiring_matches}"})
        notes.append(f"Active hiring in territory ({', '.join(hiring_matches)}) +{pts}pts")
        total += pts

    # Rule 4: Recent signal from territory
    signal_matches = [s for s in signal_states if s.upper() in target_set]
    if signal_matches:
        pts = TERRITORY_BOOST_RULES["recent_signal_in_territory"]
        rules_fired.append({"rule": "recent_signal_in_territory", "points": pts,
                             "evidence": f"Signals from {signal_matches}"})
        notes.append(f"Recent signals from territory ({', '.join(signal_matches)}) +{pts}pts")
        total += pts

    capped = min(15.0, total)
    bd = ScoreBreakdown(raw=total, capped=capped, max_possible=15, rules_fired=rules_fired)
    return bd, notes


# ════════════════════════════════════════════════════════════════════
# MAIN SCORER
# ════════════════════════════════════════════════════════════════════

MIN_SURFACE_SCORE = 55   # Below this → not surfaced at all
HEAT_THRESHOLDS = {"HOT": 85, "WARM": 70, "WATCHLIST": 55}


MIN_SURFACE_SCORE = 40   # Below this → not surfaced (was 55, lowered since scale changed)
HEAT_THRESHOLDS = {"HOT": 80, "WARM": 60, "WATCHLIST": 40}


class TessellScorer:

    def score(
        self,
        company_name: str,
        signals: List[Dict],
        enterprise_gate_result: EnterpriseGateResult,
        hq_state: Optional[str] = None,
        office_states: Optional[List[str]] = None,
        hiring_states: Optional[List[str]] = None,
        signal_states: Optional[List[str]] = None,
        target_states: Optional[List[str]] = None,
        industry: Optional[str] = None,
    ) -> FullScoreResult:

        all_text = self._signals_to_text(signals)
        notes    = []

        # ── TIER 1: Immediate buying signals ────────────────────────
        t1_pain  = self._score_db_pain(company_name, all_text, signals, industry, notes)
        t1_hire  = self._score_hiring(all_text, signals, notes)
        t1_ops   = self._score_operational(all_text, notes)
        t1_mod   = self._score_modernization(all_text, notes)
        tier1    = t1_pain.capped + t1_hire.capped + t1_ops.capped + t1_mod.capped

        # ── TIER 2: Urgency multipliers ──────────────────────────────
        t2_lead  = self._score_leadership(all_text, signals, notes)
        t2_ma    = self._score_ma(all_text, notes)
        t2_urg   = self._score_urgency_context(all_text, notes)
        recent_boost = self._recent_hiring_boost(signals, notes)
        tier2    = t2_lead.capped + t2_ma.capped + t2_urg.capped + recent_boost

        # ── TIER 3: Context / qualification ─────────────────────────
        t3_ctx   = self._score_context(all_text, enterprise_gate_result, industry, notes)
        tier3    = t3_ctx.capped

        # ── TERRITORY (0-15) ─────────────────────────────────────────
        territory, terr_notes = score_territory(
            hq_state=hq_state,
            office_states=office_states or [],
            hiring_states=hiring_states or [],
            signal_states=signal_states or [],
            target_states=target_states or [],
        )
        notes.extend(terr_notes)

        # ── PENALTIES ────────────────────────────────────────────────
        penalty  = self._score_penalties(all_text, signals, notes)

        # ── TOTAL ────────────────────────────────────────────────────
        raw_total = tier1 + tier2 + tier3 + territory.capped + penalty
        total     = max(0.0, min(100.0, raw_total))

        # ── HEAT LEVEL ───────────────────────────────────────────────
        heat = "COLD"
        for level, threshold in sorted(HEAT_THRESHOLDS.items(), key=lambda x: -x[1]):
            if total >= threshold:
                heat = level
                break

        # ── MEETING PROPENSITY ───────────────────────────────────────
        mp = self._meeting_propensity(total, tier1, tier2, signals)

        # ── SURFACE DECISION ─────────────────────────────────────────
        surfaced       = total >= MIN_SURFACE_SCORE
        surface_reason = (
            f"Score {total:.0f} ≥ threshold {MIN_SURFACE_SCORE}" if surfaced
            else f"Score {total:.0f} < threshold {MIN_SURFACE_SCORE}"
        )

        # Build legacy-compatible score breakdowns for UI
        # Map new tiers to old Fit/Pain/Timing/Territory structure
        fit = ScoreBreakdown(
            raw=tier3, capped=min(40, tier3 + t1_pain.capped * 0.3),
            max_possible=40,
            rules_fired=t3_ctx.rules_fired,
        )
        pain = ScoreBreakdown(
            raw=tier1, capped=min(40, tier1),
            max_possible=40,
            rules_fired=t1_pain.rules_fired + t1_hire.rules_fired + t1_ops.rules_fired + t1_mod.rules_fired,
        )
        timing = ScoreBreakdown(
            raw=tier2, capped=min(20, tier2),
            max_possible=20,
            rules_fired=t2_lead.rules_fired + t2_ma.rules_fired + t2_urg.rules_fired,
        )

        confidence = self._confidence(signals, enterprise_gate_result)

        return FullScoreResult(
            fit=fit, pain=pain, timing=timing, territory=territory,
            meeting_propensity=mp,
            total_score=round(total, 1),
            heat_level=heat,
            confidence=confidence,
            surfaced=surfaced,
            surface_reason=surface_reason,
            score_notes=notes,
        )


    # ── TIER 1: Oracle/DB pain (0-20) ────────────────────────────────

    def _score_db_pain(self, company_name: str, text: str,
                       signals: List[Dict], industry: Optional[str],
                       notes: List[str]) -> ScoreBreakdown:
        """
        Source-aware DB pain scoring.
        - SEC signals: capped at +2 regardless of keyword matches
        - Job/news signals: full keyword scoring
        - Company name stripped from text before scoring (avoids Oracle Corp self-match)
        - Inferred industry baseline: airlines/healthcare/etc get baseline without signals
        """
        rules = []
        total = 0.0

        # ── A. Inferred industry baseline (no signals needed) ─────────
        ind_base = 0
        if industry:
            ind_l = industry.lower()
            for ind_key, pts in INDUSTRY_DB_COMPLEXITY.items():
                if ind_key in ind_l:
                    ind_base = pts
                    break
        if ind_base > 0:
            rules.append({"rule":"industry_db_complexity","points":ind_base,
                          "evidence":f"Industry '{industry}' → known DB complexity baseline"})
            notes.append(f"Inferred DB complexity ({industry}): +{ind_base}pts baseline")
            total += ind_base

        # ── B. Signal-based scoring — source-aware ────────────────────
        # Strip company name tokens from text to prevent self-match
        # e.g. "Oracle Corp" should not score 'oracle' keyword
        name_lower  = company_name.lower()
        # Build a decontaminated text: only use text from non-SEC signals
        live_text_parts = []
        sec_text_parts  = []
        for sig in signals:
            src   = sig.get("source_type","")
            exc   = sig.get("raw_excerpt","") or ""
            title = sig.get("raw_title","")   or ""
            kws   = sig.get("keywords_matched",[]) or []
            chunk = f"{title} {exc} {' '.join(kws)}"
            if src == "sec_edgar":
                sec_text_parts.append(chunk)
            else:
                live_text_parts.append(chunk)

        # Score live signals at full weight, strip company name
        live_text = " ".join(live_text_parts).lower()
        # Remove exact company name tokens (prevents Oracle Corp scoring 'oracle')
        for token in name_lower.split():
            if len(token) > 3:
                live_text = live_text.replace(token, "___")

        live_pts = 0.0
        hit_kws  = []
        for kw, w in DB_PAIN_KEYWORDS.items():
            if kw in live_text:
                live_pts += w
                hit_kws.append(kw)
        live_pts = min(20, live_pts)
        if live_pts:
            rules.append({"rule":"live_signal_db_pain","points":live_pts,
                          "evidence":f"Live signals — DB pain: {hit_kws[:3]}"})
            notes.append(f"Live DB pain signals ({', '.join(hit_kws[:3])}): +{live_pts:.0f}pts")
            total += live_pts

        # Score SEC signals — CAPPED AT +2 TOTAL regardless of content
        if sec_text_parts:
            sec_text  = " ".join(sec_text_parts).lower()
            sec_hit   = any(kw in sec_text for kw in ["oracle","sql server","database","db2","postgresql"])
            sec_pts   = 2.0 if sec_hit else 0.0
            if sec_pts:
                rules.append({"rule":"sec_filing_mention","points":sec_pts,
                              "evidence":"SEC filing mentions DB technology (capped at +2)"})
                notes.append(f"SEC filing DB mention: +{sec_pts:.0f}pts (capped)")
                total += sec_pts

        capped = min(20.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=20, rules_fired=rules)

    # ── TIER 1: DBA/Platform hiring (0-15) ───────────────────────────

    def _score_hiring(self, text: str, signals: List[Dict], notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        # Score from job signals
        hire_sigs = [s for s in signals if s.get("signal_category") == "hiring"]
        hire_pts  = 0.0
        hire_kws  = []
        for sig in hire_sigs:
            title = f"{sig.get('raw_title','')} {sig.get('raw_excerpt','')}".lower()
            for kw, w in DB_HIRING_KEYWORDS.items():
                if kw in title:
                    hire_pts += w
                    if kw not in hire_kws:
                        hire_kws.append(kw)
                    break
        # Also scan signal text directly
        for kw, w in DB_HIRING_KEYWORDS.items():
            if kw in text_l and kw not in hire_kws:
                hire_pts += w * 0.6   # lower confidence — from news, not job posting
                hire_kws.append(kw)

        hire_pts = min(15, hire_pts)
        if hire_pts:
            rules.append({"rule":"db_sre_hiring","points":hire_pts,
                          "evidence":f"{len(hire_sigs)} hiring signals: {hire_kws[:3]}"})
            notes.append(f"DBA/SRE hiring ({', '.join(hire_kws[:3])}): +{hire_pts:.0f}pts")
            total += hire_pts

        capped = min(15.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=15, rules_fired=rules)

    # ── TIER 1: Backup/DR/automation (0-10) ──────────────────────────

    def _score_operational(self, text: str, notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        op_pts = 0.0
        op_kws = []
        for kw, w in OPERATIONAL_KEYWORDS.items():
            if kw in text_l:
                op_pts += w
                op_kws.append(kw)
        op_pts = min(10, op_pts)
        if op_pts:
            rules.append({"rule":"operational_signals","points":op_pts,
                          "evidence":f"Ops signals: {op_kws[:3]}"})
            notes.append(f"Backup/DR/automation signals ({', '.join(op_kws[:3])}): +{op_pts:.0f}pts")
            total += op_pts

        capped = min(10.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=10, rules_fired=rules)

    # ── TIER 1: Cloud/modernization (0-10) ───────────────────────────

    def _score_modernization(self, text: str, notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        mod_pts = 0.0
        mod_kws = []
        for kw, w in MODERNIZATION_KEYWORDS.items():
            if kw in text_l:
                mod_pts += w
                mod_kws.append(kw)
        mod_pts = min(10, mod_pts)
        if mod_pts:
            rules.append({"rule":"modernization","points":mod_pts,
                          "evidence":f"Modernization: {mod_kws[:3]}"})
            notes.append(f"Cloud/modernization active ({', '.join(mod_kws[:3])}): +{mod_pts:.0f}pts")
            total += mod_pts

        capped = min(10.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=10, rules_fired=rules)

    # ── TIER 2: Leadership change (0-10) ─────────────────────────────

    def _score_leadership(self, text: str, signals: List[Dict], notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        lead_pts = 0.0
        for kw, w in LEADERSHIP_TIMING_KEYWORDS.items():
            if kw in text_l:
                lead_pts += w
        lead_pts = min(10, lead_pts)
        if lead_pts:
            rules.append({"rule":"leadership_change","points":lead_pts,
                          "evidence":"CIO/CTO/VP leadership change detected"})
            notes.append(f"Leadership change (new CIO/CTO/VP Infra): +{lead_pts:.0f}pts")
            total += lead_pts

        capped = min(10.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=10, rules_fired=rules)

    # ── TIER 2: M&A complexity (0-8) ─────────────────────────────────

    def _score_ma(self, text: str, notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        ma_pts = 0.0
        ma_kws = []
        for kw, w in MA_KEYWORDS.items():
            if kw in text_l:
                ma_pts += w
                ma_kws.append(kw)
        ma_pts = min(8, ma_pts)
        if ma_pts:
            rules.append({"rule":"ma_complexity","points":ma_pts,
                          "evidence":f"M&A signals: {ma_kws[:3]}"})
            notes.append(f"M&A/acquisition complexity (DB sprawl risk): +{ma_pts:.0f}pts")
            total += ma_pts

        capped = min(8.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=8, rules_fired=rules)

    # ── TIER 2: Expansion + compliance (0-7) ─────────────────────────

    def _score_urgency_context(self, text: str, notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        # Expansion (0-4)
        exp_pts = min(4, sum(w for kw,w in EXPANSION_KEYWORDS.items() if kw in text_l))
        if exp_pts:
            rules.append({"rule":"expansion","points":exp_pts,"evidence":"Multi-region/expansion signals"})
            notes.append(f"Rapid expansion/multi-region: +{exp_pts:.0f}pts")
            total += exp_pts

        # Compliance (0-3)
        comp_pts = min(3, sum(w for kw,w in COMPLIANCE_KEYWORDS.items() if kw in text_l))
        if comp_pts:
            rules.append({"rule":"compliance","points":comp_pts,"evidence":"Security/compliance pressure"})
            notes.append(f"Security/compliance pressure: +{comp_pts:.0f}pts")
            total += comp_pts

        capped = min(7.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=7, rules_fired=rules)

    # ── TIER 3: Enterprise context (0-20) ────────────────────────────

    def _score_context(self, text: str, gate: EnterpriseGateResult,
                       industry: Optional[str], notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        # Enterprise tier (0-8, was 0-15)
        tier_pts = {
            "fortune_500": 8, "fortune_1000": 7,
            "large_enterprise": 5, "large_private": 4,
            "upper_midmarket": 2, "excluded": 0,
        }.get(gate.tier, 0)
        if tier_pts:
            rules.append({"rule":"enterprise_tier","points":tier_pts,
                          "evidence":f"Tier: {gate.tier}"})
            notes.append(f"Enterprise tier ({gate.tier}): +{tier_pts}pts")
            total += tier_pts

        # Industry fit (0-7)
        ind_pts = 0.0
        if industry:
            ind_l = industry.lower()
            if any(r in ind_l for r in HIGH_FIT_INDUSTRIES):
                ind_pts = 7
            elif any(r in ind_l for r in MED_FIT_INDUSTRIES):
                ind_pts = 4
        if ind_pts:
            rules.append({"rule":"industry_fit","points":ind_pts,"evidence":f"Industry: {industry}"})
            notes.append(f"Industry fit ({industry}): +{ind_pts:.0f}pts")
            total += ind_pts

        # Mission critical (0-5)
        mc_markers = ["mission critical","24/7","zero downtime","high availability",
                      "uptime sla","production systems","always-on","tier 1 application"]
        mc_pts = min(5, sum(1.67 for m in mc_markers if m in text_l))
        if mc_pts:
            rules.append({"rule":"mission_critical","points":mc_pts,"evidence":"Mission-critical env"})
            notes.append(f"Mission-critical environment: +{mc_pts:.0f}pts")
            total += mc_pts

        capped = min(20.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=20, rules_fired=rules)

    # ── PENALTY SYSTEM ────────────────────────────────────────────────

    def _score_penalties(self, text: str, signals: List[Dict],
                         notes: List[str]) -> float:
        """Returns negative adjustment (0 or negative)."""
        penalty = 0.0
        text_l  = text.lower()

        # Penalty: DB vendor/seller
        for sig in DB_VENDOR_SIGNALS:
            if sig in text_l:
                penalty -= 15
                notes.append(f"Penalty: DB vendor/seller signal detected: -15pts")
                break

        # Penalty: only stale signals (all >90 days old)
        if signals:
            cutoff = datetime.utcnow() - timedelta(days=STALE_SIGNAL_DAYS)
            fresh_count = 0
            for sig in signals:
                date_str = sig.get("signal_date","")
                if not date_str:
                    fresh_count += 0.5
                    continue
                try:
                    sig_date = datetime.fromisoformat(str(date_str).replace("Z","+00:00"))
                    if sig_date.replace(tzinfo=None) >= cutoff:
                        fresh_count += 1
                except Exception:
                    fresh_count += 0.3
            if fresh_count == 0 and len(signals) > 0:
                penalty -= 10
                notes.append("Penalty: all signals are stale (>90 days): -10pts")
            elif fresh_count < len(signals) * 0.3:
                penalty -= 5
                notes.append("Penalty: mostly stale signals: -5pts")

        # Penalty: Fortune rank is the ONLY positive signal
        if signals and all(
            not any(kw in (sig.get("raw_excerpt","") or "").lower()
                    for kw in ["oracle","database","dba","migration","cloud","backup","sre"])
            for sig in signals
        ):
            penalty -= 5
            notes.append("Penalty: no Tessell-relevant signals in content: -5pts")

        return max(-25, penalty)  # cap penalty at -25

    # ── RECENT HIRING BOOST ───────────────────────────────────────────

    def _recent_hiring_boost(self, signals: List[Dict], notes: List[str]) -> float:
        """Extra boost for very recent (<30d) DB/SRE hiring."""
        cutoff = datetime.utcnow() - timedelta(days=30)
        recent = 0
        for sig in signals:
            if sig.get("signal_category") != "hiring":
                continue
            date_str = sig.get("signal_date","")
            if not date_str:
                recent += 0.5; continue
            try:
                sig_date = datetime.fromisoformat(str(date_str).replace("Z","+00:00"))
                if sig_date.replace(tzinfo=None) >= cutoff:
                    recent += 1
            except Exception:
                recent += 0.3
        boost = min(6, recent * 2)
        if boost:
            notes.append(f"Recent DB/SRE hiring (<30d, {recent:.0f} roles): +{boost:.0f}pts")
        return boost


    def _meeting_propensity(self, total: float, tier1: float,
                             tier2: float, signals: List[Dict]) -> float:
        """
        0-100: How likely is a meeting THIS WEEK?
        Primary driver: Tessell buying signals (tier1) + urgency (tier2)
        """
        signal_component  = (tier1 / 55) * 45    # 45% weight on buying signals
        urgency_component = (tier2 / 25) * 30    # 30% weight on urgency
        size_component    = (total / 100) * 15   # 15% overall score
        recency = sum(1 for s in signals if s.get("signal_category") in ("hiring","timing"))
        recency_component = min(10, recency * 2) # 10% recency
        raw = signal_component + urgency_component + size_component + recency_component
        return round(min(100.0, raw), 1)

    def _signals_to_text(self, signals: List[Dict]) -> str:
        parts = []
        for s in signals:
            parts.append(s.get("raw_title", ""))
            parts.append(s.get("raw_excerpt", ""))
            parts.append(s.get("extracted_summary", ""))
            kws = s.get("keywords_matched", [])
            if isinstance(kws, list):
                parts.extend(kws)
        return " ".join(str(p) for p in parts if p)

    def _confidence(self, signals: List[Dict], gate: EnterpriseGateResult) -> float:
        score = 0.0
        if len(signals) >= 10: score += 0.3
        elif len(signals) >= 5: score += 0.2
        elif len(signals) >= 2: score += 0.1
        if gate.estimated_employees: score += 0.25
        if gate.confidence >= 0.8:   score += 0.2
        strong = sum(1 for s in signals if s.get("confidence", 0) >= 0.8)
        score += min(0.25, strong * 0.05)
        return round(min(1.0, score), 2)
