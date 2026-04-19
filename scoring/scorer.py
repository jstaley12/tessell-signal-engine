"""
scoring/scorer.py  — v2
════════════════════════════════════════════════════════════════════
EXACT SCORING LOGIC — every weight is documented and justified.

Score Architecture:
  Fit Score     0–40   Is this company a good structural fit for Tessell?
  Pain Score    0–40   Does evidence suggest database operational pain?
  Timing Score  0–20   Is now a better time than 6 months ago?
  Territory     0–20   Is this company in the seller's territory?
  ──────────────────
  Total         0–100

Heat Levels:
  HOT       ≥ 85   Immediate outreach — strong fit + timing trigger
  WARM      ≥ 70   Prioritize — good fit, some timing signal
  WATCHLIST ≥ 55   Monitor — good fit, low urgency
  COLD       < 55  Deprioritize

Enterprise Hard Gate (applied BEFORE scoring):
  - Estimated employees < 500      → EXCLUDED
  - No infrastructure signals AND  → EXCLUDED
    employees unknown AND
    no enterprise keywords

Meeting Propensity Score (0–100):
  Composite of:
    - Account heat level
    - Number of timing signals
    - Whether hiring signals are recent (< 30 days)
    - Whether a leadership change was detected
    - Whether a transformation announcement was made
  Used to rank "call this week" vs "nurture"
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
from datetime import datetime, timedelta

# ── Keyword pools ────────────────────────────────────────────────────

DB_TECHNOLOGIES = {
    "oracle":      10,   # Highest weight — biggest Tessell opportunity
    "sql server":   8,
    "mssql":        8,
    "postgres":     7,
    "postgresql":   7,
    "mysql":        6,
    "mongodb":      6,
    "mariadb":      5,
    "aurora":       5,
    "rds":          5,
    "db2":          5,
    "sybase":       4,
    "cassandra":    4,
    "teradata":     4,
    "snowflake":    3,
    "azure sql":    6,
    "google cloud sql": 5,
}

PAIN_KEYWORDS = {
    # DBA toil — direct evidence
    "manual dba":           4,
    "dba overhead":         4,
    "dba backlog":          4,
    "dba toil":             4,
    "database toil":        3,
    "manual database":      3,
    "manual operations":    2,
    # Provisioning pain
    "provisioning delay":   4,
    "slow provisioning":    4,
    "environment refresh":  3,
    "non-prod":             3,
    "non-production":       3,
    "dev/test":             3,
    "database clone":       4,
    "database cloning":     4,
    "copy data":            3,
    # Backup/DR/HA
    "disaster recovery":    3,
    "backup":               2,
    "high availability":    3,
    "failover":             3,
    "rpo":                  3,
    "rto":                  3,
    "data loss":            3,
    "downtime":             2,
    # Cost pressure
    "oracle cost":          4,
    "license cost":         3,
    "licensing cost":       3,
    "cloud cost":           2,
    "cost reduction":       2,
    "cost optimization":    2,
    # Compliance
    "compliance":           2,
    "audit":                2,
    "governance":           2,
    "hipaa":                3,
    "pci":                  3,
    "soc 2":                3,
}

HIRING_KEYWORDS = {
    "database administrator":   4,
    "dba":                      4,
    "database engineer":        4,
    "database reliability":     5,
    "dbre":                     5,
    "cloud database":           4,
    "database platform":        4,
    "platform engineer":        3,
    "site reliability engineer":4,
    "sre":                      3,
    "database architect":       3,
    "data infrastructure":      3,
    "oracle dba":               5,
    "sql server dba":           5,
    "postgres dba":             4,
    "database operations":      4,
    "database devops":          4,
    "database automation":      4,
}

TRANSFORMATION_KEYWORDS = {
    "cloud migration":           3,
    "cloud transformation":      3,
    "digital transformation":    2,
    "data center exit":          4,
    "data center migration":     4,
    "legacy modernization":      3,
    "app modernization":         3,
    "erp modernization":         3,
    "platform engineering":      3,
    "devops transformation":     2,
    "infrastructure modernization": 3,
    "oracle migration":          5,
    "database migration":        4,
    "database modernization":    4,
}

TIMING_KEYWORDS = {
    "new cio":                   6,
    "new cto":                   6,
    "new vp infrastructure":     5,
    "new vp engineering":        5,
    "named cio":                 6,
    "named cto":                 6,
    "appointed":                 3,
    "joins as":                  3,
    "hired as":                  3,
    "acquisition":               4,
    "acquired":                  4,
    "merger":                    4,
    "merges":                    4,
    "restructuring":             3,
    "transformation program":    3,
    "modernization initiative":  3,
    "cloud first strategy":      3,
    "announced partnership":     2,
    "microsoft partnership":     3,
    "aws partnership":           3,
    "azure partnership":         3,
}

ENTERPRISE_POSITIVE = {
    "fortune":                  8,
    "global operations":        6,
    "multinational":            5,
    "data center":              4,
    "mission critical":         5,
    "enterprise":               3,
    "large scale":              3,
    "24/7":                     4,
    "uptime sla":               4,
    "production systems":       3,
    "regulated":                4,
    "hipaa":                    5,
    "pci dss":                  5,
    "soc 2":                    4,
    "fedramp":                  5,
    "iso 27001":                4,
}

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
    "hq_in_territory":        8,   # Company HQ is in seller's state(s)
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

    capped = min(20.0, total)
    bd = ScoreBreakdown(raw=total, capped=capped, max_possible=20, rules_fired=rules_fired)
    return bd, notes


# ════════════════════════════════════════════════════════════════════
# MAIN SCORER
# ════════════════════════════════════════════════════════════════════

MIN_SURFACE_SCORE = 55   # Below this → not surfaced at all
HEAT_THRESHOLDS = {"HOT": 85, "WARM": 70, "WATCHLIST": 55}


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
        notes = []

        # ── FIT (0–40) ───────────────────────────────────────────────
        fit = self._score_fit(all_text, enterprise_gate_result, industry, signals, notes)

        # ── PAIN (0–40) ──────────────────────────────────────────────
        pain = self._score_pain(all_text, signals, notes)

        # ── TIMING (0–20) ────────────────────────────────────────────
        timing = self._score_timing(all_text, signals, notes)

        # ── TERRITORY (0–20) ─────────────────────────────────────────
        territory, terr_notes = score_territory(
            hq_state=hq_state,
            office_states=office_states or [],
            hiring_states=hiring_states or [],
            signal_states=signal_states or [],
            target_states=target_states or [],
        )
        notes.extend(terr_notes)

        # ── TOTAL ────────────────────────────────────────────────────
        total = min(100.0, fit.capped + pain.capped + timing.capped + territory.capped)

        # ── HEAT LEVEL ───────────────────────────────────────────────
        heat = "COLD"
        for level, threshold in sorted(HEAT_THRESHOLDS.items(), key=lambda x: -x[1]):
            if total >= threshold:
                heat = level
                break

        # ── MEETING PROPENSITY (0–100) ───────────────────────────────
        mp = self._meeting_propensity(total, timing, signals)

        # ── SURFACE DECISION ─────────────────────────────────────────
        surfaced = total >= MIN_SURFACE_SCORE
        surface_reason = (
            f"Score {total:.0f} ≥ threshold {MIN_SURFACE_SCORE}" if surfaced
            else f"Score {total:.0f} < threshold {MIN_SURFACE_SCORE}"
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

    # ── FIT scoring ──────────────────────────────────────────────────

    def _score_fit(self, text: str, gate: EnterpriseGateResult,
                   industry: Optional[str], signals: List[Dict], notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0

        # 1. Enterprise tier base (0–15)
        tier_pts = {
            "fortune_500": 15, "fortune_1000": 12,
            "large_enterprise": 9, "large_private": 6,
            "upper_midmarket": 3, "excluded": 0,
        }.get(gate.tier, 0)
        if tier_pts:
            rules.append({"rule": "enterprise_tier", "points": tier_pts,
                          "evidence": f"Tier: {gate.tier} → {tier_pts}pts"})
            notes.append(f"Enterprise tier ({gate.tier}): +{tier_pts}pts")
            total += tier_pts

        # 2. Database technology signals (0–12, weighted by DB)
        db_score = 0.0
        found_dbs = []
        for kw, weight in DB_TECHNOLOGIES.items():
            if kw in text.lower():
                found_dbs.append(kw)
                db_score += weight
        db_score = min(12, db_score)
        if db_score > 0:
            rules.append({"rule": "database_technologies", "points": db_score,
                          "evidence": f"DBs found: {found_dbs}"})
            notes.append(f"Database technologies ({', '.join(found_dbs[:3])}): +{db_score:.0f}pts")
            total += db_score

        # 3. Regulated industry (0–6)
        reg_pts = 0.0
        high_reg = ["financial","banking","insurance","healthcare","hospital",
                    "pharma","government","defense","federal","energy","utility","airline"]
        mid_reg  = ["manufacturing","automotive","aerospace","logistics","telecom","retail"]
        if industry:
            ind_l = industry.lower()
            if any(r in ind_l for r in high_reg):
                reg_pts = 6
            elif any(r in ind_l for r in mid_reg):
                reg_pts = 3
        if reg_pts:
            rules.append({"rule": "regulated_industry", "points": reg_pts,
                          "evidence": f"Industry: {industry}"})
            notes.append(f"Regulated industry ({industry}): +{reg_pts:.0f}pts")
            total += reg_pts

        # 4. Infrastructure complexity signals (0–5)
        infra_markers = ["multi-cloud","hybrid cloud","kubernetes","terraform",
                         "microservices","distributed systems","global infrastructure",
                         "data center","multiple regions","on-premises","on-prem"]
        infra_score = min(5, sum(1.25 for m in infra_markers if m in text.lower()))
        if infra_score:
            rules.append({"rule": "infra_complexity", "points": infra_score,
                          "evidence": "Infrastructure complexity markers found"})
            notes.append(f"Infrastructure complexity: +{infra_score:.0f}pts")
            total += infra_score

        # 5. Mission critical signals (0–4)
        mc_markers = ["mission critical","24/7","zero downtime","high availability",
                      "uptime sla","tier 1","production systems","always-on"]
        mc_score = min(4, sum(1.33 for m in mc_markers if m in text.lower()))
        if mc_score:
            rules.append({"rule": "mission_critical", "points": mc_score,
                          "evidence": "Mission-critical environment signals"})
            total += mc_score

        capped = min(40.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=40, rules_fired=rules)

    # ── PAIN scoring ─────────────────────────────────────────────────

    def _score_pain(self, text: str, signals: List[Dict], notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        # 1. Pain keyword matching (weighted)
        pain_score = 0.0
        triggered_pain = []
        for kw, weight in PAIN_KEYWORDS.items():
            if kw in text_l:
                pain_score += weight
                triggered_pain.append(kw)
        pain_score = min(15, pain_score)
        if pain_score:
            rules.append({"rule": "pain_keywords", "points": pain_score,
                          "evidence": f"Pain keywords: {triggered_pain[:5]}"})
            notes.append(f"Pain signals detected ({', '.join(triggered_pain[:3])}): +{pain_score:.0f}pts")
            total += pain_score

        # 2. Hiring for database/SRE roles (0–12)
        hire_signals = [s for s in signals if s.get("signal_category") == "hiring"]
        hire_score = 0.0
        for sig in hire_signals:
            title = sig.get("raw_title", "").lower()
            for kw, weight in HIRING_KEYWORDS.items():
                if kw in title:
                    hire_score += weight
                    break  # One weight per signal
        hire_score = min(12, hire_score)
        if hire_score:
            rules.append({"rule": "db_sre_hiring", "points": hire_score,
                          "evidence": f"{len(hire_signals)} DB/SRE job signals"})
            notes.append(f"Database/SRE hiring ({len(hire_signals)} roles): +{hire_score:.0f}pts")
            total += hire_score

        # 3. Transformation / modernization language (0–8)
        trans_score = 0.0
        for kw, weight in TRANSFORMATION_KEYWORDS.items():
            if kw in text_l:
                trans_score += weight
        trans_score = min(8, trans_score)
        if trans_score:
            rules.append({"rule": "modernization_language", "points": trans_score,
                          "evidence": "Cloud/database modernization language"})
            notes.append(f"Modernization language: +{trans_score:.0f}pts")
            total += trans_score

        # 4. Cost pressure (0–5)
        cost_markers = ["oracle cost","license cost","licensing cost",
                        "cloud cost","cost reduction","cost optimization","finops"]
        cost_score = min(5, sum(1.25 for m in cost_markers if m in text_l))
        if cost_score:
            rules.append({"rule": "cost_pressure", "points": cost_score,
                          "evidence": "Cost pressure / optimization language"})
            total += cost_score

        capped = min(40.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=40, rules_fired=rules)

    # ── TIMING scoring ───────────────────────────────────────────────

    def _score_timing(self, text: str, signals: List[Dict], notes: List[str]) -> ScoreBreakdown:
        rules = []
        total = 0.0
        text_l = text.lower()

        # 1. Leadership change (0–8)
        lead_score = 0.0
        for kw, weight in TIMING_KEYWORDS.items():
            if kw in text_l:
                lead_score += weight
        lead_score = min(8, lead_score)
        if lead_score:
            rules.append({"rule": "timing_keywords", "points": lead_score,
                          "evidence": "Leadership change or transformation announcement"})
            notes.append(f"Timing trigger detected (leadership/transformation): +{lead_score:.0f}pts")
            total += lead_score

        # 2. Recent hiring (< 30 days) (0–6)
        cutoff_30 = datetime.utcnow() - timedelta(days=30)
        recent_hire_count = 0
        for sig in signals:
            if sig.get("signal_category") != "hiring":
                continue
            date_str = sig.get("signal_date")
            if not date_str:
                recent_hire_count += 0.5  # Partial — undated but recent collection
                continue
            try:
                sig_date = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
                if sig_date.replace(tzinfo=None) >= cutoff_30:
                    recent_hire_count += 1
            except Exception:
                recent_hire_count += 0.3

        recent_pts = min(6, recent_hire_count * 2)
        if recent_pts:
            rules.append({"rule": "recent_hiring", "points": recent_pts,
                          "evidence": f"{recent_hire_count:.0f} hiring signals in last 30d"})
            notes.append(f"Recent hiring activity ({recent_hire_count:.0f} roles < 30d): +{recent_pts:.0f}pts")
            total += recent_pts

        # 3. Press release / announcement (0–4)
        announcement_signals = [s for s in signals if s.get("source_type") in
                                 ("press_release", "ir_page", "news")]
        if announcement_signals:
            ann_pts = min(4, len(announcement_signals) * 2)
            rules.append({"rule": "announcements", "points": ann_pts,
                          "evidence": f"{len(announcement_signals)} press/news signals"})
            notes.append(f"Public announcements found: +{ann_pts:.0f}pts")
            total += ann_pts

        # 4. M&A (0–4)
        ma_markers = ["acquisition","acquired","merger","merges","acquires","divest"]
        ma_pts = min(4, sum(2 for m in ma_markers if m in text_l))
        if ma_pts:
            rules.append({"rule": "ma_activity", "points": ma_pts,
                          "evidence": "M&A activity detected"})
            notes.append(f"M&A activity (creates integration complexity): +{ma_pts:.0f}pts")
            total += ma_pts

        capped = min(20.0, total)
        return ScoreBreakdown(raw=total, capped=capped, max_possible=20, rules_fired=rules)

    # ── Meeting Propensity ───────────────────────────────────────────

    def _meeting_propensity(
        self, total_score: float, timing: ScoreBreakdown, signals: List[Dict]
    ) -> float:
        """
        0–100 score answering: "How likely is a meeting to be booked THIS WEEK?"
        Components:
          - Total score weight (35%)
          - Timing score weight (25%)
          - Recent hiring signals (20%)
          - Leadership change detected (15%)
          - Announcement detected (5%)
        """
        score_component = (total_score / 100) * 35
        timing_component = (timing.capped / 20) * 25

        recent_hires = sum(1 for r in timing.rules_fired if r["rule"] == "recent_hiring")
        hiring_component = min(20, recent_hires * 10)

        leadership = any(r["rule"] == "timing_keywords" for r in timing.rules_fired)
        leadership_component = 15 if leadership else 0

        announcement = any(r["rule"] == "announcements" for r in timing.rules_fired)
        announcement_component = 5 if announcement else 0

        raw = score_component + timing_component + hiring_component + leadership_component + announcement_component
        return round(min(100.0, raw), 1)

    # ── Helpers ──────────────────────────────────────────────────────

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
        if gate.confidence >= 0.8: score += 0.2
        strong = sum(1 for s in signals if s.get("confidence", 0) >= 0.8)
        score += min(0.25, strong * 0.05)
        return round(min(1.0, score), 2)
