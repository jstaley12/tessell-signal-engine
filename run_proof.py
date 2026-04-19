#!/usr/bin/env python3
"""
run_proof.py  —  Tessell Signal Engine  |  Phase 2  |  Local Run
═══════════════════════════════════════════════════════════════════

SINGLE COMMAND:
    python run_proof.py

OPTIONS:
    --quick           Run 5 companies only (faster first test)
    --company NAME    Single company (exact name, case-insensitive)
    --states TX,OK,KS Override territory (default: TX,OK,KS)
    --log-level       DEBUG | INFO (default INFO)

OUTPUTS (all written to ./reports/):
    proof_output.json         Full structured results for all companies
    proof_summary.csv         One row per company, all score fields
    false_positive_report.csv Signals flagged as likely false positives
    source_quality.json       Success rate / signal count by source type
    post_run_report.txt       Human-readable summary (also printed to terminal)

LOGGING:
    logs/run_YYYYMMDD_HHMMSS.log   Timestamped log file
    stderr                          INFO-level progress
"""

import sys, os, json, csv, time, argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from loguru import logger

# ── Configure logging (file + stderr) ────────────────────────────────────────
LOG_DIR = Path(__file__).parent / "logs"
LOG_DIR.mkdir(exist_ok=True)
REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)
DATA_DIR   = Path(__file__).parent / "data" / "live"
DATA_DIR.mkdir(parents=True, exist_ok=True)

_log_file = LOG_DIR / f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logger.remove()
# stderr — INFO by default
logger.add(sys.stderr, level="INFO",
           format="<cyan>{time:HH:mm:ss}</cyan> | <level>{level:<7}</level> | {message}")
# file — DEBUG always
logger.add(str(_log_file), level="DEBUG",
           format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<7} | {name}:{line} | {message}")

from collectors.live_collectors import collect_all
from collectors.fetcher import FetchLog
from collectors.signal import LiveSignal
from scoring.scorer import (
    TessellScorer, enterprise_gate,
    detect_hiring_states, extract_states_from_text,
)

scorer = TessellScorer()


# ════════════════════════════════════════════════════════════════════
# 25 FORTUNE 1000 TARGETS
# ════════════════════════════════════════════════════════════════════

TARGETS: List[dict] = [
    # ── Texas HQ ───────────────────────────────────────────────────
    {"name":"McKesson",          "industry":"Healthcare / Distribution",       "employees":51000,  "fortune_rank":9,   "hq_state":"TX","hq_city":"Irving",       "ticker":"MCK",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://www.mckesson.com/Careers/",                       "newsroom_url":"https://www.mckesson.com/About-McKesson/Newsroom/",           "news_terms":["Oracle database","database migration","cloud platform DBA"]},
    {"name":"AT&T",              "industry":"Telecommunications",               "employees":160000, "fortune_rank":13,  "hq_state":"TX","hq_city":"Dallas",        "ticker":"T",    "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://www.att.jobs/search-jobs",                         "newsroom_url":"https://about.att.com/story/2024/",                          "news_terms":["Oracle modernization","database engineer","cloud migration"]},
    {"name":"American Airlines", "industry":"Airlines",                         "employees":95000,  "fortune_rank":69,  "hq_state":"TX","hq_city":"Fort Worth",    "ticker":"AAL",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://jobs.aa.com/",                                      "newsroom_url":"https://news.aa.com/",                                       "news_terms":["Oracle database","database engineer","cloud migration"]},
    {"name":"Southwest Airlines","industry":"Airlines",                         "employees":65000,  "fortune_rank":75,  "hq_state":"TX","hq_city":"Dallas",        "ticker":"LUV",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.southwestair.com/",                         "newsroom_url":"https://www.southwestairlinesoneblog.com/",                  "news_terms":["database engineer","Oracle migration","platform engineering"]},
    {"name":"Kimberly-Clark",    "industry":"Consumer Goods / Manufacturing",   "employees":46000,  "fortune_rank":184, "hq_state":"TX","hq_city":"Irving",        "ticker":"KMB",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://jobs.kimberly-clark.com/",                          "newsroom_url":"https://investor.kimberly-clark.com/press-releases",         "news_terms":["Oracle","SAP migration","database modernization"]},
    {"name":"ConocoPhillips",    "industry":"Energy / Oil & Gas",               "employees":9700,   "fortune_rank":111, "hq_state":"TX","hq_city":"Houston",       "ticker":"COP",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.conocophillips.com/",                       "newsroom_url":"https://investor.conocophillips.com/news-releases",          "news_terms":["Oracle database","SAP migration","cloud platform"]},
    {"name":"Phillips 66",       "industry":"Energy / Refining",                "employees":13700,  "fortune_rank":42,  "hq_state":"TX","hq_city":"Houston",       "ticker":"PSX",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.phillips66.com/",                           "newsroom_url":"https://investor.phillips66.com/news-releases",              "news_terms":["Oracle database","database engineer","SAP"]},
    # ── Oklahoma HQ ────────────────────────────────────────────────
    {"name":"ONEOK",             "industry":"Energy / Midstream",               "employees":3100,   "fortune_rank":218, "hq_state":"OK","hq_city":"Tulsa",         "ticker":"OKE",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://www.oneok.com/About-ONEOK/Careers",                "newsroom_url":"https://www.oneok.com/News",                                 "news_terms":["Oracle database","cloud migration","database administrator"]},
    {"name":"Devon Energy",      "industry":"Energy / Oil & Gas",               "employees":4300,   "fortune_rank":244, "hq_state":"OK","hq_city":"Oklahoma City", "ticker":"DVN",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://www.devonenergy.com/careers",                      "newsroom_url":"https://www.devonenergy.com/news",                           "news_terms":["Oracle","SAP","cloud transformation","database"]},
    # ── Kansas HQ ──────────────────────────────────────────────────
    {"name":"Spirit AeroSystems","industry":"Aerospace / Manufacturing",        "employees":13000,  "fortune_rank":412, "hq_state":"KS","hq_city":"Wichita",       "ticker":"SPR",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://jobs.spiritaero.com/",                              "newsroom_url":"https://www.spiritaero.com/company/news/",                   "news_terms":["Oracle ERP","cloud migration","database modernization"]},
    # ── National targets ───────────────────────────────────────────
    {"name":"Cummins",           "industry":"Manufacturing / Industrial",       "employees":59900,  "fortune_rank":147, "hq_state":"IN","hq_city":"Columbus",       "ticker":"CMI",  "greenhouse_slug":"cummins","lever_slug":None,"careers_url":"https://cummins.wd1.myworkdayjobs.com/RecruiterPortal",   "workday_url":"https://cummins.wd1.myworkdayjobs.com/RecruiterPortal",       "newsroom_url":"https://www.cummins.com/news",                "news_terms":["Oracle database","SAP migration","cloud transformation"]},
    {"name":"Eli Lilly",         "industry":"Pharmaceuticals",                  "employees":43000,  "fortune_rank":129, "hq_state":"IN","hq_city":"Indianapolis",   "ticker":"LLY",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://lilly.jobs/",                                       "newsroom_url":"https://investor.lilly.com/news-releases",                   "news_terms":["database engineer","Oracle","cloud platform","DBA"]},
    {"name":"UnitedHealth Group","industry":"Healthcare / Insurance",           "employees":400000, "fortune_rank":7,   "hq_state":"MN","hq_city":"Minnetonka",    "ticker":"UNH",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.unitedhealthgroup.com/",                    "newsroom_url":"https://newsroom.uhc.com/",                                  "news_terms":["Oracle database","database engineer","cloud platform"]},
    {"name":"JPMorgan Chase",    "industry":"Financial Services / Banking",     "employees":310000, "fortune_rank":24,  "hq_state":"NY","hq_city":"New York",       "ticker":"JPM",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.jpmorgan.com/",                             "newsroom_url":"https://www.jpmorganchase.com/news",                         "news_terms":["Oracle database","database reliability","DBA"]},
    {"name":"Citigroup",         "industry":"Financial Services / Banking",     "employees":240000, "fortune_rank":33,  "hq_state":"NY","hq_city":"New York",       "ticker":"C",    "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://jobs.citi.com/",                                    "newsroom_url":"https://www.citigroup.com/global/news",                      "news_terms":["Oracle migration","database modernization","cloud database"]},
    {"name":"Boeing",            "industry":"Aerospace / Defense",              "employees":150000, "fortune_rank":67,  "hq_state":"VA","hq_city":"Arlington",      "ticker":"BA",   "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://jobs.boeing.com/",                                  "newsroom_url":"https://investors.boeing.com/investors/news-releases",        "news_terms":["Oracle database","database engineer","SAP"]},
    {"name":"General Motors",    "industry":"Automotive / Manufacturing",       "employees":150000, "fortune_rank":16,  "hq_state":"MI","hq_city":"Detroit",        "ticker":"GM",   "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.gm.com/",                                   "newsroom_url":"https://investor.gm.com/news-releases",                      "news_terms":["Oracle database","database platform","cloud migration"]},
    {"name":"HCA Healthcare",    "industry":"Healthcare / Hospital Systems",    "employees":295000, "fortune_rank":57,  "hq_state":"TN","hq_city":"Nashville",      "ticker":"HCA",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.hcahealthcare.com/",                        "newsroom_url":"https://investor.hcahealthcare.com/news-releases",           "news_terms":["Oracle database","HIPAA database","cloud platform"]},
    {"name":"Humana",            "industry":"Healthcare / Insurance",           "employees":67000,  "fortune_rank":53,  "hq_state":"KY","hq_city":"Louisville",     "ticker":"HUM",  "greenhouse_slug":"humana", "lever_slug":None,"careers_url":"https://careers.humana.com/",                               "newsroom_url":"https://newsroom.humana.com/",                               "news_terms":["Oracle","database modernization","cloud platform"]},
    {"name":"J.B. Hunt",         "industry":"Transportation / Logistics",       "employees":35000,  "fortune_rank":409, "hq_state":"AR","hq_city":"Lowell",         "ticker":"JBHT", "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://www.jbhunt.com/careers/",                           "newsroom_url":"https://www.jbhunt.com/news/",                               "news_terms":["Oracle database","database engineer","cloud migration"]},
    {"name":"FedEx",             "industry":"Logistics / Courier",              "employees":500000, "fortune_rank":59,  "hq_state":"TN","hq_city":"Memphis",        "ticker":"FDX",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.fedex.com/",                                "newsroom_url":"https://newsroom.fedex.com/",                                "news_terms":["Oracle database","database engineer","cloud transformation"]},
    {"name":"Dollar General",    "industry":"Retail",                           "employees":164000, "fortune_rank":122, "hq_state":"TN","hq_city":"Goodlettsville", "ticker":"DG",   "greenhouse_slug":"dollargeneral","lever_slug":None,"careers_url":"https://careers.dollargeneral.com/",              "newsroom_url":"https://investor.dollargeneral.com/news-releases",           "news_terms":["Oracle database","database engineer","cloud platform"]},
    {"name":"Cognizant",         "industry":"Technology / IT Services",         "employees":340000, "fortune_rank":185, "hq_state":"NJ","hq_city":"Teaneck",        "ticker":"CTSH", "greenhouse_slug":"cognizant","lever_slug":None,"careers_url":"https://careers.cognizant.com/",                        "newsroom_url":"https://investors.cognizant.com/news-releases",              "news_terms":["Oracle DBA","database migration","database modernization"]},
    {"name":"HP Inc",            "industry":"Technology / Hardware",            "employees":58000,  "fortune_rank":61,  "hq_state":"CA","hq_city":"Palo Alto",      "ticker":"HPQ",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://jobs.hp.com/",                                      "newsroom_url":"https://investor.hp.com/press-releases",                     "news_terms":["Oracle database","database engineer","DBA"]},
    {"name":"ManpowerGroup",     "industry":"Professional Services",            "employees":28000,  "fortune_rank":197, "hq_state":"WI","hq_city":"Milwaukee",      "ticker":"MAN",  "greenhouse_slug":None,    "lever_slug":None,"careers_url":"https://careers.manpowergroup.com/",                        "newsroom_url":"https://investor.manpowergroup.com/news-releases",           "news_terms":["Oracle","database engineer","DBA"]},
]


# ════════════════════════════════════════════════════════════════════
# SIGNAL → SCORER FORMAT ADAPTER
# ════════════════════════════════════════════════════════════════════

def to_scorer_fmt(signals: List) -> List[dict]:
    DB_TECH = {"oracle","sql server","postgresql","postgres","mysql",
               "mongodb","aurora","rds","azure sql","db2","mariadb"}
    out = []
    for s in signals:
        d  = s.to_dict() if hasattr(s, "to_dict") else s
        kw = d.get("extracted_keywords", [])
        out.append({
            "raw_title":                     d.get("raw_snippet","")[:100],
            "raw_excerpt":                   d.get("raw_snippet",""),
            "source_type":                   d.get("source_type",""),
            "signal_category":               d.get("signal_type",""),
            "keywords_matched":              kw,
            "database_technologies_mentioned":[k for k in kw if k in DB_TECH],
            "signal_state":                  d.get("state_detected"),
            "signal_city":                   d.get("city_detected"),
            "signal_date":                   d.get("date_found"),
            "confidence":                    d.get("confidence_score", 0.7),
            "signal_strength":               "strong" if d.get("confidence_score",0) >= 0.85 else "moderate",
        })
    return out


# ════════════════════════════════════════════════════════════════════
# PER-COMPANY RUNNER
# ════════════════════════════════════════════════════════════════════

def run_one(cd: dict, target_states: List[str]) -> dict:
    name = cd["name"]
    logger.info(f"{'─'*50}")
    logger.info(f"▶  Starting: {name}  (F{cd.get('fortune_rank','')} | {cd['hq_state']} | {cd['employees']:,} emp)")
    t0 = time.time()

    col = collect_all(
        company        = name,
        greenhouse_slug= cd.get("greenhouse_slug"),
        lever_slug     = cd.get("lever_slug"),
        careers_url    = cd.get("careers_url"),
        workday_url    = cd.get("workday_url"),
        icims_url      = cd.get("icims_url"),
        newsroom_url   = cd.get("newsroom_url"),
        ir_url         = cd.get("ir_url"),
        ticker         = cd.get("ticker"),
        news_terms     = cd.get("news_terms"),
    )

    signals  = col["signals"]
    elapsed  = round(time.time() - t0, 1)
    sc_sigs  = to_scorer_fmt(signals)
    all_text = " ".join(
        (s.raw_snippet if hasattr(s,"raw_snippet") else s.get("raw_snippet",""))
        for s in signals
    )

    # Source log
    for src, m in col["per_source"].items():
        n   = m.get("relevant_signals", 0)
        acc = m.get("access_status","?")
        if n > 0:
            logger.info(f"   ✅ {src:<22} {n} signals")
        else:
            reason = m.get("limitation") or m.get("access_status") or "0 results"
            logger.info(f"   ⬜ {src:<22} 0 signals  ({reason})")

    logger.info(f"   Total: {col['total_raw']} raw → {col['false_positives_removed']} deduped → {len(signals)} unique signals ({elapsed}s)")

    # Enterprise gate
    gate = enterprise_gate(
        company_name       = name,
        estimated_employees= cd.get("employees"),
        all_text           = all_text,
        known_public       = bool(cd.get("ticker")),
        known_fortune_rank = cd.get("fortune_rank"),
    )

    # State detection
    hq_state      = cd.get("hq_state","")
    hiring_states = detect_hiring_states(sc_sigs)
    text_states   = extract_states_from_text(all_text)
    office_states = [s for s in text_states if s != hq_state]
    signal_states = list(set(
        (s.state_detected if hasattr(s,"state_detected") else s.get("state_detected"))
        for s in signals
        if (s.state_detected if hasattr(s,"state_detected") else s.get("state_detected"))
    ))

    # Score
    if gate.passes:
        r = scorer.score(
            company_name           = name,
            signals                = sc_sigs,
            enterprise_gate_result = gate,
            hq_state               = hq_state,
            office_states          = office_states,
            hiring_states          = hiring_states,
            signal_states          = signal_states,
            target_states          = target_states,
            industry               = cd.get("industry",""),
        )
        n = len(signals)
        scores = {
            "fit_score":           r.fit.capped,
            "pain_score":          r.pain.capped,
            "timing_score":        r.timing.capped,
            "territory_score":     r.territory.capped,
            "total_score":         r.total_score,
            "meeting_propensity":  r.meeting_propensity,
            "heat_level":          r.heat_level,
            "surfaced":            r.surfaced,
            "surface_reason":      r.surface_reason + (f" ({n} live signals)" if n else " (0 live signals — base score only)"),
            "score_evidence":      r.score_notes,
            "fit_rules":           r.fit.rules_fired,
            "pain_rules":          r.pain.rules_fired,
            "timing_rules":        r.timing.rules_fired,
            "territory_rules":     r.territory.rules_fired,
        }
        logger.info(f"   Score: {r.total_score:.0f}/100  Heat: {r.heat_level}  MtgP: {r.meeting_propensity:.0f}")
    else:
        scores = {
            "fit_score":0,"pain_score":0,"timing_score":0,"territory_score":0,
            "total_score":0,"meeting_propensity":0,"heat_level":"COLD",
            "surfaced":False,
            "surface_reason":f"Enterprise gate failed: {gate.reason}",
            "score_evidence":[],"fit_rules":[],"pain_rules":[],"timing_rules":[],"territory_rules":[],
        }
        logger.warning(f"   Gate FAILED: {gate.reason}")

    # Per-source summary for output
    per_src_out = {}
    for src, m in col["per_source"].items():
        per_src_out[src] = {
            "signals_returned":  m.get("relevant_signals", 0),
            "access_status":     m.get("access_status"),
            "total_fetched":     m.get("total_jobs_fetched") or m.get("raw_candidates") or m.get("raw_items_fetched"),
            "noise_filtered":    m.get("noise_filtered", 0),
            "strategy_used":     m.get("strategy_used"),
            "limitation":        m.get("limitation"),
            "proof_url":         m.get("endpoint") or m.get("url"),
        }

    return {
        "company_name":  name,
        "industry":      cd["industry"],
        "fortune_rank":  cd.get("fortune_rank"),
        "employees":     cd.get("employees"),
        "hq_state":      hq_state,
        "hq_city":       cd.get("hq_city",""),
        "ticker":        cd.get("ticker",""),
        "collection": {
            "elapsed_seconds":           elapsed,
            "total_raw":                 col["total_raw"],
            "false_positives_removed":   col["false_positives_removed"],
            "total_after_dedup":         col["total_after_dedup"],
            "live_signals_ingested":     len(signals),
            "per_source":                per_src_out,
        },
        "enterprise_gate": {
            "passes":     gate.passes,
            "tier":       gate.tier,
            "reason":     gate.reason,
            "confidence": gate.confidence,
        },
        "geography": {
            "hq_state":               hq_state,
            "detected_hiring_states": hiring_states,
            "detected_office_states": list(set(office_states))[:6],
            "signal_states":          signal_states,
            "target_states":          target_states,
            "hq_in_territory":        hq_state in target_states,
            "territory_presence":     list(set(
                [s for s in hiring_states  if s in target_states] +
                [s for s in office_states  if s in target_states] +
                [s for s in signal_states  if s in target_states]
            )),
        },
        "scores": scores,
        "signals": [
            s.to_dict() if hasattr(s,"to_dict") else s
            for s in signals
        ],
    }


# ════════════════════════════════════════════════════════════════════
# OUTPUT WRITERS
# ════════════════════════════════════════════════════════════════════

def write_proof_json(output: dict, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)
    logger.info(f"Written: {path}")


def write_summary_csv(companies: List[dict], path: Path):
    if not companies: return
    fieldnames = [
        "company_name","fortune_rank","hq_state","hq_city","industry",
        "employees","ticker","total_score","fit_score","pain_score",
        "timing_score","territory_score","meeting_propensity","heat_level",
        "surfaced","enterprise_tier","live_signals","elapsed_seconds",
        "sources_with_data","hq_in_territory","surface_reason",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for c in companies:
            sc = c.get("scores",{})
            cl = c.get("collection",{})
            eg = c.get("enterprise_gate",{})
            geo= c.get("geography",{})
            src_with_data = ",".join(
                k for k,v in cl.get("per_source",{}).items()
                if (v.get("signals_returned") or 0) > 0
            )
            w.writerow({
                "company_name":       c.get("company_name",""),
                "fortune_rank":       c.get("fortune_rank",""),
                "hq_state":           c.get("hq_state",""),
                "hq_city":            c.get("hq_city",""),
                "industry":           c.get("industry",""),
                "employees":          c.get("employees",""),
                "ticker":             c.get("ticker",""),
                "total_score":        sc.get("total_score",0),
                "fit_score":          sc.get("fit_score",0),
                "pain_score":         sc.get("pain_score",0),
                "timing_score":       sc.get("timing_score",0),
                "territory_score":    sc.get("territory_score",0),
                "meeting_propensity": sc.get("meeting_propensity",0),
                "heat_level":         sc.get("heat_level","COLD"),
                "surfaced":           sc.get("surfaced",False),
                "enterprise_tier":    eg.get("tier",""),
                "live_signals":       cl.get("live_signals_ingested",0),
                "elapsed_seconds":    cl.get("elapsed_seconds",0),
                "sources_with_data":  src_with_data or "none",
                "hq_in_territory":    geo.get("hq_in_territory",False),
                "surface_reason":     sc.get("surface_reason",""),
            })
    logger.info(f"Written: {path}")


def write_false_positive_csv(companies: List[dict], path: Path):
    """Signals with low confidence or weak keyword matches for human review."""
    fieldnames = [
        "company_name","source_url","source_type","signal_type",
        "confidence_score","enterprise_relevance_score","keyword_count",
        "extracted_keywords","raw_snippet","reason_flagged",
        "parser_used","extraction_method","live_collected",
    ]
    rows = []
    for c in companies:
        for s in c.get("signals",[]):
            conf    = s.get("confidence_score",0)
            er      = s.get("enterprise_relevance_score",0)
            kws     = s.get("extracted_keywords",[])
            reason  = ""
            if conf < 0.50:          reason = "low_confidence"
            elif er < 0.10:          reason = "low_enterprise_relevance"
            elif len(kws) <= 1:      reason = "single_keyword_match"
            elif s.get("parser_used") in ("text_scan","text_regex"): reason = "text_scan_low_fidelity"
            if reason:
                rows.append({
                    "company_name":               c.get("company_name",""),
                    "source_url":                 s.get("source_url",""),
                    "source_type":                s.get("source_type",""),
                    "signal_type":                s.get("signal_type",""),
                    "confidence_score":           conf,
                    "enterprise_relevance_score": er,
                    "keyword_count":              len(kws),
                    "extracted_keywords":         "; ".join(kws),
                    "raw_snippet":                s.get("raw_snippet","")[:200],
                    "reason_flagged":             reason,
                    "parser_used":                s.get("parser_used",""),
                    "extraction_method":          s.get("extraction_method",""),
                    "live_collected":             s.get("live_collected",True),
                })

    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)
    logger.info(f"Written: {path}  ({len(rows)} flagged signals)")
    return len(rows)


def write_source_quality(companies: List[dict], fetch_log_summary: dict, path: Path):
    """Source quality: signal count, success rate, avg confidence by source type."""
    by_source: Dict[str, dict] = defaultdict(lambda: {
        "total_signals":0,"companies_with_data":0,
        "confidence_sum":0.0,"confidence_count":0,
        "signal_types": defaultdict(int),
    })

    for c in companies:
        seen_sources_this_co = set()
        for s in c.get("signals",[]):
            src = s.get("source_type","unknown")
            by_source[src]["total_signals"] += 1
            if src not in seen_sources_this_co:
                by_source[src]["companies_with_data"] += 1
                seen_sources_this_co.add(src)
            conf = s.get("confidence_score",0)
            by_source[src]["confidence_sum"]   += conf
            by_source[src]["confidence_count"] += 1
            by_source[src]["signal_types"][s.get("signal_type","?")] += 1

    source_report = {}
    for src, d in by_source.items():
        avg_conf = (d["confidence_sum"] / d["confidence_count"]) if d["confidence_count"] else 0
        source_report[src] = {
            "total_signals":        d["total_signals"],
            "companies_with_data":  d["companies_with_data"],
            "avg_confidence":       round(avg_conf, 2),
            "signal_type_breakdown":dict(d["signal_types"]),
        }

    quality = {
        "source_breakdown":  source_report,
        "fetch_log_summary": fetch_log_summary,
        "total_signals_all_sources": sum(d["total_signals"] for d in by_source.values()),
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(quality, f, indent=2)
    logger.info(f"Written: {path}")
    return quality


def write_post_run_report(companies: List[dict], source_quality: dict,
                          fp_count: int, target_states: List[str], path: Path) -> str:
    """Human-readable post-run report."""
    all_signals = [s for c in companies for s in c.get("signals",[])]
    surfaced    = sorted([c for c in companies if c.get("scores",{}).get("surfaced")],
                         key=lambda x: x["scores"]["total_score"], reverse=True)
    low_conf    = sorted([s for s in all_signals if s.get("confidence_score",1) < 0.55],
                         key=lambda x: x.get("confidence_score",1))
    co_with_live= sum(1 for c in companies if c.get("collection",{}).get("live_signals_ingested",0)>0)

    lines = []
    lines.append("═"*70)
    lines.append("  TESSELL SIGNAL ENGINE — Phase 2 Post-Run Report")
    lines.append(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  |  Territory: {','.join(target_states)}")
    lines.append("═"*70)
    lines.append("")
    lines.append("── OVERVIEW ──────────────────────────────────────────────────────")
    lines.append(f"  Companies run:               {len(companies)}")
    lines.append(f"  Companies with live signals: {co_with_live}")
    lines.append(f"  Total signals collected:     {len(all_signals)}")
    lines.append(f"  Signals surfaced (scored):   {sum(len(c.get('signals',[])) for c in companies if c.get('scores',{}).get('surfaced'))}")
    lines.append(f"  Accounts surfaced:           {len(surfaced)} / {len(companies)}")
    lines.append(f"  Flagged for FP review:       {fp_count}")
    lines.append("")

    lines.append("── TOP 10 ACCOUNTS BY SCORE ──────────────────────────────────────")
    for i, c in enumerate(surfaced[:10], 1):
        sc   = c["scores"]
        sigs = c.get("collection",{}).get("live_signals_ingested",0)
        good = [k for k,v in c.get("collection",{}).get("per_source",{}).items() if v.get("signals_returned",0)>0]
        lines.append(f"  {i:2}. {c['company_name']:<24} {sc['total_score']:>5.0f}  {sc['heat_level']:<10}"
                     f"  sigs={sigs}  src={','.join(good) or 'none'}")
        if sc.get("score_evidence"):
            for ev in sc["score_evidence"][:3]:
                lines.append(f"       • {ev}")
    if not surfaced:
        lines.append("  No accounts surfaced. Likely cause: 0 live signals collected.")
        lines.append("  Run from laptop (not this sandbox) to get real signal data.")
    lines.append("")

    lines.append("── SIGNALS BY SOURCE ─────────────────────────────────────────────")
    src_bd = source_quality.get("source_breakdown", {})
    for src, d in sorted(src_bd.items(), key=lambda x: -x[1]["total_signals"]):
        lines.append(f"  {src:<28}  {d['total_signals']:>4} signals  "
                     f"avg_conf={d['avg_confidence']:.2f}  "
                     f"cos={d['companies_with_data']}")
    if not src_bd:
        lines.append("  No signals from any source (all blocked in sandbox).")

    lines.append("")
    lines.append("── SOURCE ACCESS SUMMARY ─────────────────────────────────────────")
    fl = source_quality.get("fetch_log_summary", {})
    if fl:
        lines.append(f"  URLs tried:    {fl.get('total_urls_tried',0)}")
        lines.append(f"  Success rate:  {fl.get('success_rate',0):.1f}%")
        for status, count in (fl.get("by_status") or {}).items():
            lines.append(f"  {status:<20}  {count}")
    else:
        lines.append("  Fetch log empty.")

    lines.append("")
    lines.append("── LOWEST CONFIDENCE SIGNALS (review for false positives) ────────")
    for s in low_conf[:10]:
        lines.append(f"  [{s.get('confidence_score',0):.2f}]  {s.get('company_name','')}  |  "
                     f"{s.get('source_type','')}  |  {s.get('raw_snippet','')[:60]}")
    if not low_conf:
        lines.append("  No low-confidence signals found.")

    lines.append("")
    lines.append("── SANDBOX / ENVIRONMENT NOTE ────────────────────────────────────")
    lines.append("  This Claude sandbox allows ONLY: pypi.org, registry.npmjs.org,")
    lines.append("  github.com, api.anthropic.com.")
    lines.append("  All other domains → HTTP 403 'Host not in allowlist'.")
    lines.append("  This IS a network constraint, NOT a code bug.")
    lines.append("  Run from laptop: python run_proof.py → live signals collected.")
    lines.append("═"*70)

    report_text = "\n".join(lines)
    with open(path, "w", encoding="utf-8") as f:
        f.write(report_text)
    logger.info(f"Written: {path}")
    return report_text


# ════════════════════════════════════════════════════════════════════
# MAIN
# ════════════════════════════════════════════════════════════════════

def main():
    p = argparse.ArgumentParser(description="Tessell Signal Engine — Phase 2 Live Proof")
    p.add_argument("--quick",     action="store_true", help="Run first 5 companies only")
    p.add_argument("--company",   type=str,            help="Single company name (exact, case-insensitive)")
    p.add_argument("--states",    type=str, default="TX,OK,KS", help="Target territory states")
    p.add_argument("--log-level", type=str, default="INFO",     help="Log level (DEBUG|INFO)")
    args = p.parse_args()

    # Adjust log level
    if args.log_level.upper() == "DEBUG":
        logger.remove()
        logger.add(sys.stderr, level="DEBUG",
                   format="<cyan>{time:HH:mm:ss}</cyan> | <level>{level:<7}</level> | {name}:{line} | {message}")
        logger.add(str(_log_file), level="DEBUG",
                   format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level:<7} | {name}:{line} | {message}")

    target_states = [s.strip().upper() for s in args.states.split(",")]

    if args.company:
        targets = [c for c in TARGETS if c["name"].lower() == args.company.lower()]
        if not targets:
            valid = [c["name"] for c in TARGETS]
            logger.error(f"Company '{args.company}' not found. Valid: {valid}")
            sys.exit(1)
    elif args.quick:
        targets = TARGETS[:5]
    else:
        targets = TARGETS

    logger.info(f"Tessell Signal Engine — Phase 2")
    logger.info(f"Companies: {len(targets)}  |  Territory: {target_states}  |  Log: {_log_file}")

    FetchLog.reset()

    output = {
        "run_metadata": {
            "timestamp":          datetime.utcnow().isoformat(),
            "companies_run":      len(targets),
            "target_territory":   target_states,
            "data_mode":          "LIVE",
            "log_file":           str(_log_file),
            "report_directory":   str(REPORT_DIR),
        },
        "companies": [],
    }

    t_total = time.time()
    for cd in targets:
        try:
            output["companies"].append(run_one(cd, target_states))
        except Exception as e:
            logger.error(f"Error on {cd['name']}: {e}", exc_info=True)
            output["companies"].append({
                "company_name": cd["name"], "error": str(e),
                "scores": {"total_score":0,"surfaced":False,"heat_level":"COLD"},
                "collection": {"live_signals_ingested":0, "per_source":{}},
                "signals": [],
            })

    output["run_metadata"]["elapsed_seconds"] = round(time.time() - t_total, 1)
    output["run_metadata"]["fetch_log"] = FetchLog.summary()

    companies = output["companies"]

    # ── Write all outputs ────────────────────────────────────────────
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_path = REPORT_DIR / f"proof_output_{ts}.json"
    write_proof_json(output, json_path)
    # Also write canonical latest
    write_proof_json(output, REPORT_DIR / "proof_output.json")

    csv_path = REPORT_DIR / f"proof_summary_{ts}.csv"
    write_summary_csv(companies, csv_path)
    write_summary_csv(companies, REPORT_DIR / "proof_summary.csv")

    fp_path = REPORT_DIR / f"false_positive_report_{ts}.csv"
    fp_count = write_false_positive_csv(companies, fp_path)
    write_false_positive_csv(companies, REPORT_DIR / "false_positive_report.csv")

    sq_path = REPORT_DIR / f"source_quality_{ts}.json"
    source_quality = write_source_quality(companies, FetchLog.summary(), sq_path)
    write_source_quality(companies, FetchLog.summary(), REPORT_DIR / "source_quality.json")

    rpt_path = REPORT_DIR / f"post_run_report_{ts}.txt"
    report_text = write_post_run_report(
        companies, source_quality, fp_count, target_states, rpt_path
    )
    write_post_run_report(companies, source_quality, fp_count, target_states,
                          REPORT_DIR / "post_run_report.txt")

    # ── Print post-run report ────────────────────────────────────────
    print("\n" + report_text)
    print(f"\nOutputs written to: {REPORT_DIR}/")
    print(f"  proof_output.json          Full results")
    print(f"  proof_summary.csv          One row per company")
    print(f"  false_positive_report.csv  {fp_count} signals for review")
    print(f"  source_quality.json        Source success rates")
    print(f"  post_run_report.txt        This report")
    print(f"  Log: {_log_file}")


if __name__ == "__main__":
    main()
