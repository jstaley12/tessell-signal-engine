"""
streamlit_app.py  -  Tessell Signal Engine  |  Rackspace Seller Edition
"""
import json, sys, os, time
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

import streamlit as st
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="Tessell Signal Engine", page_icon="🎯",
                   layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
section[data-testid="stSidebar"]{background:#0D1117;border-right:1px solid #21262D;}
section[data-testid="stSidebar"] *{color:#C9D1D9 !important;}
.main .block-container{background:#0D1117;padding-top:1.5rem;}
h1,h2,h3{font-family:'IBM Plex Sans',sans-serif;color:#E6EDF3 !important;}
div[data-testid="metric-container"]{background:#161B22;border:1px solid #21262D;border-radius:8px;padding:12px 16px;}
div[data-testid="metric-container"] label{color:#8B949E !important;font-size:0.72rem !important;text-transform:uppercase;letter-spacing:0.08em;}
div[data-testid="metric-container"] [data-testid="stMetricValue"]{color:#E6EDF3 !important;font-family:'IBM Plex Mono',monospace !important;font-size:1.6rem !important;}
details{background:#161B22 !important;border:1px solid #21262D !important;border-radius:8px !important;}
hr{border-color:#21262D !important;}
p,li,span,div{color:#C9D1D9;}
</style>
""", unsafe_allow_html=True)

HEAT_COLOR = {"HOT":"#EF4444","WARM":"#F97316","WATCHLIST":"#EAB308",
              "SIGNAL PENDING":"#58A6FF","BASE FIT ONLY":"#8B949E"}
HEAT_EMOJI = {"HOT":"🔴","WARM":"🟠","WATCHLIST":"🟡",
              "SIGNAL PENDING":"🔵","BASE FIT ONLY":"⬜"}
HEAT_BG    = {"HOT":"#2D1515","WARM":"#2D1A0A","WATCHLIST":"#2B2600",
              "SIGNAL PENDING":"#0D1F33","BASE FIT ONLY":"#161B22"}

BUYER_TITLES = {
    "Airlines":                    ["VP Technology","Director Database Engineering","CIO","Head of Platform Engineering"],
    "Healthcare / Distribution":   ["VP Infrastructure","Director Database Services","CIO","CISO"],
    "Healthcare / Insurance":      ["Director Database Engineering","VP Cloud Operations","CIO"],
    "Healthcare / Hospital Systems":["VP IT","Director Database Platform","CIO"],
    "Energy / Midstream":          ["VP IT","Director Infrastructure","Head of Database Ops","CIO"],
    "Energy / Oil & Gas":          ["VP Technology","Director Database Engineering","CIO"],
    "Energy / Refining":           ["VP IT","Director Infrastructure","CIO"],
    "Manufacturing / Industrial":  ["VP IT Infrastructure","Director Database Services","CIO","ERP Program Director"],
    "Aerospace / Manufacturing":   ["VP IT","Director Database Engineering","CIO"],
    "Aerospace / Defense":         ["VP Enterprise Technology","Director Database Services","CIO"],
    "Automotive / Manufacturing":  ["VP IT","Director Database Platform","CIO"],
    "Financial Services / Banking":["VP Database Engineering","Director DBRE","CTO","Head of Platform Engineering"],
    "Telecommunications":          ["VP Infrastructure","Director Database Operations","CTO"],
    "Technology / IT Services":    ["VP Database Engineering","Director SRE","CTO"],
    "Technology / Hardware":       ["VP Cloud Infrastructure","Director Database Services","CTO"],
    "Transportation / Logistics":  ["VP Technology","Director Database Engineering","CIO"],
    "Logistics / Courier":         ["VP IT","Director Database Operations","CIO"],
    "Retail":                      ["VP IT","Director Database Engineering","CIO"],
    "Consumer Goods / Manufacturing":["VP IT","Director Database Platform","CIO"],
    "Pharmaceuticals":             ["VP IT Infrastructure","Director Database Engineering","CIO"],
    "Professional Services":       ["VP Technology","Director Database Services","CTO"],
}

PRESET_FILTERS = {
    "🤠 Texas This Week":     {"states":["TX"],"heat":["HOT","WARM"]},
    "🌾 Kansas This Month":   {"states":["KS"],"heat":["HOT","WARM","WATCHLIST","SIGNAL PENDING","BASE FIT ONLY"]},
    "⛽ Oklahoma Oracle":     {"states":["OK"],"heat":["HOT","WARM","WATCHLIST","SIGNAL PENDING","BASE FIT ONLY"]},
    "🏢 Fortune 1000 Only":   {"states":[],"heat":["HOT","WARM","WATCHLIST"],"tiers":["Fortune 500","Fortune 1000"]},
    "🔥 All HOT Accounts":    {"states":[],"heat":["HOT"]},
    "📋 Show All":            {"states":[],"heat":["HOT","WARM","WATCHLIST","SIGNAL PENDING","BASE FIT ONLY"]},
}

TARGETS = [
    {"name":"McKesson",          "industry":"Healthcare / Distribution",      "employees":51000,  "fortune_rank":9,   "hq_state":"TX","hq_city":"Irving",       "ticker":"MCK",  "greenhouse_slug":None,      "careers_url":"https://www.mckesson.com/Careers/",              "newsroom_url":"https://www.mckesson.com/About-McKesson/Newsroom/","news_terms":["Oracle database","database migration","DBA"]},
    {"name":"AT&T",              "industry":"Telecommunications",              "employees":160000, "fortune_rank":13,  "hq_state":"TX","hq_city":"Dallas",       "ticker":"T",    "greenhouse_slug":None,      "careers_url":"https://www.att.jobs/search-jobs",               "newsroom_url":"https://about.att.com/story/2024/",                "news_terms":["Oracle","database engineer","cloud migration"]},
    {"name":"American Airlines", "industry":"Airlines",                        "employees":95000,  "fortune_rank":69,  "hq_state":"TX","hq_city":"Fort Worth",   "ticker":"AAL",  "greenhouse_slug":None,      "careers_url":"https://jobs.aa.com/",                           "newsroom_url":"https://news.aa.com/",                             "news_terms":["Oracle database","database engineer"]},
    {"name":"Southwest Airlines","industry":"Airlines",                        "employees":65000,  "fortune_rank":75,  "hq_state":"TX","hq_city":"Dallas",       "ticker":"LUV",  "greenhouse_slug":None,      "careers_url":"https://careers.southwestair.com/",              "newsroom_url":None,                                               "news_terms":["database engineer","Oracle migration"]},
    {"name":"Kimberly-Clark",    "industry":"Consumer Goods / Manufacturing",  "employees":46000,  "fortune_rank":184, "hq_state":"TX","hq_city":"Irving",       "ticker":"KMB",  "greenhouse_slug":None,      "careers_url":"https://jobs.kimberly-clark.com/",               "newsroom_url":"https://investor.kimberly-clark.com/press-releases","news_terms":["Oracle","SAP migration","database"]},
    {"name":"ConocoPhillips",    "industry":"Energy / Oil & Gas",              "employees":9700,   "fortune_rank":111, "hq_state":"TX","hq_city":"Houston",      "ticker":"COP",  "greenhouse_slug":None,      "careers_url":"https://careers.conocophillips.com/",            "newsroom_url":"https://investor.conocophillips.com/news-releases","news_terms":["Oracle database","SAP","cloud platform"]},
    {"name":"Phillips 66",       "industry":"Energy / Refining",               "employees":13700,  "fortune_rank":42,  "hq_state":"TX","hq_city":"Houston",      "ticker":"PSX",  "greenhouse_slug":None,      "careers_url":"https://careers.phillips66.com/",                "newsroom_url":"https://investor.phillips66.com/news-releases",    "news_terms":["Oracle database","database engineer"]},
    {"name":"ONEOK",             "industry":"Energy / Midstream",              "employees":3100,   "fortune_rank":218, "hq_state":"OK","hq_city":"Tulsa",        "ticker":"OKE",  "greenhouse_slug":None,      "careers_url":"https://www.oneok.com/About-ONEOK/Careers",      "newsroom_url":"https://www.oneok.com/News",                       "news_terms":["Oracle database","cloud migration","DBA"]},
    {"name":"Devon Energy",      "industry":"Energy / Oil & Gas",              "employees":4300,   "fortune_rank":244, "hq_state":"OK","hq_city":"Oklahoma City","ticker":"DVN",  "greenhouse_slug":None,      "careers_url":"https://www.devonenergy.com/careers",            "newsroom_url":"https://www.devonenergy.com/news",                 "news_terms":["Oracle","SAP","database"]},
    {"name":"Spirit AeroSystems","industry":"Aerospace / Manufacturing",       "employees":13000,  "fortune_rank":412, "hq_state":"KS","hq_city":"Wichita",      "ticker":"SPR",  "greenhouse_slug":None,      "careers_url":"https://jobs.spiritaero.com/",                   "newsroom_url":"https://www.spiritaero.com/company/news/",         "news_terms":["Oracle ERP","cloud migration","database"]},
    {"name":"Cummins",           "industry":"Manufacturing / Industrial",      "employees":59900,  "fortune_rank":147, "hq_state":"IN","hq_city":"Columbus",      "ticker":"CMI",  "greenhouse_slug":"cummins", "careers_url":"https://cummins.wd1.myworkdayjobs.com/RecruiterPortal","newsroom_url":"https://www.cummins.com/news",                "news_terms":["Oracle database","SAP migration","cloud"]},
    {"name":"Eli Lilly",         "industry":"Pharmaceuticals",                 "employees":43000,  "fortune_rank":129, "hq_state":"IN","hq_city":"Indianapolis", "ticker":"LLY",  "greenhouse_slug":None,      "careers_url":"https://lilly.jobs/",                            "newsroom_url":"https://investor.lilly.com/news-releases",         "news_terms":["database engineer","Oracle","DBA"]},
    {"name":"UnitedHealth Group","industry":"Healthcare / Insurance",          "employees":400000, "fortune_rank":7,   "hq_state":"MN","hq_city":"Minnetonka",   "ticker":"UNH",  "greenhouse_slug":None,      "careers_url":"https://careers.unitedhealthgroup.com/",         "newsroom_url":"https://newsroom.uhc.com/",                        "news_terms":["Oracle database","database engineer","DBA"]},
    {"name":"JPMorgan Chase",    "industry":"Financial Services / Banking",    "employees":310000, "fortune_rank":24,  "hq_state":"NY","hq_city":"New York",      "ticker":"JPM",  "greenhouse_slug":None,      "careers_url":"https://careers.jpmorgan.com/",                  "newsroom_url":"https://www.jpmorganchase.com/news",               "news_terms":["Oracle database","database reliability","DBA"]},
    {"name":"Citigroup",         "industry":"Financial Services / Banking",    "employees":240000, "fortune_rank":33,  "hq_state":"NY","hq_city":"New York",      "ticker":"C",    "greenhouse_slug":None,      "careers_url":"https://jobs.citi.com/",                         "newsroom_url":"https://www.citigroup.com/global/news",            "news_terms":["Oracle migration","database modernization"]},
    {"name":"Boeing",            "industry":"Aerospace / Defense",             "employees":150000, "fortune_rank":67,  "hq_state":"VA","hq_city":"Arlington",     "ticker":"BA",   "greenhouse_slug":None,      "careers_url":"https://jobs.boeing.com/",                       "newsroom_url":"https://investors.boeing.com/investors/news-releases","news_terms":["Oracle database","database engineer","SAP"]},
    {"name":"General Motors",    "industry":"Automotive / Manufacturing",      "employees":150000, "fortune_rank":16,  "hq_state":"MI","hq_city":"Detroit",       "ticker":"GM",   "greenhouse_slug":None,      "careers_url":"https://careers.gm.com/",                        "newsroom_url":"https://investor.gm.com/news-releases",            "news_terms":["Oracle database","database platform"]},
    {"name":"HCA Healthcare",    "industry":"Healthcare / Hospital Systems",   "employees":295000, "fortune_rank":57,  "hq_state":"TN","hq_city":"Nashville",     "ticker":"HCA",  "greenhouse_slug":None,      "careers_url":"https://careers.hcahealthcare.com/",             "newsroom_url":"https://investor.hcahealthcare.com/news-releases", "news_terms":["Oracle database","HIPAA database","cloud"]},
    {"name":"Humana",            "industry":"Healthcare / Insurance",          "employees":67000,  "fortune_rank":53,  "hq_state":"KY","hq_city":"Louisville",    "ticker":"HUM",  "greenhouse_slug":"humana",  "careers_url":"https://careers.humana.com/",                    "newsroom_url":"https://newsroom.humana.com/",                     "news_terms":["Oracle","database modernization"]},
    {"name":"J.B. Hunt",         "industry":"Transportation / Logistics",      "employees":35000,  "fortune_rank":409, "hq_state":"AR","hq_city":"Lowell",        "ticker":"JBHT", "greenhouse_slug":None,      "careers_url":"https://www.jbhunt.com/careers/",                "newsroom_url":"https://www.jbhunt.com/news/",                     "news_terms":["Oracle database","database engineer"]},
    {"name":"FedEx",             "industry":"Logistics / Courier",             "employees":500000, "fortune_rank":59,  "hq_state":"TN","hq_city":"Memphis",       "ticker":"FDX",  "greenhouse_slug":None,      "careers_url":"https://careers.fedex.com/",                     "newsroom_url":"https://newsroom.fedex.com/",                      "news_terms":["Oracle database","database engineer"]},
    {"name":"Dollar General",    "industry":"Retail",                          "employees":164000, "fortune_rank":122, "hq_state":"TN","hq_city":"Goodlettsville","ticker":"DG",   "greenhouse_slug":"dollargeneral","careers_url":"https://careers.dollargeneral.com/",          "newsroom_url":"https://investor.dollargeneral.com/news-releases", "news_terms":["Oracle database","database engineer"]},
    {"name":"Cognizant",         "industry":"Technology / IT Services",        "employees":340000, "fortune_rank":185, "hq_state":"NJ","hq_city":"Teaneck",       "ticker":"CTSH", "greenhouse_slug":"cognizant","careers_url":"https://careers.cognizant.com/",                "newsroom_url":"https://investors.cognizant.com/news-releases",    "news_terms":["Oracle DBA","database migration"]},
    {"name":"HP Inc",            "industry":"Technology / Hardware",           "employees":58000,  "fortune_rank":61,  "hq_state":"CA","hq_city":"Palo Alto",     "ticker":"HPQ",  "greenhouse_slug":None,      "careers_url":"https://jobs.hp.com/",                           "newsroom_url":"https://investor.hp.com/press-releases",           "news_terms":["Oracle database","DBA"]},
    {"name":"ManpowerGroup",     "industry":"Professional Services",           "employees":28000,  "fortune_rank":197, "hq_state":"WI","hq_city":"Milwaukee",     "ticker":"MAN",  "greenhouse_slug":None,      "careers_url":"https://careers.manpowergroup.com/",             "newsroom_url":"https://investor.manpowergroup.com/news-releases", "news_terms":["Oracle","DBA"]},
]

def display_heat(heat, live_signals):
    if heat == "COLD" and live_signals == 0:
        return "SIGNAL PENDING"
    if heat == "COLD":
        return "BASE FIT ONLY"
    return heat

def urgency_explanation(heat, live_signals, score):
    if heat == "HOT":    return "Strong fit + active timing signals. Call this week."
    if heat == "WARM":   return "Good fit with some signals. Prioritize this month."
    if heat == "WATCHLIST": return "Qualified account, low urgency. Monitor quarterly."
    if live_signals == 0: return "Enterprise-qualified but no live signals yet. Run a scan to get Pain + Timing scores."
    return "Low signal strength. Nurture list."

@st.cache_data(ttl=300)
def load_data():
    for p in [Path("proof_output.json"), Path("reports/proof_output.json"),
              Path(__file__).parent / "proof_output.json"]:
        if p.exists():
            with open(p) as f:
                return json.load(f)
    return None

def data_to_rows(companies):
    rows = []
    for c in companies:
        sc  = c.get("scores", {})
        cl  = c.get("collection", {})
        eg  = c.get("enterprise_gate", {})
        geo = c.get("geography", {})
        live     = cl.get("live_signals_ingested", 0)
        raw_heat = sc.get("heat_level", "COLD")
        rows.append({
            "company":           c.get("company_name",""),
            "industry":          c.get("industry",""),
            "hq_state":          c.get("hq_state",""),
            "hq_city":           c.get("hq_city",""),
            "fortune_rank":      c.get("fortune_rank"),
            "employees":         c.get("employees",0),
            "tier":              eg.get("tier","").replace("_"," ").title(),
            "total_score":       sc.get("total_score",0),
            "fit_score":         sc.get("fit_score",0),
            "pain_score":        sc.get("pain_score",0),
            "timing_score":      sc.get("timing_score",0),
            "territory_score":   sc.get("territory_score",0),
            "meeting_prop":      sc.get("meeting_propensity",0),
            "raw_heat":          raw_heat,
            "display_heat":      display_heat(raw_heat, live),
            "live_signals":      live,
            "score_evidence":    sc.get("score_evidence",[]),
            "signals":           c.get("signals",[]),
            "per_source":        cl.get("per_source",{}),
            "hq_in_territory":   geo.get("hq_in_territory",False),
            "territory_presence":geo.get("territory_presence",[]),
            "surface_reason":    sc.get("surface_reason",""),
        })
    return rows

data = load_data()
if "scan_data" not in st.session_state:
    st.session_state["scan_data"] = None
active_data = st.session_state["scan_data"] or data

# ── SIDEBAR ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
        <div style='padding:8px 0 16px;'>
            <div style='font-size:1.05rem;font-weight:600;color:#E6EDF3;'>🎯 Tessell Signal Engine</div>
            <div style='font-size:0.68rem;color:#6E7681;text-transform:uppercase;letter-spacing:0.1em;margin-top:3px;'>Rackspace Seller Edition</div>
        </div>
    """, unsafe_allow_html=True)

    if active_data:
        meta = active_data.get("run_metadata",{})
        try:    ts = datetime.fromisoformat(meta.get("timestamp","")).strftime("%b %d %Y %H:%M")
        except: ts = meta.get("timestamp","")[:16]
        mode_color = "#3FB950" if meta.get("data_mode")=="LIVE" else "#F97316"
        st.markdown(f"""
            <div style='background:#161B22;border:1px solid #21262D;border-radius:8px;
                        padding:10px 12px;margin-bottom:12px;font-size:0.76rem;color:#8B949E;'>
                <div><b style='color:#C9D1D9;'>Last run:</b> {ts}</div>
                <div><b style='color:#C9D1D9;'>Companies:</b> {meta.get("companies_run","?")}</div>
                <div><b style='color:#C9D1D9;'>Territory:</b> {", ".join(meta.get("target_territory",[]))}</div>
                <div><b style='color:#C9D1D9;'>Mode:</b> <span style='color:{mode_color};font-weight:600;'>{meta.get("data_mode","?")}</span></div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<div style="font-size:0.68rem;color:#6E7681;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:8px;">Quick Filters</div>', unsafe_allow_html=True)
    preset_choice = st.selectbox("", ["— choose —"] + list(PRESET_FILTERS.keys()), label_visibility="collapsed")
    st.markdown("---")
    st.markdown('<div style="font-size:0.68rem;color:#6E7681;text-transform:uppercase;letter-spacing:0.1em;margin-bottom:8px;">Manual Filters</div>', unsafe_allow_html=True)
    heat_options = ["HOT","WARM","WATCHLIST","SIGNAL PENDING","BASE FIT ONLY"]
    heat_filter  = st.multiselect("Heat Level", heat_options,
                                   default=["HOT","WARM","WATCHLIST","SIGNAL PENDING","BASE FIT ONLY"])
    min_score    = st.slider("Min Score", 0, 100, 0)
    st.markdown("---")
    page = st.radio("View", ["🏆 Territory Rankings","🔍 Account Detail",
                              "🔬 Run Live Scan","📊 Source Quality"],
                    label_visibility="collapsed")

if preset_choice != "— choose —":
    p = PRESET_FILTERS[preset_choice]
    heat_filter = p.get("heat", heat_filter)
    min_score   = p.get("min_score", 0)

if not active_data:
    st.markdown("<div style='text-align:center;padding:80px 40px;'><div style='font-size:3rem;'>🎯</div><div style='font-size:1.3rem;font-weight:600;color:#E6EDF3;'>No data yet</div><div style='color:#8B949E;'>Go to Run Live Scan to collect real signals.</div></div>", unsafe_allow_html=True)
    st.stop()

all_rows = data_to_rows(active_data.get("companies",[]))
df_all   = pd.DataFrame(all_rows)

df = df_all[df_all["display_heat"].isin(heat_filter)].copy()
df = df[df["total_score"] >= min_score]
if preset_choice != "— choose —":
    p = PRESET_FILTERS[preset_choice]
    if p.get("states"):
        target_s = p["states"]
        df = df[df["hq_state"].isin(target_s) |
                df["territory_presence"].apply(lambda tp: any(s in (tp or []) for s in target_s))]
    if p.get("tiers"):
        df = df[df["tier"].isin(p["tiers"])]
df = df.sort_values("total_score", ascending=False).reset_index(drop=True)

# ════════════════════════════════════════════════════════════════════
# TERRITORY RANKINGS
# ════════════════════════════════════════════════════════════════════
if "Rankings" in page:
    hot     = len(df_all[df_all["display_heat"]=="HOT"])
    warm    = len(df_all[df_all["display_heat"]=="WARM"])
    pending = len(df_all[df_all["display_heat"]=="SIGNAL PENDING"])
    live_total = int(df_all["live_signals"].sum())

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Accounts",          len(df_all))
    c2.metric("🔴 HOT",           hot)
    c3.metric("🟠 WARM",          warm)
    c4.metric("🔵 Signal Pending", pending)
    c5.metric("Live Signals",      live_total)

    if live_total == 0:
        st.info("💡 **No live signals yet.** Go to **Run Live Scan** to collect real Pain + Timing data. Current scores show Enterprise Fit + Territory only.")

    st.markdown("---")

    if df.empty:
        st.warning("No accounts match current filters. Try **Show All** in Quick Filters.")
    else:
        for _, row in df.iterrows():
            heat   = row["display_heat"]
            color  = HEAT_COLOR.get(heat,"#6B7280")
            emoji  = HEAT_EMOJI.get(heat,"⬜")
            bg     = HEAT_BG.get(heat,"#161B22")
            score  = row["total_score"]
            buyers = BUYER_TITLES.get(row["industry"],["VP Infrastructure","Director Database Engineering","CIO"])
            urgency = urgency_explanation(row["raw_heat"], row["live_signals"], score)
            fit_pct  = int(row["fit_score"]  / 40 * 100)
            pain_pct = int(row["pain_score"] / 40 * 100)
            time_pct = int(row["timing_score"]/ 20 * 100)
            terr_pct = int(row["territory_score"]/ 20 * 100)
            why_fit  = (f"Fortune {row['fortune_rank']} · " if row["fortune_rank"] else "") + row["tier"]
            tp = row.get("territory_presence") or []
            why_terr = (f"HQ in territory ({row['hq_state']})" if row["hq_in_territory"]
                        else (f"Presence in {', '.join(tp)}" if tp else "No territory presence detected"))

            st.markdown(f"""
<div style='background:{bg};border:1px solid {color}33;border-left:3px solid {color};border-radius:8px;padding:16px 20px;margin-bottom:12px;'>
  <div style='display:flex;justify-content:space-between;align-items:flex-start;'>
    <div style='flex:1;'>
      <div style='display:flex;align-items:center;gap:10px;margin-bottom:4px;flex-wrap:wrap;'>
        <span style='font-size:1.05rem;font-weight:600;color:#E6EDF3;'>{emoji} {row["company"]}</span>
        <span style='font-size:0.7rem;color:#8B949E;background:#21262D;padding:2px 8px;border-radius:20px;'>{row["industry"]}</span>
        {"<span style='font-size:0.7rem;color:#58A6FF;background:#0D2137;padding:2px 8px;border-radius:20px;'>F"+str(row['fortune_rank'])+"</span>" if row["fortune_rank"] else ""}
        <span style='font-size:0.7rem;color:{color};border:1px solid {color}55;padding:2px 8px;border-radius:20px;font-weight:600;'>{heat}</span>
      </div>
      <div style='font-size:0.78rem;color:#8B949E;'>📍 {row["hq_city"]}, {row["hq_state"]} &nbsp;·&nbsp; {row["tier"]} &nbsp;·&nbsp; {row["employees"]:,} employees</div>
    </div>
    <div style='text-align:right;min-width:70px;'>
      <div style='font-size:2.2rem;font-weight:700;font-family:"IBM Plex Mono",monospace;color:{color};line-height:1;'>{score:.0f}</div>
      <div style='font-size:0.65rem;color:#6E7681;'>/ 100</div>
    </div>
  </div>
  <div style='margin-top:12px;display:grid;grid-template-columns:1fr 1fr;gap:6px 20px;'>
    <div><div style='display:flex;justify-content:space-between;font-size:0.7rem;margin-bottom:2px;'><span style='color:#6E7681;'>Enterprise Fit</span><span style='color:#58A6FF;font-family:monospace;'>{row["fit_score"]:.0f}/40</span></div><div style='background:#21262D;border-radius:3px;height:5px;'><div style='background:#58A6FF;width:{fit_pct}%;height:5px;border-radius:3px;'></div></div></div>
    <div><div style='display:flex;justify-content:space-between;font-size:0.7rem;margin-bottom:2px;'><span style='color:#6E7681;'>Database Pain</span><span style='color:#BC8CFF;font-family:monospace;'>{row["pain_score"]:.0f}/40</span></div><div style='background:#21262D;border-radius:3px;height:5px;'><div style='background:#BC8CFF;width:{pain_pct}%;height:5px;border-radius:3px;'></div></div></div>
    <div><div style='display:flex;justify-content:space-between;font-size:0.7rem;margin-bottom:2px;'><span style='color:#6E7681;'>Timing Signal</span><span style='color:#3FB950;font-family:monospace;'>{row["timing_score"]:.0f}/20</span></div><div style='background:#21262D;border-radius:3px;height:5px;'><div style='background:#3FB950;width:{time_pct}%;height:5px;border-radius:3px;'></div></div></div>
    <div><div style='display:flex;justify-content:space-between;font-size:0.7rem;margin-bottom:2px;'><span style='color:#6E7681;'>Territory</span><span style='color:#F97316;font-family:monospace;'>{row["territory_score"]:.0f}/20</span></div><div style='background:#21262D;border-radius:3px;height:5px;'><div style='background:#F97316;width:{terr_pct}%;height:5px;border-radius:3px;'></div></div></div>
  </div>
  <div style='margin-top:12px;padding-top:10px;border-top:1px solid #21262D;display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;font-size:0.72rem;'>
    <div><div style='color:#6E7681;margin-bottom:2px;'>Enterprise fit</div><div style='color:#C9D1D9;'>{why_fit}</div></div>
    <div><div style='color:#6E7681;margin-bottom:2px;'>Territory</div><div style='color:#C9D1D9;'>{why_terr}</div></div>
    <div><div style='color:#6E7681;margin-bottom:2px;'>Live signals</div><div style='color:{"#3FB950" if row["live_signals"]>0 else "#F97316"};'>{row["live_signals"]} collected</div></div>
  </div>
  <div style='margin-top:8px;display:grid;grid-template-columns:1fr 1fr;gap:8px;font-size:0.72rem;'>
    <div><div style='color:#6E7681;margin-bottom:2px;'>Urgency</div><div style='color:#C9D1D9;'>{urgency}</div></div>
    <div><div style='color:#6E7681;margin-bottom:2px;'>Likely buyers</div><div style='color:#C9D1D9;'>{" · ".join(buyers[:3])}</div></div>
  </div>
  {"".join(f'<div style=\"margin-top:4px;font-size:0.71rem;color:#8B949E;\">• {ev}</div>' for ev in row["score_evidence"][:3]) if row["score_evidence"] else ""}
</div>""", unsafe_allow_html=True)

        st.markdown("---")
        export_cols = ["company","hq_state","industry","fortune_rank","total_score",
                       "fit_score","pain_score","timing_score","territory_score",
                       "meeting_prop","display_heat","live_signals","tier"]
        csv = df[export_cols].rename(columns={"display_heat":"heat_level"}).to_csv(index=False)
        st.download_button("⬇ Export CSV", data=csv,
                           file_name=f"tessell_targets_{datetime.now().strftime('%Y%m%d')}.csv",
                           mime="text/csv")

# ════════════════════════════════════════════════════════════════════
# ACCOUNT DETAIL
# ════════════════════════════════════════════════════════════════════
elif "Detail" in page:
    names    = [r["company"] for r in all_rows]
    selected = st.selectbox("Select account", names)
    row      = next((r for r in all_rows if r["company"]==selected), None)
    if not row:
        st.warning("Not found."); st.stop()

    heat   = row["display_heat"]
    color  = HEAT_COLOR.get(heat,"#6B7280")
    emoji  = HEAT_EMOJI.get(heat,"⬜")
    buyers = BUYER_TITLES.get(row["industry"],["VP Infrastructure","Director Database Engineering","CIO"])

    col_l, col_r = st.columns([3,1])
    with col_l:
        f_rank = f" &nbsp;·&nbsp; Fortune {row['fortune_rank']}" if row['fortune_rank'] else ""
        st.markdown(f"<div style='font-size:0.7rem;color:#6E7681;text-transform:uppercase;letter-spacing:0.1em;'>{row['industry']} · {row['tier']}</div><div style='font-size:1.6rem;font-weight:600;color:#E6EDF3;'>{emoji} {row['company']}</div><div style='color:#8B949E;font-size:0.82rem;margin-top:4px;'>📍 {row['hq_city']}, {row['hq_state']} &nbsp;·&nbsp; {row['tier']} &nbsp;·&nbsp; {row['employees']:,} employees{f_rank} &nbsp;·&nbsp; {row['live_signals']} live signals</div>", unsafe_allow_html=True)
    with col_r:
        st.markdown(f"<div style='text-align:right;padding-top:8px;'><div style='font-size:3rem;font-weight:800;color:{color};font-family:\"IBM Plex Mono\",monospace;line-height:1;'>{row['total_score']:.0f}</div><div style='color:{color};font-weight:600;font-size:0.9rem;'>{heat}</div></div>", unsafe_allow_html=True)

    st.markdown("---")
    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Fit",f"{row['fit_score']:.0f}/40")
    c2.metric("Pain",f"{row['pain_score']:.0f}/40")
    c3.metric("Timing",f"{row['timing_score']:.0f}/20")
    c4.metric("Territory",f"{row['territory_score']:.0f}/20")
    c5.metric("Mtg Propensity",f"{row['meeting_prop']:.0f}/100")

    st.markdown("---")
    tab1, tab2, tab3 = st.tabs(["📋 Why This Scored","📡 Signals","👤 Likely Buyers"])

    with tab1:
        col_a, col_b = st.columns(2)
        tp = row.get("territory_presence") or []
        why_terr = (f"HQ in territory ({row['hq_state']})" if row["hq_in_territory"]
                    else (f"Presence in {', '.join(tp)}" if tp else "No territory presence detected"))
        signal_color = "#3FB950" if row["live_signals"]>0 else "#F97316"
        signal_msg   = (f"{row['live_signals']} signals collected" if row["live_signals"]>0
                        else "0 live signals — run a scan to get Pain + Timing scores")
        urgency = urgency_explanation(row["raw_heat"], row["live_signals"], row["total_score"])
        with col_a:
            st.markdown("**Enterprise Fit**")
            frank2 = f" · Fortune {row['fortune_rank']}" if row['fortune_rank'] else ""
            st.markdown(f"<div style='background:#161B22;border:1px solid #21262D;border-left:3px solid #58A6FF;border-radius:6px;padding:10px 14px;font-size:0.82rem;color:#C9D1D9;'>{row['tier']}{frank2}<br>{row['employees']:,} employees · {row['industry']}</div>", unsafe_allow_html=True)
            st.markdown("**Territory Fit**")
            st.markdown(f"<div style='background:#161B22;border:1px solid #21262D;border-left:3px solid #F97316;border-radius:6px;padding:10px 14px;font-size:0.82rem;color:#C9D1D9;'>{why_terr}</div>", unsafe_allow_html=True)
        with col_b:
            st.markdown("**Live Signals Found**")
            st.markdown(f"<div style='background:#161B22;border:1px solid #21262D;border-left:3px solid {signal_color};border-radius:6px;padding:10px 14px;font-size:0.82rem;color:#C9D1D9;'>{signal_msg}</div>", unsafe_allow_html=True)
            st.markdown("**Urgency**")
            st.markdown(f"<div style='background:#161B22;border:1px solid #21262D;border-left:3px solid #EAB308;border-radius:6px;padding:10px 14px;font-size:0.82rem;color:#C9D1D9;'>{urgency}</div>", unsafe_allow_html=True)
        if row["score_evidence"]:
            st.markdown("**Score evidence**")
            for ev in row["score_evidence"]:
                st.markdown(f"<div style='background:#161B22;border:1px solid #21262D;border-left:3px solid #3FB950;border-radius:6px;padding:8px 14px;margin-bottom:5px;font-size:0.8rem;color:#C9D1D9;'>{ev}</div>", unsafe_allow_html=True)

    with tab2:
        sigs = row.get("signals",[])
        if not sigs:
            st.info("No signals yet. Run a live scan for this company.")
        else:
            for sig in sigs:
                cat = sig.get("signal_type","")
                with st.expander(f"[{cat.upper()}] {sig.get('raw_snippet','')[:70]}..."):
                    st.markdown(f"**Source:** `{sig.get('source_type','')}` · **Date:** {sig.get('date_found','')} · **State:** {sig.get('state_detected','?')}")
                    st.markdown(f"**Keywords:** `{'`, `'.join(sig.get('extracted_keywords',[])[:6])}`")
                    st.code(sig.get("raw_snippet",""), language=None)
                    if sig.get("source_url"):
                        st.markdown(f"[View source]({sig['source_url']})")

    with tab3:
        st.markdown("**Likely buyer titles to target at this account:**")
        tier_labels = ["Economic Buyer","Technical Champion","Influencer","Influencer"]
        for i, title in enumerate(buyers[:4]):
            lbl = tier_labels[i] if i < len(tier_labels) else "Influencer"
            st.markdown(f"<div style='display:flex;justify-content:space-between;align-items:center;background:#161B22;border:1px solid #21262D;border-radius:6px;padding:10px 14px;margin-bottom:6px;'><span style='font-size:0.85rem;color:#E6EDF3;font-weight:500;'>{title}</span><span style='font-size:0.7rem;color:#6E7681;background:#21262D;padding:2px 8px;border-radius:20px;'>{lbl}</span></div>", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════════════
# RUN LIVE SCAN  —  full per-source diagnostics
# ════════════════════════════════════════════════════════════════════
elif "Scan" in page:
    st.markdown("### 🔬 Run Live Signal Scan")
    st.markdown("Runs signal collection directly from Streamlit Cloud. Full per-source diagnostics shown after each scan.")

    col1, col2 = st.columns(2)
    with col1:
        target_states  = st.multiselect("Territory", ["TX","OK","KS","AR","NM","AZ","CO","LA"], default=["TX","OK","KS"])
        company_subset = st.selectbox("Companies to scan",
                                       ["Quick test — 3 companies","TX HQ only (7)","OK + KS HQ (3)","All 25"])
    with col2:
        st.markdown("**Sources attempted per company:**")
        st.markdown("- Greenhouse (public JSON API)\n- Lever (public JSON API)\n- Google News RSS\n- Company newsroom HTML\n- Careers page HTML scrape")

    if st.button("▶ Run Live Scan", type="primary"):
        subset_map = {
            "All 25":                   [c["name"] for c in TARGETS],
            "TX HQ only (7)":           [c["name"] for c in TARGETS if c["hq_state"]=="TX"],
            "OK + KS HQ (3)":           [c["name"] for c in TARGETS if c["hq_state"] in ("OK","KS")],
            "Quick test — 3 companies": ["McKesson","ONEOK","Cummins"],
        }
        companies_to_scan = [c for c in TARGETS if c["name"] in subset_map.get(company_subset,[])]

        progress_bar  = st.progress(0)
        status_text   = st.empty()
        results_store = []
        diag_store    = []   # full diagnostics per company
        DB_TECH = {"oracle","sql server","postgresql","postgres","mysql",
                   "mongodb","aurora","rds","azure sql","db2","mariadb"}

        try:
            from collectors.live_collectors import collect_all
            from collectors.fetcher import FetchLog
            from scoring.scorer import (TessellScorer, enterprise_gate,
                                        detect_hiring_states, extract_states_from_text)
            scorer_obj = TessellScorer()

            for i, cd in enumerate(companies_to_scan):
                name = cd["name"]
                status_text.markdown(f"🔍 Scanning **{name}** ({i+1}/{len(companies_to_scan)})...")

                # Reset fetch log for this company so we get clean per-company data
                FetchLog.reset()
                t_company_start = time.time()

                col_result = collect_all(
                    company=name,
                    greenhouse_slug=cd.get("greenhouse_slug"),
                    careers_url=cd.get("careers_url"),
                    newsroom_url=cd.get("newsroom_url"),
                    news_terms=cd.get("news_terms"),
                )

                t_company_elapsed = round(time.time() - t_company_start, 1)
                signals    = col_result["signals"]
                fetch_log  = FetchLog.entries.copy()   # capture before next company resets it

                # Build per-source diagnostics dict — one entry per source
                per_source_diag = {}
                for src, meta in col_result.get("per_source", {}).items():
                    # Find all FetchLog entries that belong to this source
                    src_entries = [e for e in fetch_log if e.source_name == src
                                   or (src in e.source_name) or (e.source_name in src)]

                    # Determine failure reason
                    status      = meta.get("access_status", "unknown")
                    # Use the right field name per source type
                    if src in ("greenhouse","lever"):
                        total_found = meta.get("total_jobs_fetched", 0)
                    elif src in ("careers_page","newsroom","ir_page","workday","icims"):
                        total_found = meta.get("raw_candidates", 0)
                    elif src in ("google_news","google_news_rss"):
                        total_found = meta.get("raw_items_fetched", 0)
                    else:
                        total_found = meta.get("total_jobs_fetched") or meta.get("raw_candidates") or meta.get("raw_items_fetched") or 0
                    noise_out   = meta.get("noise_filtered", 0)
                    accepted    = meta.get("relevant_signals", 0)
                    strategy    = meta.get("strategy_used", "")
                    limitation  = meta.get("limitation", "")
                    endpoint    = meta.get("endpoint") or meta.get("url") or meta.get("proof_url") or ""

                    # HTTP details from FetchLog
                    http_codes   = [e.http_code for e in src_entries if e.http_code]
                    elapsed_list = [e.elapsed_ms for e in src_entries]
                    content_lens = [e.content_len for e in src_entries if e.content_len]
                    notes        = list(set(e.note for e in src_entries if e.note))
                    statuses     = list(set(e.status for e in src_entries))

                    # Determine failure reason bucket
                    if accepted > 0:
                        failure_reason = None
                    elif status == "no_slug":
                        failure_reason = "no_slug_configured"
                    elif status == "not_implemented":
                        failure_reason = "not_implemented_phase3"
                    elif "robots" in str(statuses):
                        failure_reason = "robots_txt_blocked"
                    elif "http_403" in str(statuses) or 403 in http_codes:
                        failure_reason = "http_403_forbidden"
                    elif "http_404" in str(statuses) or 404 in http_codes:
                        failure_reason = "http_404_not_found"
                    elif "timeout" in str(statuses):
                        failure_reason = "timeout"
                    elif "connection_error" in str(statuses):
                        failure_reason = "connection_error"
                    elif status == "success" and total_found == 0:
                        failure_reason = "connected_but_no_records_found"
                    elif status == "success" and total_found > 0 and noise_out >= total_found:
                        failure_reason = "keyword_filter_removed_all"
                    elif status == "success" and total_found > 0:
                        failure_reason = "parser_found_records_but_0_passed_filter"
                    elif limitation:
                        failure_reason = f"partial_js_render_needed"
                    elif status in ("error", "unknown"):
                        failure_reason = f"error: {notes[0][:60] if notes else 'unknown'}"
                    else:
                        failure_reason = f"status={status}"

                    # Grab raw sample snippets from accepted signals for this source
                    source_signals = [s for s in signals
                                      if (s.source_type if hasattr(s,"source_type") else s.get("source_type","")) == src
                                      or src in (s.source_type if hasattr(s,"source_type") else s.get("source_type",""))]
                    samples = []
                    for sig in source_signals[:2]:
                        snippet = sig.raw_snippet if hasattr(sig,"raw_snippet") else sig.get("raw_snippet","")
                        samples.append(snippet[:200])

                    per_source_diag[src] = {
                        "endpoint":        endpoint,
                        "http_codes":      http_codes,
                        "http_statuses":   statuses,
                        "content_lengths": content_lens,
                        "elapsed_ms":      sum(elapsed_list) if elapsed_list else 0,
                        "total_fetched":   total_found,
                        "noise_filtered":  noise_out,
                        "accepted":        accepted,
                        "strategy":        strategy,
                        "failure_reason":  failure_reason,
                        "limitation":      limitation,
                        "notes":           notes,
                        "raw_samples":     samples,
                        "fetch_entries":   len(src_entries),
                    }

                diag_store.append({
                    "company":          name,
                    "total_signals":    len(signals),
                    "elapsed_seconds":  t_company_elapsed,
                    "per_source":       per_source_diag,
                    "fetch_log_count":  len(fetch_log),
                })

                # Scoring (unchanged from before)
                all_text = " ".join(
                    (s.raw_snippet if hasattr(s,"raw_snippet") else s.get("raw_snippet",""))
                    for s in signals
                )
                sc_sigs = []
                for s in signals:
                    d  = s.to_dict() if hasattr(s,"to_dict") else s
                    kw = d.get("extracted_keywords",[])
                    sc_sigs.append({
                        "raw_title":     d.get("raw_snippet","")[:100],
                        "raw_excerpt":   d.get("raw_snippet",""),
                        "source_type":   d.get("source_type",""),
                        "signal_category": d.get("signal_type",""),
                        "keywords_matched": kw,
                        "database_technologies_mentioned": [k for k in kw if k in DB_TECH],
                        "signal_state":  d.get("state_detected"),
                        "signal_city":   d.get("city_detected"),
                        "signal_date":   d.get("date_found"),
                        "confidence":    d.get("confidence_score",0.7),
                        "signal_strength": "strong" if d.get("confidence_score",0)>=0.85 else "moderate",
                    })

                gate = enterprise_gate(
                    company_name=name, estimated_employees=cd.get("employees"),
                    all_text=all_text, known_public=bool(cd.get("ticker")),
                    known_fortune_rank=cd.get("fortune_rank"),
                )
                hq_state      = cd.get("hq_state","")
                hiring_states = detect_hiring_states(sc_sigs)
                text_states   = extract_states_from_text(all_text)
                office_states = [s for s in text_states if s != hq_state]
                signal_states = list(set(
                    (s.state_detected if hasattr(s,"state_detected") else s.get("state_detected"))
                    for s in signals
                    if (s.state_detected if hasattr(s,"state_detected") else s.get("state_detected"))
                ))

                if gate.passes:
                    r = scorer_obj.score(
                        company_name=name, signals=sc_sigs, enterprise_gate_result=gate,
                        hq_state=hq_state, office_states=office_states,
                        hiring_states=hiring_states, signal_states=signal_states,
                        target_states=target_states, industry=cd.get("industry",""),
                    )
                    scores = {
                        "fit_score":r.fit.capped,"pain_score":r.pain.capped,
                        "timing_score":r.timing.capped,"territory_score":r.territory.capped,
                        "total_score":r.total_score,"meeting_propensity":r.meeting_propensity,
                        "heat_level":r.heat_level,"surfaced":r.surfaced,
                        "surface_reason":r.surface_reason,"score_evidence":r.score_notes,
                        "fit_rules":r.fit.rules_fired,"pain_rules":r.pain.rules_fired,
                        "timing_rules":r.timing.rules_fired,"territory_rules":r.territory.rules_fired,
                    }
                else:
                    scores = {"fit_score":0,"pain_score":0,"timing_score":0,"territory_score":0,
                              "total_score":0,"meeting_propensity":0,"heat_level":"COLD",
                              "surfaced":False,"surface_reason":f"Gate failed: {gate.reason}",
                              "score_evidence":[],"fit_rules":[],"pain_rules":[],"timing_rules":[],"territory_rules":[]}

                results_store.append({
                    "company_name":name,"industry":cd["industry"],
                    "fortune_rank":cd.get("fortune_rank"),"employees":cd.get("employees"),
                    "hq_state":hq_state,"hq_city":cd.get("hq_city",""),"ticker":cd.get("ticker",""),
                    "collection":{
                        "live_signals_ingested":len(signals),"total_raw":col_result["total_raw"],
                        "false_positives_removed":col_result["false_positives_removed"],
                        "per_source":{src:{"signals_returned":m.get("relevant_signals",0),
                                           "access_status":m.get("access_status")}
                                      for src,m in col_result.get("per_source",{}).items()},
                    },
                    "enterprise_gate":{"passes":gate.passes,"tier":gate.tier,
                                       "reason":gate.reason,"confidence":gate.confidence},
                    "geography":{
                        "hq_state":hq_state,"detected_hiring_states":hiring_states,
                        "detected_office_states":list(set(office_states))[:6],
                        "signal_states":signal_states,"target_states":target_states,
                        "hq_in_territory":hq_state in target_states,
                        "territory_presence":list(set(
                            [s for s in hiring_states if s in target_states]+
                            [s for s in office_states if s in target_states]
                        )),
                    },
                    "scores":scores,
                    "signals":[s.to_dict() if hasattr(s,"to_dict") else s for s in signals],
                })
                progress_bar.progress((i+1)/len(companies_to_scan))

            # ── Store results ─────────────────────────────────────────
            scan_output = {
                "run_metadata":{
                    "timestamp":  datetime.utcnow().isoformat(),
                    "companies_run": len(companies_to_scan),
                    "target_territory": target_states,
                    "data_mode": "LIVE",
                },
                "companies": results_store,
            }
            st.session_state["scan_data"]  = scan_output
            st.session_state["diag_data"]  = diag_store
            progress_bar.progress(1.0)

            total_sigs = sum(c["collection"]["live_signals_ingested"] for c in results_store)
            hot_count  = sum(1 for c in results_store if c["scores"]["heat_level"]=="HOT")
            warm_count = sum(1 for c in results_store if c["scores"]["heat_level"]=="WARM")
            status_text.markdown("✅ **Scan complete!**")
            st.success(f"**{len(results_store)} companies · {total_sigs} live signals · {hot_count} HOT · {warm_count} WARM**")

        except Exception as e:
            st.error(f"Scan error: {e}")
            st.exception(e)

    # ── DIAGNOSTICS — always show after scan ─────────────────────────
    diag_data = st.session_state.get("diag_data", [])
    if diag_data:
        st.markdown("---")
        st.markdown("### 🔎 Per-Source Diagnostics")
        st.caption("Showing exactly what happened for every source on every company.")

        STATUS_ICON = {
            None:                                  "✅",
            "no_slug_configured":                  "⬜",
            "not_implemented_phase3":              "⬜",
            "robots_txt_blocked":                  "🚫",
            "http_403_forbidden":                  "🔒",
            "http_404_not_found":                  "❌",
            "timeout":                             "⏱",
            "connection_error":                    "🔌",
            "connected_but_no_records_found":      "📭",
            "keyword_filter_removed_all":          "🔍",
            "parser_found_records_but_0_passed_filter": "🔍",
            "partial_js_render_needed":            "⚠️",
        }

        for diag in diag_data:
            co_name   = diag["company"]
            total     = diag["total_signals"]
            elapsed   = diag["elapsed_seconds"]
            color     = "#3FB950" if total > 0 else "#F97316"
            sig_label = f"{total} signal{'s' if total!=1 else ''}"

            with st.expander(f"{'✅' if total>0 else '⬜'} {co_name}  —  {sig_label}  ({elapsed}s)", expanded=(total==0)):
                per_src = diag.get("per_source", {})
                if not per_src:
                    st.warning("No per-source data captured.")
                    continue

                for src_name, d in per_src.items():
                    accepted = d.get("accepted", 0)
                    failure  = d.get("failure_reason")
                    icon     = STATUS_ICON.get(failure, "⬜") if accepted == 0 else "✅"
                    elapsed_ms = d.get("elapsed_ms", 0)
                    http_codes = d.get("http_codes", [])
                    content_lens = d.get("content_lengths", [])
                    statuses = d.get("http_statuses", [])
                    fetched  = d.get("total_fetched", 0)
                    noise    = d.get("noise_filtered", 0)
                    strategy = d.get("strategy", "")
                    endpoint = d.get("endpoint", "")
                    notes    = d.get("notes", [])
                    samples  = d.get("raw_samples", [])
                    limitation = d.get("limitation","")

                    # ── source row ─────────────────────────────────
                    st.markdown(f"""
<div style='background:#161B22;border:1px solid #21262D;border-left:3px solid {"#3FB950" if accepted>0 else "#6E7681"};
            border-radius:6px;padding:10px 14px;margin-bottom:8px;'>
  <div style='display:flex;justify-content:space-between;align-items:flex-start;'>
    <div style='flex:1;'>
      <div style='font-size:0.85rem;font-weight:600;color:#E6EDF3;margin-bottom:6px;'>
        {icon} {src_name}
      </div>
      <div style='display:grid;grid-template-columns:repeat(4,1fr);gap:6px;font-size:0.72rem;'>
        <div><span style='color:#6E7681;'>HTTP</span><br>
             <span style='color:#C9D1D9;font-family:monospace;'>{", ".join(str(c) for c in http_codes) if http_codes else "—"}</span></div>
        <div><span style='color:#6E7681;'>Response size</span><br>
             <span style='color:#C9D1D9;font-family:monospace;'>{f"{max(content_lens):,} chars" if content_lens else "—"}</span></div>
        <div><span style='color:#6E7681;'>Records found</span><br>
             <span style='color:#C9D1D9;font-family:monospace;'>{fetched}</span></div>
        <div><span style='color:#6E7681;'>Runtime</span><br>
             <span style='color:#C9D1D9;font-family:monospace;'>{elapsed_ms}ms</span></div>
        <div><span style='color:#6E7681;'>Noise filtered</span><br>
             <span style='color:#C9D1D9;font-family:monospace;'>{noise}</span></div>
        <div><span style='color:#6E7681;'>Accepted</span><br>
             <span style='color:{"#3FB950" if accepted>0 else "#F97316"};font-family:monospace;font-weight:600;'>{accepted}</span></div>
        <div><span style='color:#6E7681;'>Parser</span><br>
             <span style='color:#C9D1D9;font-family:monospace;'>{strategy or "—"}</span></div>
        <div><span style='color:#6E7681;'>HTTP statuses</span><br>
             <span style='color:#C9D1D9;font-family:monospace;'>{", ".join(statuses) if statuses else "—"}</span></div>
      </div>
      {f'<div style="margin-top:6px;font-size:0.72rem;"><span style="color:#6E7681;">Endpoint: </span><span style="color:#58A6FF;font-family:monospace;">{endpoint[:80]}</span></div>' if endpoint else ""}
      {f'<div style="margin-top:4px;font-size:0.72rem;background:#2D1A0A;border-left:3px solid #F97316;padding:4px 8px;border-radius:4px;color:#F97316;">Failure reason: {failure}</div>' if failure else ""}
      {f'<div style="margin-top:4px;font-size:0.72rem;color:#8B949E;">Limitation: {limitation}</div>' if limitation else ""}
      {f'<div style="margin-top:4px;font-size:0.72rem;color:#8B949E;">Note: {"; ".join(notes[:2])}</div>' if notes else ""}
    </div>
  </div>
  {"""<div style='margin-top:8px;border-top:1px solid #21262D;padding-top:6px;'>
    <div style='font-size:0.7rem;color:#6E7681;margin-bottom:4px;'>Raw sample snippets:</div>""" +
    "".join(f"<div style='font-size:0.71rem;font-family:monospace;color:#C9D1D9;background:#0D1117;padding:4px 8px;border-radius:4px;margin-bottom:3px;white-space:pre-wrap;'>{s[:200]}</div>" for s in samples) +
    "</div>"
  if samples else ""}
</div>
                    """, unsafe_allow_html=True)

        st.markdown("---")
        # Download diagnostics as JSON
        st.download_button(
            "⬇ Download full diagnostics JSON",
            data=json.dumps({
                "diagnostics": diag_data,
                "summary": {
                    "total_companies": len(diag_data),
                    "companies_with_signals": sum(1 for d in diag_data if d["total_signals"]>0),
                    "total_signals": sum(d["total_signals"] for d in diag_data),
                    "sources_attempted": list(set(src for d in diag_data for src in d["per_source"])),
                }
            }, indent=2, default=str),
            file_name="scan_diagnostics.json",
            mime="application/json",
        )

        if st.session_state.get("scan_data"):
            st.download_button(
                "⬇ Download proof_output.json (commit to GitHub)",
                data=json.dumps(st.session_state["scan_data"], indent=2, default=str),
                file_name="proof_output.json",
                mime="application/json",
            )

# ════════════════════════════════════════════════════════════════════
# SOURCE QUALITY
# ════════════════════════════════════════════════════════════════════
elif "Source" in page:
    st.markdown("### 📊 Source Quality")
    fetch_log  = active_data.get("run_metadata",{}).get("fetch_log",{})
    by_status  = fetch_log.get("by_status",{}) if fetch_log else {}

    if fetch_log:
        c1,c2,c3 = st.columns(3)
        c1.metric("URLs Tried",   fetch_log.get("total_urls_tried",0))
        c2.metric("Success Rate", f"{fetch_log.get('success_rate',0):.1f}%")
        c3.metric("Successful",   by_status.get("success",0))
        st.markdown("---")
        if by_status:
            st.dataframe(pd.DataFrame([{"Status":k,"Count":v} for k,v in sorted(by_status.items(),key=lambda x:-x[1])]),
                         use_container_width=True, hide_index=True)
    else:
        st.info("No fetch log in current data. Run a live scan to see source quality.")

    st.markdown("---")
    st.markdown("**Signal coverage by company**")
    coverage = [{"Company":r["company"],"Live Signals":r["live_signals"],
                 "Score":f"{r['total_score']:.0f}","Heat":r["display_heat"]} for r in all_rows]
    st.dataframe(pd.DataFrame(coverage).sort_values("Live Signals",ascending=False),
                 use_container_width=True, hide_index=True)
