"""
streamlit_app.py  —  Tessell Signal Engine Dashboard
Reads proof_output.json and displays ranked enterprise accounts.
Run locally:  streamlit run streamlit_app.py
Deploy:       streamlit.io → connect GitHub repo → set main file
"""

import json
import os
import streamlit as st
import pandas as pd
from pathlib import Path
from datetime import datetime

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Tessell Signal Engine",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS — dark enterprise aesthetic ────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;500;600&display=swap');

html, body, [class*="css"] {
    font-family: 'IBM Plex Sans', sans-serif;
}

/* Dark sidebar */
section[data-testid="stSidebar"] {
    background-color: #0D1117;
    border-right: 1px solid #21262D;
}
section[data-testid="stSidebar"] * {
    color: #C9D1D9 !important;
}

/* Main background */
.main .block-container {
    background-color: #0D1117;
    padding-top: 1.5rem;
}

/* Headers */
h1, h2, h3 { font-family: 'IBM Plex Sans', sans-serif; color: #E6EDF3 !important; }

/* Metric cards */
div[data-testid="metric-container"] {
    background: #161B22;
    border: 1px solid #21262D;
    border-radius: 8px;
    padding: 12px 16px;
}
div[data-testid="metric-container"] label {
    color: #8B949E !important;
    font-size: 0.72rem !important;
    text-transform: uppercase;
    letter-spacing: 0.08em;
}
div[data-testid="metric-container"] [data-testid="stMetricValue"] {
    color: #E6EDF3 !important;
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 1.6rem !important;
}

/* Dataframe */
div[data-testid="stDataFrame"] {
    border: 1px solid #21262D;
    border-radius: 8px;
}

/* Expander */
details {
    background: #161B22 !important;
    border: 1px solid #21262D !important;
    border-radius: 8px !important;
}

/* Selectbox / multiselect */
div[data-baseweb="select"] {
    background: #161B22 !important;
}

/* Info boxes */
div[data-testid="stInfo"] {
    background: #0D2137 !important;
    border-color: #1F6FEB !important;
}

/* Divider */
hr { border-color: #21262D !important; }

/* General text */
p, li, span, div { color: #C9D1D9; }
</style>
""", unsafe_allow_html=True)


# ── Load data ─────────────────────────────────────────────────────────────────

@st.cache_data
def load_data():
    """Load proof_output.json — from repo root or reports/ directory."""
    candidates = [
        Path("proof_output.json"),
        Path("reports/proof_output.json"),
        Path(__file__).parent / "proof_output.json",
        Path(__file__).parent / "reports" / "proof_output.json",
    ]
    for p in candidates:
        if p.exists():
            with open(p) as f:
                return json.load(f)
    return None


data = load_data()


# ── Heat colors / emoji ───────────────────────────────────────────────────────

HEAT_COLOR = {"HOT": "#EF4444", "WARM": "#F97316", "WATCHLIST": "#EAB308", "COLD": "#6B7280"}
HEAT_EMOJI = {"HOT": "🔴", "WARM": "🟠", "WATCHLIST": "🟡", "COLD": "⚫"}
HEAT_BG    = {"HOT": "#2D1515", "WARM": "#2D1A0A", "WATCHLIST": "#2B2600", "COLD": "#1A1A1A"}


# ── Sidebar ───────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("""
        <div style='padding: 8px 0 20px;'>
            <div style='font-size:1.05rem; font-weight:600; color:#E6EDF3; letter-spacing:-0.01em;'>
                🎯 Tessell Signal Engine
            </div>
            <div style='font-size:0.68rem; color:#6E7681; text-transform:uppercase;
                        letter-spacing:0.1em; margin-top:3px;'>
                Rackspace Seller Edition
            </div>
        </div>
    """, unsafe_allow_html=True)

    if data:
        meta   = data.get("run_metadata", {})
        ts_raw = meta.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_raw).strftime("%b %d, %Y %H:%M")
        except Exception:
            ts = ts_raw[:16]

        st.markdown(f"""
            <div style='background:#161B22; border:1px solid #21262D; border-radius:8px;
                        padding:12px; margin-bottom:16px; font-size:0.78rem; color:#8B949E;'>
                <div><b style='color:#C9D1D9;'>Last run:</b> {ts}</div>
                <div><b style='color:#C9D1D9;'>Companies:</b> {meta.get('companies_run', '?')}</div>
                <div><b style='color:#C9D1D9;'>Territory:</b>
                     {', '.join(meta.get('target_territory', []))}</div>
                <div><b style='color:#C9D1D9;'>Mode:</b>
                     <span style='color:{"#3FB950" if meta.get("data_mode")=="LIVE" else "#F97316"};'>
                     {meta.get('data_mode','?')}</span></div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")
    st.markdown('<div style="font-size:0.68rem; color:#6E7681; text-transform:uppercase; letter-spacing:0.1em; margin-bottom:8px;">Filters</div>', unsafe_allow_html=True)

    heat_filter = st.multiselect(
        "Heat Level",
        ["HOT", "WARM", "WATCHLIST", "COLD"],
        default=["HOT", "WARM", "WATCHLIST"],
    )
    min_score = st.slider("Min Score", 0, 100, 0)

    st.markdown("---")
    page = st.radio(
        "View",
        ["🏆 Territory Rankings", "🔍 Account Detail", "📊 Source Quality", "📋 Run a New Scan"],
        label_visibility="collapsed",
    )


# ── No data state ─────────────────────────────────────────────────────────────

if not data:
    st.markdown("""
        <div style='text-align:center; padding:80px 40px;'>
            <div style='font-size:3rem; margin-bottom:16px;'>🎯</div>
            <div style='font-size:1.3rem; font-weight:600; color:#E6EDF3; margin-bottom:8px;'>
                No data yet
            </div>
            <div style='color:#8B949E; font-size:0.9rem; max-width:480px; margin:0 auto;'>
                Run the proof script first, then reload this page.
            </div>
            <div style='background:#161B22; border:1px solid #21262D; border-radius:8px;
                        padding:16px; margin:24px auto; max-width:380px;
                        font-family:monospace; font-size:0.82rem; color:#3FB950; text-align:left;'>
                python run_proof.py
            </div>
        </div>
    """, unsafe_allow_html=True)
    st.stop()


# ── Build dataframe ───────────────────────────────────────────────────────────

companies = data.get("companies", [])

rows = []
for c in companies:
    sc  = c.get("scores", {})
    cl  = c.get("collection", {})
    eg  = c.get("enterprise_gate", {})
    geo = c.get("geography", {})
    rows.append({
        "id":             c.get("company_name",""),
        "company":        c.get("company_name",""),
        "industry":       c.get("industry",""),
        "hq_state":       c.get("hq_state",""),
        "hq_city":        c.get("hq_city",""),
        "fortune_rank":   c.get("fortune_rank"),
        "employees":      c.get("employees",0),
        "tier":           eg.get("tier","").replace("_"," ").title(),
        "total_score":    sc.get("total_score", 0),
        "fit_score":      sc.get("fit_score", 0),
        "pain_score":     sc.get("pain_score", 0),
        "timing_score":   sc.get("timing_score", 0),
        "territory_score":sc.get("territory_score", 0),
        "meeting_prop":   sc.get("meeting_propensity", 0),
        "heat_level":     sc.get("heat_level", "COLD"),
        "surfaced":       sc.get("surfaced", False),
        "live_signals":   cl.get("live_signals_ingested", 0),
        "surface_reason": sc.get("surface_reason",""),
        "hq_in_territory":geo.get("hq_in_territory", False),
        "score_evidence": sc.get("score_evidence", []),
        "signals":        c.get("signals", []),
        "per_source":     cl.get("per_source", {}),
    })

df = pd.DataFrame(rows)

# Apply filters
if heat_filter:
    df = df[df["heat_level"].isin(heat_filter)]
df = df[df["total_score"] >= min_score]
df_sorted = df.sort_values("total_score", ascending=False).reset_index(drop=True)


# ════════════════════════════════════════════════════════════════════
# PAGE: TERRITORY RANKINGS
# ════════════════════════════════════════════════════════════════════

if "Rankings" in page:

    # ── Top metrics ───────────────────────────────────────────────
    all_df = pd.DataFrame(rows)
    hot    = len(all_df[all_df["heat_level"] == "HOT"])
    warm   = len(all_df[all_df["heat_level"] == "WARM"])
    total  = len(all_df)
    avg_sc = all_df["total_score"].mean() if len(all_df) else 0
    live   = all_df["live_signals"].sum()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Accounts",   total)
    c2.metric("🔴 HOT",    hot)
    c3.metric("🟠 WARM",   warm)
    c4.metric("Avg Score",  f"{avg_sc:.0f}")
    c5.metric("Live Signals", live)

    st.markdown("---")

    if df_sorted.empty:
        st.info("No accounts match the current filters.")
    else:
        # ── Account cards ─────────────────────────────────────────
        for _, row in df_sorted.iterrows():
            heat  = row["heat_level"]
            color = HEAT_COLOR.get(heat, "#6B7280")
            emoji = HEAT_EMOJI.get(heat, "⚫")
            bg    = HEAT_BG.get(heat, "#1A1A1A")
            score = row["total_score"]

            with st.container():
                st.markdown(f"""
                <div style='background:{bg}; border:1px solid {color}33;
                            border-left:3px solid {color};
                            border-radius:8px; padding:14px 18px; margin-bottom:10px;'>
                  <div style='display:flex; justify-content:space-between; align-items:flex-start;'>
                    <div style='flex:1;'>
                      <div style='display:flex; align-items:center; gap:10px; margin-bottom:4px;'>
                        <span style='font-size:1.05rem; font-weight:600; color:#E6EDF3;'>
                          {emoji} {row['company']}
                        </span>
                        <span style='font-size:0.7rem; color:#8B949E; background:#21262D;
                                     padding:2px 8px; border-radius:20px;'>
                          {row['industry']}
                        </span>
                        {'<span style="font-size:0.7rem; color:#58A6FF; background:#0D2137; padding:2px 8px; border-radius:20px;">F' + str(row['fortune_rank']) + '</span>' if row['fortune_rank'] else ''}
                      </div>
                      <div style='font-size:0.78rem; color:#8B949E;'>
                        📍 {row['hq_city']}, {row['hq_state']}
                        &nbsp;·&nbsp; {row['tier']}
                        &nbsp;·&nbsp; {row['employees']:,} employees
                        &nbsp;·&nbsp; {row['live_signals']} live signals
                      </div>
                    </div>
                    <div style='text-align:right; min-width:80px;'>
                      <div style='font-size:2rem; font-weight:700;
                                  font-family:"IBM Plex Mono",monospace; color:{color};
                                  line-height:1;'>
                        {score:.0f}
                      </div>
                      <div style='font-size:0.65rem; color:#6E7681; text-transform:uppercase;
                                  letter-spacing:0.1em;'>
                        {heat}
                      </div>
                    </div>
                  </div>

                  <div style='display:flex; gap:20px; margin-top:10px; padding-top:10px;
                              border-top:1px solid #21262D;'>
                    <div style='font-size:0.75rem;'>
                      <span style='color:#6E7681;'>Fit</span>
                      <span style='color:#58A6FF; font-family:monospace; font-weight:600;
                                   margin-left:6px;'>{row['fit_score']:.0f}</span>
                    </div>
                    <div style='font-size:0.75rem;'>
                      <span style='color:#6E7681;'>Pain</span>
                      <span style='color:#BC8CFF; font-family:monospace; font-weight:600;
                                   margin-left:6px;'>{row['pain_score']:.0f}</span>
                    </div>
                    <div style='font-size:0.75rem;'>
                      <span style='color:#6E7681;'>Timing</span>
                      <span style='color:#3FB950; font-family:monospace; font-weight:600;
                                   margin-left:6px;'>{row['timing_score']:.0f}</span>
                    </div>
                    <div style='font-size:0.75rem;'>
                      <span style='color:#6E7681;'>Territory</span>
                      <span style='color:#F97316; font-family:monospace; font-weight:600;
                                   margin-left:6px;'>{row['territory_score']:.0f}</span>
                    </div>
                    <div style='font-size:0.75rem; margin-left:auto;'>
                      <span style='color:#6E7681;'>Mtg propensity</span>
                      <span style='color:#E6EDF3; font-family:monospace; font-weight:600;
                                   margin-left:6px;'>{row['meeting_prop']:.0f}</span>
                    </div>
                  </div>

                  {"".join(f"<div style='margin-top:6px; font-size:0.72rem; color:#8B949E;'>• {ev}</div>" for ev in row['score_evidence'][:3]) if row['score_evidence'] else ""}
                </div>
                """, unsafe_allow_html=True)

        # ── CSV download ──────────────────────────────────────────
        st.markdown("---")
        export_cols = ["company","hq_state","industry","fortune_rank","total_score",
                       "fit_score","pain_score","timing_score","territory_score",
                       "meeting_prop","heat_level","live_signals","tier"]
        csv = df_sorted[export_cols].to_csv(index=False)
        st.download_button(
            "⬇ Export to CSV",
            data=csv,
            file_name=f"tessell_targets_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )


# ════════════════════════════════════════════════════════════════════
# PAGE: ACCOUNT DETAIL
# ════════════════════════════════════════════════════════════════════

elif "Detail" in page:

    all_names = [r["company"] for r in rows]
    selected  = st.selectbox("Select account", all_names)

    match = next((r for r in rows if r["company"] == selected), None)
    if not match:
        st.warning("Account not found.")
        st.stop()

    heat  = match["heat_level"]
    color = HEAT_COLOR.get(heat, "#6B7280")
    emoji = HEAT_EMOJI.get(heat, "⚫")

    # Header
    col_l, col_r = st.columns([3, 1])
    with col_l:
        st.markdown(f"""
            <div style='margin-bottom:4px; font-size:0.7rem; color:#6E7681;
                        text-transform:uppercase; letter-spacing:0.1em;'>
                {match['industry']} · {match['tier']}
            </div>
            <div style='font-size:1.6rem; font-weight:600; color:#E6EDF3;
                        letter-spacing:-0.02em;'>
                {emoji} {match['company']}
            </div>
            <div style='color:#8B949E; font-size:0.82rem; margin-top:4px;'>
                {match['hq_city']}, {match['hq_state']}
                &nbsp;·&nbsp; {match['employees']:,} employees
                {f"&nbsp;·&nbsp; Fortune {match['fortune_rank']}" if match['fortune_rank'] else ""}
                &nbsp;·&nbsp; {match['live_signals']} live signals collected
            </div>
        """, unsafe_allow_html=True)
    with col_r:
        st.markdown(f"""
            <div style='text-align:right; padding-top:8px;'>
                <div style='font-size:3rem; font-weight:800; color:{color};
                            font-family:"IBM Plex Mono",monospace; line-height:1;'>
                    {match['total_score']:.0f}
                </div>
                <div style='color:{color}; font-weight:600; font-size:0.9rem;'>
                    {heat}
                </div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    # Score breakdown
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Fit",            f"{match['fit_score']:.0f} / 40")
    c2.metric("Pain",           f"{match['pain_score']:.0f} / 40")
    c3.metric("Timing",         f"{match['timing_score']:.0f} / 20")
    c4.metric("Territory",      f"{match['territory_score']:.0f} / 20")
    c5.metric("Mtg Propensity", f"{match['meeting_prop']:.0f} / 100")

    st.markdown("---")

    tab1, tab2, tab3 = st.tabs(["📋 Score Evidence", "📡 Signals", "🔌 Source Log"])

    with tab1:
        if match["score_evidence"]:
            for ev in match["score_evidence"]:
                st.markdown(f"""
                    <div style='background:#161B22; border:1px solid #21262D;
                                border-left:3px solid #3FB950; border-radius:6px;
                                padding:8px 14px; margin-bottom:6px; font-size:0.82rem;
                                color:#C9D1D9;'>
                        {ev}
                    </div>
                """, unsafe_allow_html=True)
        else:
            st.info("No score evidence — 0 live signals collected. Run the proof from your laptop to get real signals.")

        st.markdown(f"""
            <div style='margin-top:12px; background:#161B22; border:1px solid #21262D;
                        border-radius:6px; padding:10px 14px; font-size:0.78rem; color:#8B949E;'>
                <b style='color:#C9D1D9;'>Surface decision:</b> {match['surface_reason']}
            </div>
        """, unsafe_allow_html=True)

    with tab2:
        signals = match.get("signals", [])
        if not signals:
            st.info("No signals collected. Scores shown are base tier + territory only.")
            st.markdown("""
                **Why?** This run was from the Claude sandbox, which blocks all external
                domains. Run `python run_proof.py` from your laptop to collect real signals.
            """)
        else:
            for sig in signals:
                cat   = sig.get("signal_type", "")
                stype = sig.get("source_type", "")
                conf  = sig.get("confidence_score", 0)
                kws   = sig.get("extracted_keywords", [])
                cat_colors = {
                    "hiring":         "#58A6FF",
                    "transformation": "#BC8CFF",
                    "timing":         "#3FB950",
                    "pain":           "#EF4444",
                    "news":           "#F97316",
                }
                c = cat_colors.get(cat, "#6E7681")

                with st.expander(f"[{cat.upper()}] {sig.get('raw_snippet','')[:70]}..."):
                    col_a, col_b = st.columns([3, 1])
                    with col_a:
                        st.markdown(f"**Source:** `{stype}`")
                        st.markdown(f"**Date:** {sig.get('date_found','')}")
                        st.markdown(f"**State:** {sig.get('state_detected','?')} · **City:** {sig.get('city_detected','?')}")
                        st.markdown(f"**Keywords:** `{'`, `'.join(kws[:6])}`")
                        st.markdown(f"**Buyer function:** {sig.get('likely_buyer_function','?')}")
                        st.markdown("**Snippet:**")
                        st.code(sig.get("raw_snippet",""), language=None)
                        if sig.get("source_url"):
                            st.markdown(f"[View source]({sig['source_url']})")
                    with col_b:
                        st.metric("Confidence",   f"{conf:.0%}")
                        st.metric("Enterprise rel", f"{sig.get('enterprise_relevance_score',0):.0%}")
                        st.markdown(f"""
                            <div style='font-size:0.7rem; color:#8B949E; margin-top:8px;'>
                                <div><b>Parser:</b> {sig.get('parser_used','?')}</div>
                                <div><b>Method:</b> {sig.get('extraction_method','?')}</div>
                                <div><b>Live:</b> {sig.get('live_collected',True)}</div>
                                <div><b>Access:</b> {sig.get('source_access_status','?')}</div>
                            </div>
                        """, unsafe_allow_html=True)

    with tab3:
        per_source = match.get("per_source", {})
        if not per_source:
            st.info("No source log available.")
        else:
            for src, meta in per_source.items():
                n      = meta.get("signals_returned", 0)
                status = meta.get("access_status","?")
                icon   = "✅" if n > 0 else ("⬜" if status in ("no_slug","not_implemented") else "🚫")
                proof  = meta.get("proof_url","")

                st.markdown(f"""
                    <div style='display:flex; justify-content:space-between; align-items:center;
                                background:#161B22; border:1px solid #21262D; border-radius:6px;
                                padding:8px 14px; margin-bottom:6px; font-size:0.8rem;'>
                        <div>
                            <span style='font-family:monospace; color:#E6EDF3;'>{icon} {src}</span>
                            <span style='color:#6E7681; margin-left:10px;'>{status}</span>
                            {f'<span style="color:#8B949E; margin-left:8px; font-size:0.72rem;">{meta.get("limitation","")}</span>' if meta.get("limitation") else ""}
                        </div>
                        <div style='font-family:monospace; color:{"#3FB950" if n > 0 else "#6E7681"};
                                    font-weight:600;'>
                            {n} signals
                        </div>
                    </div>
                """, unsafe_allow_html=True)

                if proof:
                    st.caption(f"Endpoint: `{proof}`")


# ════════════════════════════════════════════════════════════════════
# PAGE: SOURCE QUALITY
# ════════════════════════════════════════════════════════════════════

elif "Source" in page:
    st.markdown("### Source Quality Report")

    # Fetch log summary from metadata
    fetch_log = data.get("run_metadata", {}).get("fetch_log", {})
    by_status = fetch_log.get("by_status", {})
    by_source = fetch_log.get("by_source", {})

    if fetch_log:
        st.markdown("#### URL Access Summary")
        c1, c2, c3 = st.columns(3)
        c1.metric("URLs Tried",    fetch_log.get("total_urls_tried", 0))
        c2.metric("Success Rate",  f"{fetch_log.get('success_rate', 0):.1f}%")
        c3.metric("Successful",    by_status.get("success", 0))

        st.markdown("---")

        if by_status:
            st.markdown("#### Status Breakdown")
            status_rows = [{"Status": k, "Count": v} for k, v in sorted(by_status.items(), key=lambda x: -x[1])]
            status_df   = pd.DataFrame(status_rows)
            st.dataframe(status_df, use_container_width=True, hide_index=True)

        if by_source:
            st.markdown("#### Per-Source Access")
            src_rows = []
            for src, d in by_source.items():
                src_rows.append({
                    "Source":       src,
                    "Tried":        d.get("tried", 0),
                    "Success":      d.get("success", 0),
                    "Blocked":      d.get("blocked", 0),
                    "Failed":       d.get("failed", 0),
                    "Success Rate": f"{d['success']/d['tried']*100:.0f}%" if d.get("tried") else "0%",
                })
            src_df = pd.DataFrame(src_rows)
            st.dataframe(src_df, use_container_width=True, hide_index=True)

    else:
        st.info("No fetch log in this run.")

    st.markdown("---")
    st.markdown("#### Why 0% success rate in this output file")
    st.markdown("""
    This `proof_output.json` was generated inside the Claude sandbox, which restricts
    outbound HTTP to a whitelist of package registries only.

    **What you'll see when you run from your laptop:**
    - Greenhouse API → returns structured JSON job listings
    - Google News RSS → returns 5–15 news items per company
    - Company newsrooms → returns press release titles + excerpts
    - Careers pages (static) → returns job listings

    **Run this to get real data:**
    ```bash
    cd tessell-local
    python run_proof.py
    ```
    Then copy the new `proof_output.json` into this repo and redeploy.
    """)


# ════════════════════════════════════════════════════════════════════
# PAGE: RUN NEW SCAN
# ════════════════════════════════════════════════════════════════════

elif "Scan" in page:
    st.markdown("### Run a New Scan")
    st.info("""
    **This page is for reference only in the cloud version.**
    The signal collector runs as a Python script on your local machine or a server,
    not inside the Streamlit app itself. After running it, upload the new
    `proof_output.json` to this repo and redeploy.
    """)

    st.markdown("#### Steps to update this dashboard with fresh data")

    steps = [
        ("1. Run the collector locally",
         "```bash\ncd tessell-local\npython run_proof.py\n```"),
        ("2. Copy the output file",
         "```bash\ncp reports/proof_output.json ../tessell-streamlit/proof_output.json\n```"),
        ("3. Commit and push to GitHub",
         "```bash\ngit add proof_output.json\ngit commit -m 'update: fresh signal run'\ngit push\n```"),
        ("4. Streamlit Cloud auto-redeploys",
         "Streamlit Cloud watches your repo. The dashboard updates within ~60 seconds of the push."),
    ]

    for title, body in steps:
        with st.expander(title):
            st.markdown(body)

    st.markdown("---")
    st.markdown("#### Current data file info")
    meta = data.get("run_metadata", {})
    st.json({
        "timestamp":        meta.get("timestamp",""),
        "companies_run":    meta.get("companies_run",0),
        "data_mode":        meta.get("data_mode",""),
        "target_territory": meta.get("target_territory",[]),
        "elapsed_seconds":  meta.get("elapsed_seconds",0),
    })
