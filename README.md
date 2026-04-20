# Tessell Signal Engine

Enterprise account intelligence for Rackspace sellers.
Finds Fortune 1000 companies showing Oracle/database pain, modernization signals,
and hiring activity — then scores and ranks them by territory fit.

## Live Dashboard

[![Streamlit App](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://tessell-signal-engine.streamlit.app)

## What It Does

- Collects live signals from Greenhouse, Lever, Google News RSS, company newsrooms, and careers pages
- Scores each company on Fit (0–40) + Pain (0–40) + Timing (0–20) + Territory (0–20)
- Ranks by Meeting Propensity — who to call this week
- Exports to CSV for Salesforce

## Run Locally

```bash
# Install
pip install -r requirements.txt

# Collect fresh signals (25 Fortune 1000 companies)
python run_proof.py

# Launch dashboard
streamlit run streamlit_app.py
```

## Update the Dashboard

After running `python run_proof.py`, the new `proof_output.json` is written to your
local `reports/` folder. Copy it to the repo root and push:

```bash
cp reports/proof_output.json proof_output.json
git add proof_output.json
git commit -m "update: fresh signal run $(date +%Y-%m-%d)"
git push
```

Streamlit Cloud redeploys automatically within ~60 seconds.

## Scoring

| Dimension | Max | What it measures |
|-----------|-----|-----------------|
| Fit | 40 | Enterprise size, DB technologies, regulated industry |
| Pain | 40 | DBA hiring, pain keywords, modernization language |
| Timing | 20 | Leadership change, recent hiring, M&A activity |
| Territory | 20 | Geographic presence in seller's states |

## Territory

Default: TX, OK, KS. Override: `python run_proof.py --states TX,OK,KS,AR`

## Files

```
streamlit_app.py       Dashboard UI
run_proof.py           Signal collector (run locally)
proof_output.json      Latest results (committed to repo)
collectors/            HTTP fetchers + signal model
scoring/               Enterprise gate + scoring engine
requirements.txt       Python dependencies
```
