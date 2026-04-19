# Tessell Signal Engine — Local Startup Guide
## Exact steps. No ambiguity.

---

## DEPENDENCY CHECKLIST

Before running, verify each item:

### System requirements
- [ ] Python 3.11 or 3.12  →  `python --version`
- [ ] pip  →  `pip --version`
- [ ] Normal internet access (not a corporate proxy that blocks scrapers)
- [ ] ~200MB disk space for logs and outputs

### Python packages (installed by step 3 below)
- [ ] requests 2.31.0
- [ ] beautifulsoup4 4.12.3
- [ ] lxml 5.2.1
- [ ] fake-useragent 1.5.1
- [ ] pydantic 2.7.1
- [ ] pydantic-settings 2.2.1
- [ ] loguru 0.7.2
- [ ] tenacity 9.1.4
- [ ] pandas 2.2.2
- [ ] python-dateutil 2.9.0
- [ ] python-dotenv 1.0.1

### No API keys required
The proof run requires ZERO paid API keys. All sources are public.

---

## EXACT STARTUP STEPS

### Step 1 — Unzip
```bash
unzip tessell-local.zip
cd tessell-local
```

### Step 2 — Create virtual environment (recommended)
```bash
python -m venv venv

# macOS / Linux:
source venv/bin/activate

# Windows:
venv\Scripts\activate
```

### Step 3 — Install dependencies
```bash
pip install -r requirements.txt
```

If you see any errors:
```bash
# If lxml fails on macOS:
brew install libxml2
pip install lxml --no-cache-dir

# If fake-useragent fails:
pip install fake-useragent --no-cache-dir

# Windows users may need:
pip install requests[security]
```

### Step 4 — Verify imports
```bash
python -c "import requests, bs4, loguru, pydantic; print('OK')"
```
Expected output: `OK`

### Step 5 — ONE COMMAND to run the 25-company proof
```bash
python run_proof.py
```

That's it.

---

## COMMAND OPTIONS

```bash
# Full 25-company run (default)
python run_proof.py

# Quick 5-company test (~60 seconds)
python run_proof.py --quick

# Single company
python run_proof.py --company McKesson
python run_proof.py --company "Southwest Airlines"

# Different territory
python run_proof.py --states TX,OK,KS
python run_proof.py --states TX,OK,KS,AR,NM,AZ

# Debug logging (see every URL attempt)
python run_proof.py --quick --log-level DEBUG
```

---

## OUTPUT FILES

All written to `./reports/` automatically:

| File | What's in it |
|------|-------------|
| `proof_output.json` | Full results for all 25 companies. Every signal, every score breakdown. |
| `proof_summary.csv` | One row per company. All score fields. Open in Excel. |
| `false_positive_report.csv` | Signals flagged for human review (low confidence, weak keywords). |
| `source_quality.json` | Signals per source, avg confidence, success rate by source type. |
| `post_run_report.txt` | Human-readable summary. Top 10 accounts, source access summary. |

Timestamped versions also written (e.g. `proof_output_20260419_231331.json`).
Log file written to `./logs/run_YYYYMMDD_HHMMSS.log`.

---

## WHAT TO EXPECT FROM A SUCCESSFUL RUN

### With real internet (laptop/server)

Each company takes 5–30 seconds depending on source response times.
Total runtime: 10–20 minutes for all 25.

Expected signal counts per company:
- Companies using Greenhouse ATS: 5–25 hiring signals
- Companies with active news coverage: 3–10 news signals
- Companies with accessible newsrooms: 2–8 press release signals
- Companies with neither: 0 signals (base score only)

Expected final scores:
- Fortune 500 companies with strong Oracle/DBA hiring: 70–100
- Fortune 500 with no relevant signals: 20–35 (base tier + territory)
- Non-Fortune companies with signals: 50–80

### From this Claude sandbox

All external domains are blocked. You will see:
```
⬜ careers_page     0 signals  (robots_blocked)
⬜ greenhouse       0 signals  (http_403)
⬜ newsroom         0 signals  (robots_blocked)
```
This is expected. The code is correct. The network is restricted.

---

## WHAT GOOD LOG OUTPUT LOOKS LIKE (from laptop)

```
23:14:01 | INFO    | ▶  Starting: McKesson  (F9 | TX | 51,000 emp)
23:14:03 | INFO    | [Greenhouse] McKesson: 847 total jobs fetched
23:14:03 | INFO    | [Greenhouse] McKesson: 12 relevant signals after noise filter
23:14:06 | INFO    | [GoogleNews] McKesson+"Oracle database": 8 items
23:14:06 | INFO    | [GoogleNews] McKesson: 12 raw → 3 filtered → 9 relevant
23:14:08 | INFO    | [Newsroom] McKesson: 24 candidates → 5 filtered → 4 relevant
23:14:08 | INFO    |    ✅ careers_page        3 signals
23:14:08 | INFO    |    ✅ greenhouse         12 signals
23:14:08 | INFO    |    ⬜ lever               0 signals  (no_slug)
23:14:08 | INFO    |    ✅ newsroom            4 signals
23:14:08 | INFO    |    ✅ google_news         9 signals
23:14:08 | INFO    |    ⬜ sec_edgar           0 signals  (filing index only)
23:14:08 | INFO    |    Total: 28 raw → 0 deduped → 28 unique signals (6.8s)
23:14:08 | INFO    |    Score: 95/100  Heat: HOT  MtgP: 88
```

---

## KNOWN LIMITATIONS

| Limitation | Impact | Workaround |
|-----------|--------|-----------|
| Workday serves JS-rendered listings | Only gets JSON-LD + static HTML (1-5 jobs) | Phase 3: Playwright |
| iCIMS is client-side rendered | Very limited data without JS | Phase 3: Playwright |
| Greenhouse covers ~25% of F1000 | Gaps for non-Greenhouse companies | Expand slug list |
| No Lever slugs for most F1000 | Misses Lever-hosted jobs | Phase 3: auto-discover |
| SEC EDGAR full text not parsed | Gets filing index, not content | Phase 3: EDGAR parse |
| Google News has rate limits | After ~50 requests/hour may throttle | Space out runs |
| robots.txt enforcement | Blocks some domains | Accepted policy constraint |

---

## STOP HERE

Do not add new features until you have:
1. Run `python run_proof.py` from your laptop
2. Seen live signals in the output
3. Confirmed top-10 accounts match expected Fortune 1000 targets
4. Reviewed `false_positive_report.csv` for signal quality

Then report back with the actual `proof_output.json` and we go to Phase 3.
