# UMA Optimistic Oracle — Dispute Funnel Scraper

Pulls the full history of UMA Optimistic Oracle assertions from public
subgraphs (no API key required) and reports on how many pass through
each stage of the escalation funnel, and how disputes resolve.

---

## What it measures

```
Total assertions
    └─ With proposal                     (some expire unproposed)
        └─ Disputed                      ← dispute rate
            └─ Challenger won            ← legitimacy of challenges
            └─ Proposer won              ← frivolous challenge rate
```

The ~1.5–2% dispute rate is UMA's published headline figure. This tool
breaks that down by:
- **Requester** — who is submitting oracle requests (Polymarket, Across, etc.)
- **Month** — is dispute behaviour changing over time?
- **Question type** — which identifiers (YES_OR_NO_QUERY, etc.) are contested most?
- **Stake size** — do higher-reward assertions attract more disputes?

---

## Quick start

```bash
pip install -r requirements.txt

# Full OOv2 history from 2022 onward (fetched once, cached locally)
python uma_scraper.py --since 2022-01-01

# Same command again — loads from cache, fetches only new records
python uma_scraper.py --since 2022-01-01

# Generate PNG plots into ./plots/
python uma_scraper.py --since 2022-01-01 --plots

# Save normalised data to CSV as well
python uma_scraper.py --since 2022-01-01 --csv disputes.csv

# Only Polymarket's oracle requests (Polygon MOOV2)
python uma_scraper.py --version moov2 --since 2023-01-01

# Cross-chain comparison
python uma_scraper.py --version all --since 2023-01-01

# Specific requester address
python uma_scraper.py --requester 0xd91e80cf2e7be2e162c6513ced06f1dd0da35296

# Force a full re-fetch, ignoring the cache
python uma_scraper.py --no-cache
```

---

## Available subgraphs

| `--version` | What it covers |
|---|---|
| `v2` | Mainnet OOv2 (default) — oldest, most history |
| `v3` | Mainnet OOv3 — newer assertion-based API |
| `v2-polygon` | Polygon OOv2 — where most Polymarket activity lives |
| `moov2` | Polygon Managed OOv2 — Polymarket post-UMIP-189 (Aug 2025) |
| `all` | Fetches all of the above and combines |

All endpoints are public Goldsky-hosted subgraphs from UMA's own
[subgraphs repo](https://github.com/UMAprotocol/subgraphs).

---

## Output sections

1. **Overall funnel** — top-level dispute and win rates
2. **Monthly trend** — how rates have evolved over time
3. **Top requesters** — which contracts drive the most volume and disputes
4. **Identifier breakdown** — dispute rates by question type
5. **High-stakes disputes** — biggest rewards in disputed assertions

---

## Caching

Raw subgraph data is cached locally in `./cache/` (gitignored) as JSON files keyed by `{version}_{since-timestamp}.json`.

**On subsequent runs the script:**
1. Loads the cached records instantly
2. Fetches only records newer than the cache high-water mark
3. Re-fetches the last **30 days** of records to pick up state changes on in-flight requests (e.g. `Proposed → Disputed → Settled`)
4. Merges and saves back to the cache

```bash
# Widen the back-check window (empirical p95 for disputed records is ~118 days)
python uma_scraper.py --recheck-days 120

# Custom cache location
python uma_scraper.py --cache-dir ./data

# Bypass cache entirely
python uma_scraper.py --no-cache
```

---

## Plots

Add `--plots` to generate five PNG charts saved to `./plots/` (gitignored):

| File | What it shows |
|---|---|
| `funnel.png` | Horizontal bar: requests → proposed → disputed → resolved |
| `dispute_outcomes.png` | Pie: challenger wins / proposer wins / unknown |
| `monthly_trend.png` | Bars (volume) + line (dispute rate %) by month |
| `challenger_win_rate.png` | Monthly challenger win rate with 50% reference line |
| `top_requesters.png` | Stacked bar (disputed/undisputed) + dispute rate per requester |

```bash
python uma_scraper.py --plots --plot-dir ./figs
```

---

## Extending it

### Add Polymarket question enrichment

```python
from polymarket_enricher import enrich_with_polymarket
df = enrich_with_polymarket(df)
# Now df has 'question' and 'volume_usd' columns
```

### Kleros comparison

The same funnel logic works on the Kleros subgraph (The Graph):
```
https://api.thegraph.com/subgraphs/name/kleros/kleros-v1-mainnet
```
Entity: `disputes` with fields `ruled`, `ruling`, `period`, `rounds`.
The "challenger win rate" equivalent is `ruling != 0` (jurors did not 
abstain) and comparing against the initial ruling direction.

### Custom analysis in Python

```python
import pandas as pd
from uma_scraper import fetch_all_v2, normalise_v2, ENDPOINTS

raw = fetch_all_v2(ENDPOINTS["v2"], min_timestamp=1672531200)  # 2023-01-01
df  = normalise_v2(raw)

# All disputed assertions where challenger won
won = df[df["challenger_won"] == True]

# Dispute rate over time
df["month"] = df["request_ts"].dt.to_period("M")
monthly = df.groupby("month")["disputed"].agg(["sum", "count"])
monthly["rate"] = monthly["sum"] / monthly["count"]
print(monthly)
```

---

## Key fields in the normalised DataFrame

| Column | Description |
|---|---|
| `id` | Unique request ID (hex) |
| `version` | `v2` or `v3` |
| `identifier` | Question type (e.g. `YES_OR_NO_QUERY`) |
| `requester` | Contract that requested oracle data |
| `proposer` | Address that proposed the answer |
| `disputer` | Address that challenged (null if undisputed) |
| `request_ts` | When the request was made |
| `dispute_ts` | When disputed (null if undisputed) |
| `settlement_ts` | When finally resolved |
| `reward` | Bond/reward size (in token units, 18 decimals normalised) |
| `disputed` | Boolean — was this challenged? |
| `resolved` | Boolean — has it settled? |
| `challenger_won` | `True` = challenger won, `False` = proposer won, `None` = not disputed |

---

## Notes on data quality

- **OOv2 `disputeSucceeded`** is a reliable field — it directly encodes
  which side won after DVM vote.
- **OOv3 `settlementResolution`** is `"true"` (assertion upheld) or
  `"false"` (assertion rejected). Challenger wins when it's `"false"`.
- Some records show `disputed=True` but `challenger_won=None` — these
  are disputes still pending DVM resolution at time of fetch.
- The subgraph may lag a few blocks behind chain tip.

---

## Interesting questions to explore

- Does dispute rate increase for markets with larger total volume at stake?
- Are certain proposer addresses associated with higher dispute rates
  (potential bad actors or bots)?
- Did the MOOV2 whitelisting (Aug 2025) reduce the dispute rate on
  Polymarket's oracle?
- How does the challenger win rate compare across question types?
  (Sports scores vs. political outcomes vs. price thresholds)
- Is there a "dispute fatigue" effect — do challengers win less often
  in high-frequency dispute periods?
