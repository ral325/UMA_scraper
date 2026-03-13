"""
UMA Optimistic Oracle — Dispute Funnel Scraper
================================================
Pulls the full history of price requests from UMA's OOv2 subgraph
and reports on how many make it through each stage of the escalation
funnel, and how those disputes resolve.

Endpoints used (all public, no API key required):
  - Goldsky (OOv2 mainnet):
      https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i
      /subgraphs/mainnet-optimistic-oracle-v2/latest/gn
  - Goldsky (OOv3 mainnet):
      https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i
      /subgraphs/mainnet-optimistic-oracle-v3/latest/gn  (schema differs, see below)

Usage:
    python uma_scraper.py                      # full history, all requesters
    python uma_scraper.py --requester 0xabc    # filter to one requester (e.g. Polymarket)
    python uma_scraper.py --since 2024-01-01   # only requests after this date
    python uma_scraper.py --csv results.csv    # also save raw data to CSV
    python uma_scraper.py --version v3         # use OOv3 subgraph instead
    python uma_scraper.py --plots              # generate and save plots
    python uma_scraper.py --no-cache              # force re-fetch even if cached data exists
    python uma_scraper.py --cache-dir ./data      # custom cache directory (default: ./cache)
    python uma_scraper.py --plot-dir ./figs       # custom plot output directory (default: ./plots)
    python uma_scraper.py --recheck-days 120      # widen back-check window (default: 30 days)

Dependencies:
    pip install requests pandas tabulate matplotlib
"""

import argparse
import sys
import json
import time
import os
import hashlib
from datetime import datetime, timezone
from collections import defaultdict
from pathlib import Path

import requests
import pandas as pd
from tabulate import tabulate


# ── Caching ────────────────────────────────────────────────────────────────────

DEFAULT_CACHE_DIR = Path("cache")
DEFAULT_PLOT_DIR  = Path("plots")


def _cache_path(cache_dir: Path, version: str) -> Path:
    """One cache file per version — no timestamp in the key."""
    return cache_dir / f"{version}.json"


def _get_max_ts(records: list[dict], version: str) -> int:
    """Return the highest requestTimestamp / assertionTimestamp in a record list."""
    field = "assertionTimestamp" if version == "v3" else "requestTimestamp"
    vals = [int(r[field]) for r in records if r.get(field)]
    return max(vals) if vals else 0


def _get_min_ts(records: list[dict], version: str) -> int:
    """Return the lowest requestTimestamp / assertionTimestamp in a record list."""
    field = "assertionTimestamp" if version == "v3" else "requestTimestamp"
    vals = [int(r[field]) for r in records if r.get(field)]
    return min(vals) if vals else 0


def load_cache(cache_dir: Path, version: str, min_timestamp: int) -> tuple[list[dict] | None, int]:
    """
    Return (records, max_ts) from the cache envelope, or (None, 0) on miss.

    Cache envelope format:
        {
          "query_min_ts":  <int>,   # earliest --since ever used to populate this cache
          "max_ts":        <int>,   # highest request/assertion timestamp seen
          "fetched_at":    <str>,   # ISO-8601 wall-clock of last write
          "records":       [...]    # raw GraphQL dicts (all history, unfiltered)
        }

    The cache is keyed only by version (one file per version). The --since filter
    is applied in-memory after loading, so different --since values share the same
    cache and never duplicate data.

    If the requested min_timestamp predates the cache's query_min_ts the cache
    doesn't cover the full range — treat as a miss so the caller re-fetches.
    """
    path = _cache_path(cache_dir, version)
    if not path.exists():
        # Also look for old-format files ({version}_{timestamp}.json) to hint the user
        old = sorted(cache_dir.glob(f"{version}_*.json"))
        if old:
            print(f"  ⚠ Found old-format cache(s) for {version}: {[p.name for p in old]}")
            print(f"    These are no longer used. Delete them or run without --offline to rebuild.")
        return None, 0
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        print(f"  ⚠ Old-format cache at {path} — will rebuild")
        return None, 0
    query_min = data.get("query_min_ts", data.get("min_timestamp", 0))
    if query_min > min_timestamp:
        cached_since = datetime.fromtimestamp(query_min, tz=timezone.utc).strftime("%Y-%m-%d")
        want_since   = datetime.fromtimestamp(min_timestamp, tz=timezone.utc).strftime("%Y-%m-%d")
        print(f"  ⚠ Cache covers from {cached_since} but --since {want_since} requests earlier data.")
        print(f"    Run with --no-cache to re-fetch from {want_since}.")
        return None, 0
    records = data.get("records", [])
    max_ts  = data.get("max_ts", 0)
    fetched = data.get("fetched_at", "unknown")
    print(f"  ✓ Cache hit: {len(records):,} records (last fetched {fetched[:10]})")
    return records, max_ts


def save_cache(cache_dir: Path, version: str, min_timestamp: int, records: list[dict]):
    """Persist records to the cache directory using the envelope format."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = _cache_path(cache_dir, version)
    # Preserve the earliest query_min_ts if the file already exists
    existing_min = min_timestamp
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                old = json.load(f)
            existing_min = min(min_timestamp, old.get("query_min_ts", min_timestamp))
        except Exception:
            pass
    envelope = {
        "query_min_ts": existing_min,
        "max_ts":       _get_max_ts(records, version),
        "fetched_at":   datetime.now(tz=timezone.utc).isoformat(),
        "records":      records,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(envelope, f)
    print(f"  ✓ Cached {len(records):,} records → {path}")


# ── Subgraph endpoints ─────────────────────────────────────────────────────────

ENDPOINTS = {
    "v2": "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/mainnet-optimistic-oracle-v2/latest/gn",
    "v3": "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/mainnet-optimistic-oracle-v3/latest/gn",
    # Polygon OOv2 — same schema, different chain
    "v2-polygon": "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/polygon-optimistic-oracle-v2/1.1.0/gn",
    # Managed OOv2 (Polymarket's dedicated oracle post-UMIP-189)
    "moov2": "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/polygon-managed-optimistic-oracle-v2/1.0.5/gn",
}

# ── Fetching ───────────────────────────────────────────────────────────────────

PAGE_SIZE = 1000  # max The Graph returns per query

# ── Schema discovery ───────────────────────────────────────────────────────────

# Candidate entity names — we try these in order until one works
OOV2_CANDIDATE_ENTITIES = [
    "optimisticPriceRequests",  # confirmed correct (mainnet-optimistic-oracle-v2)
    "assertions",               # OOv3-style fallback
    "requests",
    "proposals",
    "priceRequests",
]

OOV3_CANDIDATE_ENTITIES = [
    "assertions",
    "oracleAssertions",
]


def discover_entity(endpoint: str, candidates: list[str]) -> str:
    """
    Introspect the subgraph schema and return the first candidate entity
    that actually exists. Falls back to candidates[0] if introspection fails.
    """
    introspect = '{ __schema { queryType { fields { name } } } }'
    try:
        resp = requests.post(endpoint, json={"query": introspect}, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        if "errors" in data or "data" not in data:
            return candidates[0]
        available = {f["name"] for f in data["data"]["__schema"]["queryType"]["fields"]}
        print(f"  Schema fields: {sorted(available)}")
        for name in candidates:
            if name in available:
                print(f"  → Using entity: '{name}'")
                return name
        # If none match, show what's available and use first candidate
        print(f"  ⚠ None of {candidates} found in schema. Available: {sorted(available)}")
        print(f"  → Falling back to first available plural field...")
        # Try to pick any plural-looking field that's not a meta field
        for f in sorted(available):
            if not f.startswith("_") and f not in ("query",):
                print(f"  → Using: '{f}'")
                return f
        return candidates[0]
    except Exception as exc:
        print(f"  ⚠ Schema introspection failed ({exc}), using '{candidates[0]}'")
        return candidates[0]


def run_query(endpoint: str, query: str, variables: dict, retries: int = 3) -> dict:
    """POST a GraphQL query with simple retry logic."""
    for attempt in range(retries):
        try:
            resp = requests.post(
                endpoint,
                json={"query": query, "variables": variables},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            if "errors" in data:
                raise ValueError(f"GraphQL errors: {data['errors']}")
            return data["data"]
        except Exception as exc:
            if attempt == retries - 1:
                raise
            wait = 2 ** attempt
            print(f"  ⚠  Attempt {attempt+1} failed ({exc}), retrying in {wait}s…")
            time.sleep(wait)


def fetch_all_v2(endpoint: str, min_timestamp: int) -> list[dict]:
    """Paginate through all OOv2 requests, auto-discovering the entity name."""
    print(f"Fetching OOv2 from {endpoint[:60]}…")
    entity = discover_entity(endpoint, OOV2_CANDIDATE_ENTITIES)

    # Field names confirmed via schema introspection against live Goldsky endpoint.
    # No isResolved or disputeSucceeded fields exist — use 'state' enum instead.
    # Winner is inferred from settlementRecipient vs proposer/disputer addresses.
    query = f"""
    query FetchRequests($first: Int!, $skip: Int!, $minTime: BigInt!) {{
      {entity}(
        first: $first
        skip: $skip
        orderBy: requestTimestamp
        orderDirection: asc
        where: {{ requestTimestamp_gte: $minTime }}
      ) {{
        id
        identifier
        ancillaryData
        requestTimestamp
        requester
        currency
        reward
        finalFee
        bond
        proposer
        proposedPrice
        proposalTimestamp
        proposalExpirationTimestamp
        disputer
        disputeTimestamp
        settlementPrice
        settlementPayout
        settlementTimestamp
        settlementRecipient
        state
      }}
    }}
    """

    records = []
    skip = 0
    while True:
        try:
            data = run_query(
                endpoint,
                query,
                {"first": PAGE_SIZE, "skip": skip, "minTime": str(min_timestamp)},
            )
        except Exception as exc:
            # If the query still fails, print schema so user can report back
            print(f"\n  ✗ Query failed: {exc}")
            print("  Run this to see the real schema fields:")
            print(f"    curl -s -X POST '{endpoint}' -H 'Content-Type: application/json' \\")
            print("""    -d '{"query":"{ __schema { queryType { fields { name } } } }"}' | python3 -m json.tool""")
            raise

        batch = data.get(entity, [])
        if not batch:
            # Try alternate field in response (schema might differ)
            for key, val in data.items():
                if isinstance(val, list) and len(val) > 0:
                    print(f"  ℹ Response used key '{key}' instead of '{entity}' — adapting")
                    entity = key
                    batch = val
                    break

        records.extend(batch)
        print(f"  fetched {len(records)} records so far…", end="\r")
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(0.25)  # be polite
    print(f"\n  ✓ Total OOv2 records: {len(records)}")
    return records


def fetch_all_v3(endpoint: str, min_timestamp: int) -> list[dict]:
    """Paginate through all OOv3 assertions, auto-discovering the entity name."""
    print(f"Fetching OOv3 from {endpoint[:60]}…")
    entity = discover_entity(endpoint, OOV3_CANDIDATE_ENTITIES)

    query = f"""
    query FetchAssertions($first: Int!, $skip: Int!, $minTime: BigInt!) {{
      {entity}(
        first: $first
        skip: $skip
        orderBy: assertionTimestamp
        orderDirection: asc
        where: {{ assertionTimestamp_gte: $minTime }}
      ) {{
        id
        identifier
        currency
        bond
        assertionTimestamp
        asserter
        callbackRecipient
        escalationManager
        disputer
        disputeTimestamp
        settlementTimestamp
        settlementPayout
        settlementRecipient
        expirationTime
        settlementResolution
      }}
    }}
    """

    records = []
    skip = 0
    while True:
        try:
            data = run_query(
                endpoint,
                query,
                {"first": PAGE_SIZE, "skip": skip, "minTime": str(min_timestamp)},
            )
        except Exception as exc:
            print(f"\n  ✗ Query failed: {exc}")
            print("  Run this to see the real schema fields:")
            print(f"    curl -s -X POST '{endpoint}' -H 'Content-Type: application/json' \\")
            print("""    -d '{"query":"{ __schema { queryType { fields { name } } } }"}' | python3 -m json.tool""")
            raise

        batch = data.get(entity, [])
        records.extend(batch)
        print(f"  fetched {len(records)} records so far…", end="\r")
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(0.25)
    print(f"\n  ✓ Total OOv3 records: {len(records)}")
    return records


# ── Normalisation ──────────────────────────────────────────────────────────────

def normalise_v2(records: list[dict]) -> pd.DataFrame:
    """Convert raw OOv2 optimisticPriceRequests to a common schema.

    Field notes (confirmed from live schema introspection):
    - state: enum — "Requested", "Proposed", "Disputed", "Settled", "Expired", "Invalid"
    - No isResolved field: use state == "Settled" or state == "Expired"
    - No disputeSucceeded field: infer from settlementRecipient
        - If settlementRecipient == disputer  → challenger won
        - If settlementRecipient == proposer  → proposer won
    - reward/finalFee/bond/settlementPayout are raw BigInt strings (18 decimals)
    """
    rows = []
    for r in records:
        state     = r.get("state") or ""
        disputed  = r.get("disputer") is not None
        proposer  = (r.get("proposer") or "").lower()
        disputer  = (r.get("disputer") or "").lower()
        recipient = (r.get("settlementRecipient") or "").lower()
        ccy       = (r.get("currency") or "").lower()

        # Infer who won from settlement recipient address
        if not disputed or not recipient:
            challenger_won = None
        elif recipient == disputer:
            challenger_won = True   # challenger got the payout → they won
        elif recipient == proposer:
            challenger_won = False  # proposer got it back → they won
        else:
            challenger_won = None   # unknown (e.g. refund to requester)

        rows.append({
            "id":               r["id"],
            "version":          "v2",
            "identifier":       r.get("identifier", ""),
            "ancillary_data":   r.get("ancillaryData", ""),
            "requester":        (r.get("requester") or "").lower(),
            "proposer":         proposer,
            "disputer":         disputer,
            "request_ts":       _ts(r.get("requestTimestamp")),
            "proposal_ts":      _ts(r.get("proposalTimestamp")),
            "dispute_ts":       _ts(r.get("disputeTimestamp")),
            "settlement_ts":    _ts(r.get("settlementTimestamp")),
            "proposed_price":   _wei(r.get("proposedPrice")),
            "settlement_price": _wei(r.get("settlementPrice")),
            "settlement_payout":_wei(r.get("settlementPayout"), ccy),
            "reward":           _wei(r.get("reward"), ccy),
            "bond":             _wei(r.get("bond"), ccy),
            "final_fee":        _wei(r.get("finalFee"), ccy),
            "state":            state,
            "has_proposal":     proposer != "",
            "disputed":         disputed,
            "resolved":         state in ("Settled", "Expired"),
            "challenger_won":   challenger_won,
        })
    df = pd.DataFrame(rows)
    # Explicitly cast monetary columns to float64 so pd.concat never sees
    # ambiguous all-NA object columns (avoids FutureWarning in pandas 2.x).
    for col in ("proposed_price", "settlement_price", "settlement_payout",
                "reward", "bond", "final_fee"):
        if col in df.columns:
            df[col] = df[col].astype("float64")
    return df


def normalise_v3(records: list[dict]) -> pd.DataFrame:
    """Convert raw OOv3 assertion records to the common schema."""
    rows = []
    for r in records:
        disputed = r.get("disputer") is not None
        ccy      = (r.get("currency") or "").lower()
        # OOv3 settlementResolution: "true" means assertion was upheld,
        # so challenger_won = True when resolution is "false"
        res = r.get("settlementResolution")
        if res is None or not disputed:
            challenger_won = None
        elif isinstance(res, bool):
            # Subgraph may return a native bool instead of string "true"/"false"
            challenger_won = not res  # True = assertion upheld = proposer won
        else:
            challenger_won = (str(res).lower() == "false")  # assertion rejected → challenger won

        rows.append({
            "id":                  r["id"],
            "version":             "v3",
            "identifier":          _decode_identifier(r.get("identifier", "")),
            "ancillary_data":      "",
            "requester":           (r.get("callbackRecipient") or "").lower(),
            "proposer":            (r.get("asserter") or "").lower(),
            "disputer":            (r.get("disputer") or "").lower(),
            "request_ts":          _ts(r.get("assertionTimestamp")),
            "proposal_ts":         _ts(r.get("assertionTimestamp")),
            "dispute_ts":          _ts(r.get("disputeTimestamp")),
            "settlement_ts":       _ts(r.get("settlementTimestamp")),
            "proposed_price":      None,
            "settlement_price":    _wei(r.get("settlementPayout"), ccy),
            "settlement_payout":   _wei(r.get("settlementPayout"), ccy),
            "reward":              _wei(r.get("bond"), ccy),
            "bond":                _wei(r.get("bond"), ccy),
            "final_fee":           None,
            "state":               "",
            "has_proposal":        True,  # v3 asserter IS the proposer
            "disputed":            disputed,
            "resolved":            r.get("settlementTimestamp") is not None,
            "challenger_won":      challenger_won,
        })
    df = pd.DataFrame(rows)
    for col in ("proposed_price", "settlement_price", "settlement_payout",
                "reward", "bond", "final_fee"):
        if col in df.columns:
            df[col] = df[col].astype("float64")
    return df


def _ts(val) -> datetime | None:
    if val is None:
        return None
    try:
        return datetime.fromtimestamp(int(val), tz=timezone.utc)
    except Exception:
        return None


# Known 6-decimal token addresses (lower-case). USDC and USDT are the most
# common collateral on Polymarket / Polygon and use 6 decimals, not 18.
_DECIMALS_6 = frozenset({
    "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",  # USDC  (Ethereum mainnet)
    "0x2791bca1f2de4661ed88a30c99a7a9449aa84174",  # USDC.e (Polygon PoS)
    "0x3c499c542cef5e3811e1192ce70d8cc03d5c3359",  # native USDC (Polygon)
    "0xdac17f958d2ee523a2206206994597c13d831ec7",  # USDT  (Ethereum mainnet)
    "0xc2132d05d31c914a87c6611c10748aeb04b58e8f",  # USDT  (Polygon PoS)
})


def _wei(val, currency: str = "") -> float | None:
    """Convert a raw integer string to a human-readable float.

    Uses 6 decimals for known stablecoins (USDC, USDT), 18 for everything else.
    Pass the token address via `currency` for correct conversion.
    """
    if val is None:
        return None
    try:
        decimals = 6 if currency.lower() in _DECIMALS_6 else 18
        return int(val) / (10 ** decimals)
    except Exception:
        return None


def _decode_identifier(hex_or_str: str) -> str:
    """UMA identifiers are bytes32; try to decode as UTF-8 text."""
    if hex_or_str.startswith("0x"):
        try:
            raw = bytes.fromhex(hex_or_str[2:]).rstrip(b"\x00")
            return raw.decode("utf-8", errors="replace")
        except Exception:
            pass
    return hex_or_str


# ── Analysis ───────────────────────────────────────────────────────────────────

def compute_funnel(df: pd.DataFrame, label: str = "All") -> dict:
    """Compute funnel statistics from the normalised DataFrame."""
    total        = len(df)
    proposed     = df["has_proposal"].sum()
    disputed     = df["disputed"].sum()
    resolved     = df["resolved"].sum()

    dispute_df   = df[df["disputed"]]
    challenger_wins = (dispute_df["challenger_won"] == True).sum()   # noqa: E712
    proposer_wins   = (dispute_df["challenger_won"] == False).sum()  # noqa: E712
    unknown_outcome = dispute_df["challenger_won"].isna().sum()

    dispute_rate     = disputed / total       if total       > 0 else 0
    challenger_rate  = challenger_wins / disputed if disputed > 0 else 0
    proposer_rate    = proposer_wins   / disputed if disputed > 0 else 0

    return {
        "label":            label,
        "total_requests":   int(total),
        "proposed":         int(proposed),
        "disputed":         int(disputed),
        "resolved":         int(resolved),
        "dispute_rate_pct": round(dispute_rate * 100, 3),
        "challenger_wins":  int(challenger_wins),
        "proposer_wins":    int(proposer_wins),
        "unknown_outcome":  int(unknown_outcome),
        "challenger_win_pct": round(challenger_rate * 100, 1),
        "proposer_win_pct":   round(proposer_rate  * 100, 1),
    }


def top_requesters(df: pd.DataFrame, n: int = 10) -> pd.DataFrame:
    """Group by requester address and compute per-requester funnel stats."""
    rows = []
    for addr, grp in df.groupby("requester"):
        stats = compute_funnel(grp, label=addr[:10] + "…")
        stats["address"] = addr
        rows.append(stats)
    result = pd.DataFrame(rows).sort_values("total_requests", ascending=False)
    return result.head(n)


def monthly_trend(df: pd.DataFrame) -> pd.DataFrame:
    """Dispute rate and challenger win rate by calendar month."""
    df2 = df.copy()
    df2["month"] = df2["request_ts"].apply(
        lambda t: t.strftime("%Y-%m") if t else "unknown"
    )
    rows = []
    for month, grp in sorted(df2.groupby("month")):
        s = compute_funnel(grp, label=month)
        rows.append(s)
    return pd.DataFrame(rows)


def identifier_breakdown(df: pd.DataFrame) -> pd.DataFrame:
    """Which question types (identifiers) drive the most disputes?"""
    rows = []
    for ident, grp in df.groupby("identifier"):
        s = compute_funnel(grp, label=ident[:40])
        rows.append(s)
    return pd.DataFrame(rows).sort_values("disputed", ascending=False)


# ── Printing ───────────────────────────────────────────────────────────────────

def print_section(title: str):
    print(f"\n{'═'*60}")
    print(f"  {title}")
    print(f"{'═'*60}")


def print_funnel(stats: dict):
    print(f"""
  Total requests:       {stats['total_requests']:>8,}
  ├─ With proposal:     {stats['proposed']:>8,}
  ├─ Disputed:          {stats['disputed']:>8,}   ({stats['dispute_rate_pct']:.3f}% dispute rate)
  └─ Resolved:          {stats['resolved']:>8,}

  Of {stats['disputed']:,} disputes:
    Challenger won:     {stats['challenger_wins']:>8,}   ({stats['challenger_win_pct']:.1f}%)
    Proposer won:       {stats['proposer_wins']:>8,}   ({stats['proposer_win_pct']:.1f}%)
    Outcome unknown:    {stats['unknown_outcome']:>8,}
""")


def print_table(df: pd.DataFrame, cols: list[str], headers: list[str]):
    subset = df[cols].copy()
    subset.columns = headers
    print(tabulate(subset, headers="keys", tablefmt="rounded_outline", showindex=False))


# ── Plotting ───────────────────────────────────────────────────────────────────

def _setup_plot_style():
    import matplotlib.pyplot as plt
    plt.style.use("seaborn-v0_8-darkgrid")
    plt.rcParams.update({
        "figure.dpi":      120,
        "axes.titlesize":  13,
        "axes.labelsize":  11,
        "xtick.labelsize": 9,
        "ytick.labelsize": 9,
        "legend.fontsize": 9,
    })
    return plt


def plot_monthly_trend(df: pd.DataFrame, plot_dir: Path):
    """Line chart: monthly request volume and dispute rate over time."""
    import matplotlib.pyplot as plt
    plt = _setup_plot_style()

    trend = monthly_trend(df)
    trend = trend[trend["label"] != "unknown"]
    if trend.empty:
        return

    months = trend["label"].tolist()
    x      = range(len(months))

    fig, ax1 = plt.subplots(figsize=(12, 5))

    ax1.bar(x, trend["total_requests"], color="#4C72B0", alpha=0.6, label="Total Requests")
    ax1.bar(x, trend["disputed"],       color="#DD8452", alpha=0.8, label="Disputed")
    ax1.set_ylabel("Request Count")
    ax1.set_xticks(list(x))
    ax1.set_xticklabels(months, rotation=45, ha="right")

    ax2 = ax1.twinx()
    ax2.plot(x, trend["dispute_rate_pct"], color="#C44E52", linewidth=2,
             marker="o", markersize=4, label="Dispute Rate %")
    ax2.set_ylabel("Dispute Rate (%)", color="#C44E52")
    ax2.tick_params(axis="y", labelcolor="#C44E52")

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

    ax1.set_title("Monthly Request Volume & Dispute Rate")
    fig.tight_layout()

    out = plot_dir / "monthly_trend.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"  ✓ Plot saved: {out}")


def plot_funnel(stats: dict, plot_dir: Path):
    """Horizontal bar funnel: requests → proposed → disputed → resolved."""
    import matplotlib.pyplot as plt
    plt = _setup_plot_style()

    stages  = ["Total Requests", "With Proposal", "Disputed", "Resolved"]
    values  = [
        stats["total_requests"],
        stats["proposed"],
        stats["disputed"],
        stats["resolved"],
    ]
    colors = ["#4C72B0", "#55A868", "#DD8452", "#8172B2"]

    fig, ax = plt.subplots(figsize=(8, 4))
    bars = ax.barh(stages[::-1], values[::-1], color=colors[::-1], edgecolor="white", height=0.5)

    for bar, val in zip(bars, values[::-1]):
        ax.text(bar.get_width() + max(values) * 0.01, bar.get_y() + bar.get_height() / 2,
                f"{val:,}", va="center", fontsize=10)

    ax.set_xlabel("Count")
    ax.set_title("Dispute Escalation Funnel")
    ax.set_xlim(0, max(values) * 1.15)
    fig.tight_layout()

    out = plot_dir / "funnel.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"  ✓ Plot saved: {out}")


def plot_top_requesters(df: pd.DataFrame, plot_dir: Path, n: int = 10):
    """Stacked bar: top requesters by request volume, coloured by disputed/undisputed."""
    import matplotlib.pyplot as plt
    plt = _setup_plot_style()

    top = top_requesters(df, n=n)
    if top.empty:
        return

    labels    = [a[:10] + "…" for a in top["address"]]
    total     = top["total_requests"].tolist()
    disputed  = top["disputed"].tolist()
    undisputed = [t - d for t, d in zip(total, disputed)]

    x = range(len(labels))

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.bar(x, undisputed, label="Undisputed",  color="#4C72B0", alpha=0.85)
    ax.bar(x, disputed,   label="Disputed",    color="#DD8452", alpha=0.85, bottom=undisputed)

    ax2 = ax.twinx()
    ax2.plot(x, top["dispute_rate_pct"].tolist(), color="#C44E52", linewidth=2,
             marker="o", markersize=5, label="Dispute Rate %")
    ax2.set_ylabel("Dispute Rate (%)", color="#C44E52")
    ax2.tick_params(axis="y", labelcolor="#C44E52")

    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("Request Count")
    ax.set_title(f"Top {n} Requesters by Volume")

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1 + lines2, labels1 + labels2, loc="upper right")

    fig.tight_layout()
    out = plot_dir / "top_requesters.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"  ✓ Plot saved: {out}")


def plot_dispute_outcomes(stats: dict, plot_dir: Path):
    """Pie chart: of disputed requests, who wins?"""
    import matplotlib.pyplot as plt
    plt = _setup_plot_style()

    labels = []
    sizes  = []
    colors = []
    if stats["challenger_wins"] > 0:
        labels.append(f"Challenger Wins\n({stats['challenger_win_pct']}%)")
        sizes.append(stats["challenger_wins"])
        colors.append("#DD8452")
    if stats["proposer_wins"] > 0:
        labels.append(f"Proposer Wins\n({stats['proposer_win_pct']}%)")
        sizes.append(stats["proposer_wins"])
        colors.append("#4C72B0")
    if stats["unknown_outcome"] > 0:
        labels.append("Unknown")
        sizes.append(stats["unknown_outcome"])
        colors.append("#8C8C8C")

    if not sizes:
        return

    fig, ax = plt.subplots(figsize=(6, 6))
    ax.pie(sizes, labels=labels, colors=colors, autopct="%1.1f%%",
           startangle=90, wedgeprops=dict(edgecolor="white", linewidth=1.5))
    ax.set_title(f"Dispute Outcomes\n({stats['disputed']:,} total disputes)")
    fig.tight_layout()

    out = plot_dir / "dispute_outcomes.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"  ✓ Plot saved: {out}")


def plot_challenger_win_rate_trend(df: pd.DataFrame, plot_dir: Path):
    """Line chart: monthly challenger win rate among resolved disputes."""
    import matplotlib.pyplot as plt
    plt = _setup_plot_style()

    trend = monthly_trend(df)
    trend = trend[trend["label"] != "unknown"]
    # Only months with at least one known outcome
    trend = trend[(trend["challenger_wins"] + trend["proposer_wins"]) > 0]
    if trend.empty:
        return

    months = trend["label"].tolist()
    x      = range(len(months))

    fig, ax = plt.subplots(figsize=(12, 4))
    ax.plot(x, trend["challenger_win_pct"], color="#DD8452", linewidth=2,
            marker="o", markersize=5, label="Challenger Win %")
    ax.axhline(50, color="grey", linestyle="--", linewidth=1, alpha=0.7, label="50% line")
    ax.fill_between(x, trend["challenger_win_pct"], 50,
                    where=[v > 50 for v in trend["challenger_win_pct"]],
                    alpha=0.15, color="#DD8452")
    ax.fill_between(x, trend["challenger_win_pct"], 50,
                    where=[v <= 50 for v in trend["challenger_win_pct"]],
                    alpha=0.15, color="#4C72B0")

    ax.set_xticks(list(x))
    ax.set_xticklabels(months, rotation=45, ha="right")
    ax.set_ylabel("Challenger Win Rate (%)")
    ax.set_ylim(0, 100)
    ax.set_title("Monthly Challenger Win Rate")
    ax.legend()
    fig.tight_layout()

    out = plot_dir / "challenger_win_rate.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"  ✓ Plot saved: {out}")


def generate_plots(df: pd.DataFrame, stats: dict, plot_dir: Path, top_n: int = 10):
    """Generate all plots and save to plot_dir."""
    try:
        import matplotlib
        matplotlib.use("Agg")  # non-interactive backend — safe for all environments
    except ImportError:
        print("  ⚠ matplotlib not installed — skipping plots. Run: pip install matplotlib")
        return

    plot_dir.mkdir(parents=True, exist_ok=True)
    print(f"\nGenerating plots → {plot_dir}/")

    plot_funnel(stats, plot_dir)
    plot_dispute_outcomes(stats, plot_dir)
    plot_monthly_trend(df, plot_dir)
    plot_challenger_win_rate_trend(df, plot_dir)
    plot_top_requesters(df, plot_dir, n=top_n)


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="UMA OO dispute funnel analyser")
    p.add_argument("--version", choices=["v2", "v3", "v2-polygon", "moov2", "all"],
                   default="v2", help="Which subgraph to query (default: v2)")
    p.add_argument("--since", default="2021-01-01",
                   help="Only include requests on or after this date (YYYY-MM-DD)")
    p.add_argument("--requester", default=None,
                   help="Filter to a specific requester address (e.g. Polymarket contract)")
    p.add_argument("--csv", default=None,
                   help="Save raw normalised data to this CSV path")
    p.add_argument("--top-n", type=int, default=10,
                   help="How many top requesters to show (default: 10)")
    # Caching
    p.add_argument("--no-cache", action="store_true",
                   help="Ignore cached data and re-fetch from the subgraph")
    p.add_argument("--offline", action="store_true",
                   help="Use cached data only — skip all network calls (error if no cache)")
    p.add_argument("--cache-dir", default=str(DEFAULT_CACHE_DIR),
                   help=f"Directory for cached raw data (default: {DEFAULT_CACHE_DIR})")
    p.add_argument("--recheck-days", type=int, default=30,
                   help="On incremental updates, re-fetch this many days before the cache "
                        "high-water mark to catch state changes on in-flight records "
                        "(default: 30; empirical p95 for disputed records is ~118 days)")
    # Plotting
    p.add_argument("--plots", action="store_true",
                   help="Generate and save PNG plots")
    p.add_argument("--plot-dir", default=str(DEFAULT_PLOT_DIR),
                   help=f"Directory to write plot PNGs (default: {DEFAULT_PLOT_DIR})")
    return p.parse_args()


def main():
    args = parse_args()

    since_dt  = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    min_ts    = int(since_dt.timestamp())
    cache_dir = Path(args.cache_dir)
    plot_dir  = Path(args.plot_dir)

    print(f"\nUMA Optimistic Oracle — Dispute Funnel Analyser")
    print(f"Since: {args.since}  |  Version: {args.version}")
    print("─" * 60)

    # ── Fetch (with caching) ───────────────────────────────────────────────────
    frames = []

    versions_to_fetch = (
        ["v2", "v3", "v2-polygon", "moov2"] if args.version == "all"
        else [args.version]
    )

    for ver in versions_to_fetch:
        ep = ENDPOINTS[ver]
        try:
            cached_records, cached_max_ts = None, 0
            if not args.no_cache:
                cached_records, cached_max_ts = load_cache(cache_dir, ver, min_ts)

            ts_field = "assertionTimestamp" if ver == "v3" else "requestTimestamp"

            if args.offline:
                # Offline mode: use cache as-is, no network calls
                if cached_records is None:
                    print(f"  ✗ No cache found for {ver} — run without --offline to fetch first.")
                    continue
                raw = cached_records
            elif cached_records is None:
                # No cache — full fetch from --since date
                if ver == "v3":
                    raw = fetch_all_v3(ep, min_ts)
                else:
                    raw = fetch_all_v2(ep, min_ts)
                save_cache(cache_dir, ver, min_ts, raw)
            else:
                # Incremental fetch: new records + a back-check window to catch state changes.
                # We back up --recheck-days from the high-water mark so that in-flight records
                # (e.g. "Proposed" → "Disputed" → "Settled") get their latest state re-fetched.
                recheck_secs = args.recheck_days * 86_400
                fetch_from = max(min_ts, cached_max_ts - recheck_secs) if cached_max_ts > 0 else min_ts
                fetch_from_dt = datetime.fromtimestamp(fetch_from, tz=timezone.utc).strftime("%Y-%m-%d")
                print(f"  → Incremental fetch from {fetch_from_dt} "
                      f"(recheck window: {args.recheck_days} days)")

                if ver == "v3":
                    new_records = fetch_all_v3(ep, fetch_from)
                else:
                    new_records = fetch_all_v2(ep, fetch_from)

                if new_records:
                    # Merge: build id→record dict so newer data overwrites stale entries
                    merged = {r["id"]: r for r in cached_records}
                    before = len(merged)
                    for r in new_records:
                        merged[r["id"]] = r
                    raw = sorted(merged.values(), key=lambda r: int(r.get(ts_field, 0)))
                    added = len(merged) - before
                    updated = len(new_records) - added
                    print(f"  → {added:,} new records, {updated:,} updated in recheck window")
                    save_cache(cache_dir, ver, min_ts, raw)
                else:
                    print(f"  → Cache is up to date, no new records")
                    raw = cached_records

            # Apply --since filter in memory. The cache stores all history; --since
            # is just a view filter so different values share the same cache file.
            raw = [r for r in raw if int(r.get(ts_field) or 0) >= min_ts]

            if ver == "v3":
                df = normalise_v3(raw)
            else:
                df = normalise_v2(raw)
            df["source"] = ver
            frames.append(df)
        except Exception as exc:
            print(f"  ✗ Failed to fetch {ver}: {exc}")

    if not frames:
        print("No data fetched. Check your network / endpoint.")
        sys.exit(1)

    df = pd.concat(frames, ignore_index=True)

    # ── Filter ─────────────────────────────────────────────────────────────────
    if args.requester:
        addr = args.requester.lower()
        df = df[df["requester"] == addr]
        print(f"\nFiltered to requester {addr}: {len(df):,} records")

    # ── Save raw CSV ───────────────────────────────────────────────────────────
    if args.csv:
        df.to_csv(args.csv, index=False)
        print(f"\n✓ Raw data saved to {args.csv}")

    # ── Overall funnel ─────────────────────────────────────────────────────────
    print_section("OVERALL FUNNEL")
    print_funnel(compute_funnel(df, "All"))

    # ── Monthly trend ──────────────────────────────────────────────────────────
    print_section("MONTHLY TREND (dispute rate & challenger win rate)")
    trend = monthly_trend(df)
    if not trend.empty:
        print_table(
            trend,
            cols=["label", "total_requests", "disputed", "dispute_rate_pct",
                  "challenger_wins", "challenger_win_pct"],
            headers=["Month", "Requests", "Disputes", "Dispute%",
                     "Chall.Wins", "Chall.Win%"],
        )

    # ── Top requesters ─────────────────────────────────────────────────────────
    print_section(f"TOP {args.top_n} REQUESTERS BY VOLUME")
    top = top_requesters(df, n=args.top_n)
    if not top.empty:
        print_table(
            top,
            cols=["address", "total_requests", "disputed", "dispute_rate_pct",
                  "challenger_wins", "challenger_win_pct"],
            headers=["Requester", "Requests", "Disputes", "Dispute%",
                     "Chall.Wins", "Chall.Win%"],
        )

    # ── Identifier breakdown ───────────────────────────────────────────────────
    print_section("DISPUTE RATE BY QUESTION TYPE (identifier)")
    ident = identifier_breakdown(df)
    if not ident.empty:
        print_table(
            ident.head(20),
            cols=["label", "total_requests", "disputed", "dispute_rate_pct",
                  "challenger_wins", "challenger_win_pct"],
            headers=["Identifier", "Requests", "Disputes", "Dispute%",
                     "Chall.Wins", "Chall.Win%"],
        )

    # ── High-stakes disputes ───────────────────────────────────────────────────
    print_section("HIGH-STAKES DISPUTES (top 10 by reward)")
    # Sort by settlement_payout (actual value at stake), fall back to bond or reward.
    # Use > 0 (not just notna) because tokens with wrong decimal conversion show as ~0.
    disputed_df = df[df["disputed"]]
    payout_col = "bond"  # default
    for col in ("settlement_payout", "bond", "reward"):
        if col in df.columns and (disputed_df[col] > 0).any():
            payout_col = col
            break
    hot = (
        df[df["disputed"] & df[payout_col].notna()]
        .sort_values(payout_col, ascending=False)
        .head(10)
    )
    if not hot.empty:
        hot_display = hot[["id", "identifier", "requester", payout_col,
                            "challenger_won", "dispute_ts"]].copy()
        hot_display["id"]        = hot_display["id"].str[:20] + "…"
        hot_display["requester"] = hot_display["requester"].str[:12] + "…"
        hot_display[payout_col]  = hot_display[payout_col].apply(
            lambda x: f"{x:,.2f}" if x else "—"
        )
        hot_display["dispute_ts"] = hot_display["dispute_ts"].apply(
            lambda t: t.strftime("%Y-%m-%d") if t else "—"
        )
        hot_display["challenger_won"] = hot_display["challenger_won"].apply(
            lambda x: "✓ Challenger" if x is True else ("✗ Proposer" if x is False else "?")
        )
        print_table(
            hot_display,
            cols=["id", "identifier", payout_col, "challenger_won", "dispute_ts"],
            headers=["ID", "Question type", "Payout (tokens)", "Winner", "Disputed"],
        )

    # ── Plots ──────────────────────────────────────────────────────────────────
    if args.plots:
        generate_plots(df, compute_funnel(df, "All"), plot_dir, top_n=args.top_n)

    print("\n✓ Done.\n")


if __name__ == "__main__":
    main()
