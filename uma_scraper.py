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

Dependencies:
    pip install requests pandas tabulate
"""

import argparse
import sys
import json
import time
from datetime import datetime, timezone
from collections import defaultdict

import requests
import pandas as pd
from tabulate import tabulate


# ── Subgraph endpoints ─────────────────────────────────────────────────────────

ENDPOINTS = {
    "v2": "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/mainnet-optimistic-oracle-v2/latest/gn",
    "v3": "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/mainnet-optimistic-oracle-v3/latest/gn",
    # Polygon OOv2 — same schema, different chain
    "v2-polygon": "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/polygon-optimistic-oracle-v2/1.1.0/gn",
    # Managed OOv2 (Polymarket's dedicated oracle post-UMIP-189)
    "moov2": "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/polygon-managed-optimistic-oracle-v2/1.0.5/gn",
}

# ── GraphQL queries ────────────────────────────────────────────────────────────

# OOv2 schema: priceRequests with proposal/dispute/settlement fields
OOV2_QUERY = """
query FetchRequests($first: Int!, $skip: Int!, $minTime: BigInt!) {
  priceRequests(
    first: $first
    skip: $skip
    orderBy: requestTimestamp
    orderDirection: asc
    where: { requestTimestamp_gte: $minTime }
  ) {
    id
    identifier
    currency
    reward
    finalFee
    requestTimestamp
    requester

    # Proposal fields
    proposer
    proposedPrice
    proposalTimestamp
    proposalExpirationTimestamp

    # Dispute fields (null if never disputed)
    disputer
    disputeTimestamp

    # Settlement
    settlementPrice
    settlementTimestamp
    settlementRecipient   # who got the bond payout
    isResolved

    # Whether the dispute was resolved in favour of the disputer
    # true  → challenger won (original proposal was wrong)
    # false → proposer won  (challenge was frivolous)
    disputeSucceeded
  }
}
"""

# OOv3 has a different entity name: "assertions" rather than "priceRequests"
OOV3_QUERY = """
query FetchAssertions($first: Int!, $skip: Int!, $minTime: BigInt!) {
  assertions(
    first: $first
    skip: $skip
    orderBy: assertionTimestamp
    orderDirection: asc
    where: { assertionTimestamp_gte: $minTime }
  ) {
    id
    identifier
    currency
    bond
    assertionTimestamp
    asserter
    callbackRecipient
    escalationManager

    # Dispute fields
    disputer
    disputeTimestamp

    # Settlement
    settlementTimestamp
    settlementPayout
    settlementRecipient
    expirationTime

    # Resolution
    settled
    settlementResolution   # "true" | "false" | null
  }
}
"""

# ── Fetching ───────────────────────────────────────────────────────────────────

PAGE_SIZE = 1000  # max The Graph returns per query


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
    """Paginate through all OOv2 priceRequests."""
    records = []
    skip = 0
    print(f"Fetching OOv2 requests from {endpoint[:60]}…")
    while True:
        data = run_query(
            endpoint,
            OOV2_QUERY,
            {"first": PAGE_SIZE, "skip": skip, "minTime": str(min_timestamp)},
        )
        batch = data.get("priceRequests", [])
        records.extend(batch)
        print(f"  fetched {len(records)} records so far…", end="\r")
        if len(batch) < PAGE_SIZE:
            break
        skip += PAGE_SIZE
        time.sleep(0.25)  # be polite
    print(f"\n  ✓ Total OOv2 records: {len(records)}")
    return records


def fetch_all_v3(endpoint: str, min_timestamp: int) -> list[dict]:
    """Paginate through all OOv3 assertions."""
    records = []
    skip = 0
    print(f"Fetching OOv3 assertions from {endpoint[:60]}…")
    while True:
        data = run_query(
            endpoint,
            OOV3_QUERY,
            {"first": PAGE_SIZE, "skip": skip, "minTime": str(min_timestamp)},
        )
        batch = data.get("assertions", [])
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
    """Convert raw OOv2 records to a common schema."""
    rows = []
    for r in records:
        disputed = r.get("disputer") is not None
        dispute_succeeded = r.get("disputeSucceeded")  # bool or None
        rows.append({
            "id":                  r["id"],
            "version":             "v2",
            "identifier":          _decode_identifier(r.get("identifier", "")),
            "requester":           (r.get("requester") or "").lower(),
            "proposer":            (r.get("proposer") or "").lower(),
            "disputer":            (r.get("disputer") or "").lower(),
            "request_ts":          _ts(r.get("requestTimestamp")),
            "proposal_ts":         _ts(r.get("proposalTimestamp")),
            "dispute_ts":          _ts(r.get("disputeTimestamp")),
            "settlement_ts":       _ts(r.get("settlementTimestamp")),
            "proposed_price":      _wei(r.get("proposedPrice")),
            "settlement_price":    _wei(r.get("settlementPrice")),
            "reward":              _wei(r.get("reward")),
            "final_fee":           _wei(r.get("finalFee")),
            "has_proposal":        r.get("proposer") is not None,
            "disputed":            disputed,
            "resolved":            r.get("isResolved", False),
            # For disputes: True = challenger won, False = proposer won, None = not disputed
            "challenger_won":      dispute_succeeded if disputed else None,
        })
    return pd.DataFrame(rows)


def normalise_v3(records: list[dict]) -> pd.DataFrame:
    """Convert raw OOv3 assertion records to the common schema."""
    rows = []
    for r in records:
        disputed = r.get("disputer") is not None
        # OOv3 settlementResolution: "true" means assertion was upheld,
        # so challenger_won = True when resolution is "false"
        res = r.get("settlementResolution")
        if res is None or not disputed:
            challenger_won = None
        else:
            challenger_won = (res.lower() == "false")  # assertion rejected → challenger won

        rows.append({
            "id":                  r["id"],
            "version":             "v3",
            "identifier":          _decode_identifier(r.get("identifier", "")),
            "requester":           (r.get("callbackRecipient") or "").lower(),
            "proposer":            (r.get("asserter") or "").lower(),
            "disputer":            (r.get("disputer") or "").lower(),
            "request_ts":          _ts(r.get("assertionTimestamp")),
            "proposal_ts":         _ts(r.get("assertionTimestamp")),
            "dispute_ts":          _ts(r.get("disputeTimestamp")),
            "settlement_ts":       _ts(r.get("settlementTimestamp")),
            "proposed_price":      None,
            "settlement_price":    _wei(r.get("settlementPayout")),
            "reward":              _wei(r.get("bond")),
            "final_fee":           None,
            "has_proposal":        True,  # v3 asserter IS the proposer
            "disputed":            disputed,
            "resolved":            r.get("settled", False),
            "challenger_won":      challenger_won,
        })
    return pd.DataFrame(rows)


def _ts(val) -> datetime | None:
    if val is None:
        return None
    try:
        return datetime.fromtimestamp(int(val), tz=timezone.utc)
    except Exception:
        return None


def _wei(val) -> float | None:
    """Convert a raw integer string (18 decimals) to float. Returns None if absent."""
    if val is None:
        return None
    try:
        return int(val) / 1e18
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
    return p.parse_args()


def main():
    args = parse_args()

    since_dt = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    min_ts   = int(since_dt.timestamp())

    print(f"\nUMA Optimistic Oracle — Dispute Funnel Analyser")
    print(f"Since: {args.since}  |  Version: {args.version}")
    print("─" * 60)

    # ── Fetch ──────────────────────────────────────────────────────────────────
    frames = []

    versions_to_fetch = (
        ["v2", "v3", "v2-polygon", "moov2"] if args.version == "all"
        else [args.version]
    )

    for ver in versions_to_fetch:
        ep = ENDPOINTS[ver]
        try:
            if ver == "v3":
                raw = fetch_all_v3(ep, min_ts)
                df  = normalise_v3(raw)
            else:
                raw = fetch_all_v2(ep, min_ts)
                df  = normalise_v2(raw)
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
    hot = (
        df[df["disputed"] & df["reward"].notna()]
        .sort_values("reward", ascending=False)
        .head(10)
    )
    if not hot.empty:
        hot_display = hot[["id", "identifier", "requester", "reward",
                            "challenger_won", "dispute_ts"]].copy()
        hot_display["id"]       = hot_display["id"].str[:16] + "…"
        hot_display["requester"] = hot_display["requester"].str[:12] + "…"
        hot_display["reward"]   = hot_display["reward"].apply(
            lambda x: f"{x:.4f}" if x else "—"
        )
        hot_display["dispute_ts"] = hot_display["dispute_ts"].apply(
            lambda t: t.strftime("%Y-%m-%d") if t else "—"
        )
        hot_display["challenger_won"] = hot_display["challenger_won"].apply(
            lambda x: "✓ Challenger" if x is True else ("✗ Proposer" if x is False else "?")
        )
        print_table(
            hot_display,
            cols=["id", "identifier", "reward", "challenger_won", "dispute_ts"],
            headers=["ID", "Question type", "Reward (ETH)", "Winner", "Disputed"],
        )

    print("\n✓ Done.\n")


if __name__ == "__main__":
    main()
