"""
Polymarket Enricher
====================
Fetches Polymarket market metadata from their public REST API and
joins it to the UMA dispute data so you get human-readable question
titles alongside the on-chain dispute records.

Polymarket's API endpoint: https://gamma-api.polymarket.com
No authentication required.

Usage (standalone):
    python polymarket_enricher.py

Usage (as module):
    from polymarket_enricher import enrich_with_polymarket
    df = enrich_with_polymarket(df)  # adds 'question' and 'volume_usd' columns
"""

import time
import requests
import pandas as pd


POLYMARKET_API = "https://gamma-api.polymarket.com"

# Known Polymarket UMA OO requester addresses (may change with contract upgrades)
POLYMARKET_REQUESTERS = {
    # Polygon mainnet — primary Polymarket oracle requester
    "0xd91e80cf2e7be2e162c6513ced06f1dd0da35296",
    # Add others if you discover them via on-chain inspection
}


def fetch_polymarket_markets(limit: int = 500) -> list[dict]:
    """
    Fetch recent Polymarket markets from the Gamma API.
    Returns a list of market dicts with 'conditionId', 'question', 'volume', etc.
    """
    markets = []
    offset  = 0
    print("Fetching Polymarket market metadata…")
    while True:
        try:
            resp = requests.get(
                f"{POLYMARKET_API}/markets",
                params={"limit": limit, "offset": offset, "closed": "true"},
                timeout=20,
            )
            resp.raise_for_status()
            batch = resp.json()
        except Exception as exc:
            print(f"  ⚠ Polymarket API error: {exc}")
            break

        if not batch:
            break

        markets.extend(batch)
        print(f"  fetched {len(markets)} markets…", end="\r")

        if len(batch) < limit:
            break
        offset += limit
        time.sleep(0.2)

    print(f"\n  ✓ {len(markets)} Polymarket markets fetched")
    return markets


def build_polymarket_lookup(markets: list[dict]) -> dict:
    """
    Build a dict keyed by conditionId (hex) → {question, volume_usd, end_date}.
    conditionId maps to the ancillaryData / identifier used in UMA requests.
    """
    lookup = {}
    for m in markets:
        cid = (m.get("conditionId") or "").lower()
        if cid:
            lookup[cid] = {
                "question":   m.get("question", ""),
                "volume_usd": float(m.get("volume", 0) or 0),
                "end_date":   m.get("endDate", ""),
                "resolved":   m.get("closed", False),
                "outcome":    m.get("outcomePrices", []),
            }
    return lookup


def enrich_with_polymarket(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add 'question' and 'volume_usd' columns to a UMA dispute DataFrame
    by joining against Polymarket's market API on condition ID.

    The join key is extracted from the UMA request 'id' field, which
    typically encodes: requester + timestamp + identifier + ancillaryData.
    For Polymarket markets the ancillaryData contains the conditionId.

    This is a best-effort enrichment — many rows won't match.
    """
    markets  = fetch_polymarket_markets()
    lookup   = build_polymarket_lookup(markets)

    questions  = []
    volumes    = []
    end_dates  = []

    for _, row in df.iterrows():
        # Try to match on the lower-case hex ID substring
        matched = None
        row_id  = (row.get("id") or "").lower()
        for cid, meta in lookup.items():
            if cid and cid in row_id:
                matched = meta
                break

        questions.append(matched["question"]   if matched else "")
        volumes.append(matched["volume_usd"]   if matched else None)
        end_dates.append(matched["end_date"]   if matched else "")

    df = df.copy()
    df["question"]    = questions
    df["volume_usd"]  = volumes
    df["end_date"]    = end_dates
    return df


if __name__ == "__main__":
    # Quick standalone test — just print a sample of markets
    markets = fetch_polymarket_markets(limit=10)
    for m in markets[:5]:
        print(f"  {m.get('question', '')[:80]}")
        print(f"    volume=${float(m.get('volume', 0) or 0):,.0f}  closed={m.get('closed')}")
