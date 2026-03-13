"""
inspect_schema.py — dump the full field list for a UMA subgraph entity.
Run this once to confirm field names before using uma_scraper.py.

Usage:
    python inspect_schema.py
    python inspect_schema.py --version v3
    python inspect_schema.py --version moov2
"""

import argparse
import requests
import json

ENDPOINTS = {
    "v2":        "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/mainnet-optimistic-oracle-v2/latest/gn",
    "v3":        "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/mainnet-optimistic-oracle-v3/latest/gn",
    "v2-polygon":"https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/polygon-optimistic-oracle-v2/1.1.0/gn",
    "moov2":     "https://api.goldsky.com/api/public/project_clus2fndawbcc01w31192938i/subgraphs/polygon-managed-optimistic-oracle-v2/1.0.5/gn",
}

def get_top_level_entities(url):
    q = '{ __schema { queryType { fields { name } } } }'
    r = requests.post(url, json={"query": q}, timeout=20)
    r.raise_for_status()
    fields = r.json()["data"]["__schema"]["queryType"]["fields"]
    return [f["name"] for f in fields]

def get_entity_fields(url, entity_name):
    q = f"""
    {{
      __type(name: "{entity_name}") {{
        name
        fields {{
          name
          type {{
            name
            kind
            ofType {{ name kind }}
          }}
        }}
      }}
    }}
    """
    r = requests.post(url, json={"query": q}, timeout=20)
    r.raise_for_status()
    type_info = r.json()["data"]["__type"]
    if not type_info:
        return []
    return type_info["fields"]

def type_str(t):
    if t["kind"] == "NON_NULL":
        inner = t.get("ofType") or {}
        return f"{inner.get('name', '?')}!"
    if t["kind"] == "LIST":
        inner = t.get("ofType") or {}
        return f"[{inner.get('name', '?')}]"
    return t.get("name", "?")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--version", default="v2", choices=ENDPOINTS.keys())
    args = p.parse_args()

    url = ENDPOINTS[args.version]
    print(f"\nEndpoint: {url}\n")

    print("=== Top-level query entities ===")
    entities = get_top_level_entities(url)
    for e in entities:
        print(f"  {e}")

    # Find the main data entity (skip _meta and singular forms)
    plural = [e for e in entities if not e.startswith("_") and e.endswith("s")]
    if not plural:
        print("\nNo plural entities found.")
        return

    # Pick the most interesting one
    target = plural[0]
    # Capitalise to get the type name (GraphQL convention: entity "fooRequests" → type "FooRequest")
    singular = target[0].upper() + target[1:]
    if singular.endswith("s"):
        singular = singular[:-1]

    print(f"\n=== Fields on '{singular}' type ===")
    fields = get_entity_fields(url, singular)
    if not fields:
        # Try alternate capitalisation
        singular2 = "".join(w.capitalize() for w in target.rstrip("s").split("_"))
        print(f"  (trying type name '{singular2}'…)")
        fields = get_entity_fields(url, singular2)

    if not fields:
        print(f"  Could not resolve type. Try running:")
        print(f'  curl -s -X POST \'{url}\' -H \'Content-Type: application/json\' \\')
        print("""  -d '{"query":"{ __type(name: \\"OptimisticPriceRequest\\") { fields { name type { name kind } } } }"}' """)
    else:
        for f in fields:
            print(f"  {f['name']:40s}  {type_str(f['type'])}")

    # Also fetch one real record so we can see actual values
    print(f"\n=== Sample record from '{target}' ===")
    field_names = " ".join(f["name"] for f in fields[:20]) if fields else "id"
    sample_q = f"{{ {target}(first: 1) {{ {field_names} }} }}"
    try:
        r = requests.post(url, json={"query": sample_q}, timeout=20)
        data = r.json()
        if "errors" in data:
            print(f"  Query error: {data['errors']}")
        else:
            records = data["data"].get(target, [])
            if records:
                print(json.dumps(records[0], indent=2))
            else:
                print("  (no records returned)")
    except Exception as e:
        print(f"  Error fetching sample: {e}")


if __name__ == "__main__":
    main()
