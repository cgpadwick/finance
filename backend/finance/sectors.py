"""Fetch company sector/industry from SEC EDGAR (free, no API key, no daily cap).

Two steps:
  1. SEC's company_tickers.json maps ticker -> CIK + company name.
  2. Per company, submissions/CIK##########.json carries SIC code + description.
We map the SIC code to a broad (GICS-like) sector for grouping/filtering, and
keep the precise SIC description as `industry`. Cached in the DuckDB companies
table; the run is resumable (skips symbols already populated).

Run:  python -m finance.sectors            # all stored symbols
      python -m finance.sectors --symbols AAPL MU NVDA
"""

from __future__ import annotations

import argparse
import time

import requests
from tqdm import tqdm

from finance.store import BarStore

# SEC requires a descriptive User-Agent with contact info.
HEADERS = {"User-Agent": "momentum-screener (research; cgpadwick@gmail.com)"}
TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik:010d}.json"


def sic_to_sector(sic: str | int | None) -> str:
    """Map a SIC code to a broad GICS-like sector. Specific cases first."""
    try:
        s = int(sic)
    except (TypeError, ValueError):
        return "Unknown"

    # --- specific overrides (must precede broad ranges) ---
    if 2833 <= s <= 2836:  # pharma / biologics
        return "Health Care"
    if s == 3826 or 3840 <= s <= 3851:  # lab/medical/surgical instruments & devices
        return "Health Care"
    if s == 8731 or 8000 <= s <= 8099:  # biological research; health services
        return "Health Care"
    if s in (3559, 3827):  # semiconductor fab equipment & optical/inspection instruments
        return "Technology"
    if 3570 <= s <= 3579:  # computers & office equipment
        return "Technology"
    if 3661 <= s <= 3679:  # comms equipment, semiconductors, electronic components
        return "Technology"
    if 7370 <= s <= 7379:  # software / IT services
        return "Technology"
    if 3711 <= s <= 3716:  # motor vehicles
        return "Consumer Discretionary"
    if 3720 <= s <= 3729:  # aircraft / aerospace
        return "Industrials"
    if 5400 <= s <= 5499 or s == 5912:  # food & drug stores
        return "Consumer Staples"
    if 1300 <= s <= 1399 or 2900 <= s <= 2999:  # oil & gas, petroleum refining
        return "Energy"
    if 4800 <= s <= 4899:  # telecom
        return "Communication Services"
    if 7800 <= s <= 7999 or 2700 <= s <= 2799 or 7310 <= s <= 7319:  # media/publishing/ads
        return "Communication Services"
    if 4900 <= s <= 4999:  # utilities
        return "Utilities"
    if 6500 <= s <= 6599 or s == 6798:  # real estate / REITs
        return "Real Estate"
    if 6000 <= s <= 6799:  # banks, insurance, finance
        return "Financials"

    # --- broad ranges ---
    if 100 <= s <= 999:
        return "Consumer Staples"
    if 1000 <= s <= 1499:
        return "Materials"
    if 1500 <= s <= 1799:
        return "Industrials"
    if 2000 <= s <= 2199:
        return "Consumer Staples"
    if 2200 <= s <= 2399 or 2500 <= s <= 2599 or 3100 <= s <= 3199 or 3900 <= s <= 3999:
        return "Consumer Discretionary"
    if 2400 <= s <= 2499 or 2600 <= s <= 2699 or 2800 <= s <= 2899 or 3000 <= s <= 3399:
        return "Materials"
    if 3400 <= s <= 3599:
        return "Industrials"
    if 3600 <= s <= 3699:
        return "Technology"
    if 3700 <= s <= 3799:
        return "Industrials"
    if 3800 <= s <= 3899:
        return "Technology"
    if 4000 <= s <= 4799 or 5000 <= s <= 5199:  # transport, wholesale
        return "Industrials"
    if 5200 <= s <= 5999:  # retail
        return "Consumer Discretionary"
    if 7000 <= s <= 7299 or 8200 <= s <= 8299:  # hotels, personal services, education
        return "Consumer Discretionary"
    if 7300 <= s <= 7399 or 8700 <= s <= 8799:  # business / professional services
        return "Industrials"
    return "Other"


def load_ticker_cik_map() -> dict[str, dict]:
    resp = requests.get(TICKERS_URL, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    out: dict[str, dict] = {}
    for row in resp.json().values():
        out[row["ticker"].upper()] = {"cik": row["cik_str"], "name": row["title"]}
    return out


def fetch_one(symbol: str, cik: int) -> tuple[str, str, str]:
    """Return (name, sic, sic_description) for a CIK from SEC submissions."""
    resp = requests.get(SUBMISSIONS_URL.format(cik=cik), headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    return (
        data.get("name", ""),
        str(data.get("sic", "")),
        data.get("sicDescription", ""),
    )


def run(symbols: list[str], delay: float = 0.12, resume: bool = True) -> dict[str, int]:
    store = BarStore()
    try:
        done = store.companies_with_sector() if resume else set()
        todo = [s for s in symbols if s not in done]
        print(f"{len(todo)} symbols to fetch ({len(done)} already cached).")
        cik_map = load_ticker_cik_map()
        counts: dict[str, int] = {}
        missing = []
        for sym in tqdm(todo, desc="SEC sectors"):
            info = cik_map.get(sym.upper())
            if not info:
                missing.append(sym)
                store.upsert_company(sym, "", "", "", "Unknown")
                continue
            try:
                name, sic, industry = fetch_one(sym, info["cik"])
                sector = sic_to_sector(sic)
                store.upsert_company(sym, name or info["name"], sic, industry, sector)
                counts[sector] = counts.get(sector, 0) + 1
            except Exception as e:  # noqa: BLE001 - best-effort, keep going
                missing.append(f"{sym}({e})")
                store.upsert_company(sym, info["name"], "", "", "Unknown")
            time.sleep(delay)
        print("\nSector distribution:")
        for sec, n in sorted(counts.items(), key=lambda x: -x[1]):
            print(f"  {sec:<24} {n}")
        if missing:
            print(f"\n{len(missing)} not resolved (e.g. {missing[:5]})")
        return counts
    finally:
        store.close()


def main():
    parser = argparse.ArgumentParser(description="Fetch sectors from SEC EDGAR.")
    parser.add_argument("--symbols", nargs="*", help="Specific symbols (default: all in store).")
    parser.add_argument("--no-resume", action="store_true", help="Refetch even if cached.")
    args = parser.parse_args()

    store = BarStore(read_only=True)
    symbols = args.symbols or store.list_symbols()
    store.close()
    run(symbols, resume=not args.no_resume)


if __name__ == "__main__":
    main()
