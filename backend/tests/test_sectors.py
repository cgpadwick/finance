"""Tests for the SIC -> sector mapping (pure logic, no network)."""

from finance.sectors import sic_to_sector


def test_known_sic_mappings():
    cases = {
        3674: "Technology",          # semiconductors (MU, AXTI)
        3559: "Technology",          # semiconductor fab equipment (LRCX)
        3677: "Technology",          # electronic components (Bel Fuse)
        7372: "Technology",          # prepackaged software
        2834: "Health Care",         # pharmaceutical preparations (ARWR)
        3845: "Health Care",         # electromedical devices (AXGN)
        8731: "Health Care",         # commercial biological research (biotech)
        3711: "Consumer Discretionary",  # motor vehicles
        3721: "Industrials",         # aircraft
        4911: "Utilities",           # electric services
        6021: "Financials",          # national commercial banks
        6798: "Real Estate",         # REITs
        1311: "Energy",              # crude petroleum & natural gas
        4812: "Communication Services",  # radiotelephone
    }
    for sic, expected in cases.items():
        assert sic_to_sector(sic) == expected, f"SIC {sic} -> {sic_to_sector(sic)}, want {expected}"


def test_bad_sic_is_unknown():
    assert sic_to_sector(None) == "Unknown"
    assert sic_to_sector("") == "Unknown"
    assert sic_to_sector("not-a-number") == "Unknown"
