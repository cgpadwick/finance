"""Pluggable market-data sources. All return split+dividend-adjusted daily bars."""

from finance.datasources.base import DataSource, get_data_source

__all__ = ["DataSource", "get_data_source"]
