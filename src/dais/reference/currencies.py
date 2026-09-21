"""The shared currency reference table, core.t_ref_currency - the list of
valid ISO-4217 codes that FX (and any other currency-bearing) feeds validate
against via a `lookup: type: sql` rule.

ensure_currency_reference() is idempotent: it creates the table if it does
not exist and inserts only the codes that are missing, never touching or
removing rows already there - so it's safe to re-run, and anything added by
hand (or by another team) is preserved.
"""
from __future__ import annotations

from dais.resilience.connectors.base import ColumnDef, DatabaseConnector

SCHEMA = "core"
TABLE = "t_ref_currency"

# (code, name). Includes every currency in the FX sample file (USD, INR, AUD,
# HKD) plus the other major/commonly-traded ISO-4217 codes.
DEFAULT_CURRENCIES: list[tuple[str, str]] = [
    ("USD", "US Dollar"), ("EUR", "Euro"), ("GBP", "Pound Sterling"), ("JPY", "Japanese Yen"),
    ("INR", "Indian Rupee"), ("AUD", "Australian Dollar"), ("HKD", "Hong Kong Dollar"),
    ("CAD", "Canadian Dollar"), ("CHF", "Swiss Franc"), ("CNY", "Chinese Yuan Renminbi"),
    ("SGD", "Singapore Dollar"), ("NZD", "New Zealand Dollar"), ("SEK", "Swedish Krona"),
    ("NOK", "Norwegian Krone"), ("DKK", "Danish Krone"), ("KRW", "South Korean Won"),
    ("TWD", "New Taiwan Dollar"), ("THB", "Thai Baht"), ("MYR", "Malaysian Ringgit"),
    ("IDR", "Indonesian Rupiah"), ("PHP", "Philippine Peso"), ("BRL", "Brazilian Real"),
    ("MXN", "Mexican Peso"), ("ZAR", "South African Rand"), ("AED", "UAE Dirham"),
    ("SAR", "Saudi Riyal"), ("PLN", "Polish Zloty"), ("CZK", "Czech Koruna"),
    ("HUF", "Hungarian Forint"), ("TRY", "Turkish Lira"), ("ILS", "Israeli New Shekel"),
]


def ensure_currency_reference(
    connector: DatabaseConnector, currencies: list[tuple[str, str]] | None = None
) -> list[str]:
    """Creates core.t_ref_currency if needed and inserts any missing codes.
    Returns the codes that were newly inserted (empty if nothing to add)."""
    connector.create_table_if_not_exists(
        SCHEMA,
        TABLE,
        [ColumnDef("currency_code", "TEXT"), ColumnDef("currency_name", "TEXT")],
        unique_columns=["currency_code"],
    )
    added = []
    for code, name in currencies or DEFAULT_CURRENCIES:
        if not connector.value_exists(SCHEMA, TABLE, "currency_code", code):
            connector.bulk_insert(SCHEMA, TABLE, ["currency_code", "currency_name"], [(code, name)])
            added.append(code)
    return added
