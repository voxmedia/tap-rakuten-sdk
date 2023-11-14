"""Stream type classes for tap-rakuten."""

from __future__ import annotations

import typing as t
from pathlib import Path

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_rakuten.client import RakutenStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")\


class ReportStream(RakutenStream):
    """Define custom stream."""

    name = "Report"
    path = ""
    replication_key = 'transaction_date'
    schema_filepath = SCHEMAS_DIR / "report.schema.json"