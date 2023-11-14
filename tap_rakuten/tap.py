"""Rakuten tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_rakuten import streams


class TapRakuten(Tap):
    """Rakuten tap class."""

    name = "tap-rakuten"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "region",
            th.StringType,
            required=True,
            description="region",
        ),
        th.Property(
            "report_slug",
            th.StringType,
            required=True,
            description="name of the report to be ingested",
        ),
        th.Property(
            "date_type",
            th.StringType,
            required=True,
            description="which date to include for each transaction. Acceptable values are `transaction` and `process`.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType())
    ).to_dict()

    def discover_streams(self) -> list[streams.RakutenStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.ReportStream(self),
        ]


if __name__ == "__main__":
    TapRakuten.cli()
