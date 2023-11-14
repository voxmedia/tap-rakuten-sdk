"""REST client handling, including RakutenStream base class."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import csv
import io
import pendulum
import requests
from singer_sdk.authenticators import BearerTokenAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class DayChunkPaginator(BaseAPIPaginator):
    """A paginator that increments days in a date range."""

    def __init__(self, start_date: str, increment: int = 1, *args: Any, **kwargs: Any) -> None:
        super().__init__(start_date)
        self._value = pendulum.parse(start_date)
        self._end = pendulum.today()
        self._increment = increment

    @property
    def end_date(self):
        """Get the end pagination value.

        Returns:
            End date.
        """
        return self._end

    @property
    def increment(self):
        """Get the paginator increment.

        Returns:
            Increment.
        """
        return self._increment

    def get_next(self, response: requests.Response):
        return self.current_value + pendulum.duration(days=self.increment) if self.has_more(response) else None

    def has_more(self, response: requests.Response) -> bool:
        """Checks if there are more days to process.

        Args:
            response: API response object.

        Returns:
            Boolean flag used to indicate if the endpoint has more pages.
        """
        return self.current_value + pendulum.duration(days=self.increment) < self.end_date


class RakutenStream(RESTStream):
    """Rakuten stream class."""

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return f"https://ran-reporting.rakutenmarketing.com/{self.config.get('region')}/reports/{self.config.get('report_slug')}/filters"

    @property
    def next_page_token(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self._starting_replication_key_value
    
    def get_new_paginator(self) -> BaseAPIPaginator:
        """
            Return a new paginator object.
            finds either the start date from config or the last replication key value.
            Looks back 60 days to ensure we capture records that may have changed.
            60 days is mostly arbitrary but felt like a conservative enough return window.
        """
        start_date = (pendulum.parse(self._starting_replication_key_value) - pendulum.duration(days=60)).format('YYYY-MM-DD')
        return DayChunkPaginator(start_date=start_date, increment=28)

    def get_records(self, context: Optional[dict]) -> Iterable[dict[str, Any]]:
        # Adding replication key value to the stream properties since it's currently impossible to add context to the paginator object
        # https://github.com/meltano/sdk/issues/1520
        self._starting_replication_key_value = self.get_starting_replication_key_value(context)
        yield from super().get_records(context=context)


    def get_url_params(
        self,
        context: dict | None,  # noqa: ARG002
        next_page_token: Any | None,  # noqa: ANN401
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {
            'include_summary': 'N',
            'network': 1,
            'tz': 'GMT',
            'date_type': self.config.get('date_type'),
            'token': self.config.get('auth_token'),
        }
        next_page_date = next_page_token.format('YYYY-MM-DD') if next_page_token else None
        if next_page_date:
            params['start_date'] = next_page_date
            end_date = pendulum.parse(next_page_date) + pendulum.duration(days=28)
            if end_date > pendulum.today():
                end_date = pendulum.today()
            params['end_date'] = end_date.format('YYYY-MM-DD')
        return params


    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        reader = csv.DictReader(
            io.TextIOWrapper(io.BytesIO(response.content), encoding='utf-8'),
            delimiter=',',
            quotechar='"',
            )
        for row in reader:
            yield row

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        """As needed, append or transform raw data to match expected structure.

        Args:
            row: An individual record from the stream.
            context: The stream context.

        Returns:
            The updated record dictionary, or ``None`` to skip the record.
        """
        # TODO: Delete this method if not needed.
        new_row = {}
        for key, value in row.items():
            new_row[key.lower().replace(' ', '_').replace('#', 'num')] = value
        new_row['transaction_date'] = pendulum.from_format(new_row['transaction_date'], 'M/D/YY').format('YYYY-MM-DD HH:mm:ss')
        for int_col in ['mid', 'num_of_impressions', 'num_of_clicks', 'num_of_orders']:
            new_row[int_col] = int(new_row[int_col].replace(',', '').replace('$', ''))
        for float_col in ['gross_sales', 'sales', 'gross_total_commissions', 'total_commission']:
            new_row[float_col] = float(new_row[float_col].replace(',', '').replace('$', ''))
        return new_row
