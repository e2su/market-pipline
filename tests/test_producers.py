import json

import pytest

from producers.binance_producer import parse_trade
from producers.stock_producer import RateLimitError, parse_quote


def test_parse_trade():
    message = json.dumps({"e": "trade", "s": "BTCUSDT", "p": "64592.01000000", "q": "0.00150000", "T": 1720000000123})
    assert parse_trade(message) == {
        "symbol": "BTCUSDT",
        "price": 64592.01,
        "quantity": 0.0015,
        "timestamp": 1720000000123,
    }


def test_parse_trade_ignores_non_trade_messages():
    assert parse_trade(json.dumps({"result": None, "id": 1})) is None


def test_parse_quote():
    data = {"Global Quote": {
        "01. symbol": "AAPL",
        "05. price": "231.5400",
        "06. volume": "48123456",
        "07. latest trading day": "2026-09-28",
    }}
    assert parse_quote(data) == {"symbol": "AAPL", "price": 231.54, "volume": 48123456, "timestamp": "2026-09-28"}


def test_parse_quote_empty_returns_none():
    assert parse_quote({"Global Quote": {}}) is None


@pytest.mark.parametrize("key", ["Note", "Information"])
def test_parse_quote_rate_limit_raises(key):
    with pytest.raises(RateLimitError):
        parse_quote({key: "You have reached the 25 requests/day limit."})
