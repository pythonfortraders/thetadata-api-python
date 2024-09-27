import pytest
import pandas as pd
import requests

from unittest.mock import patch
from src.stocks_historical import ThetaDataStocksHistorical
from src.utils import is_valid_date_format


@pytest.fixture
def historical_data():
    return ThetaDataStocksHistorical(log_level="WARNING", output_dir="./")


def test_get_eod_report_invalid_date_format(historical_data):
    result = historical_data.get_eod_report("AAPL", "2024-01-01", "2024-01-31")
    assert result is None


def test_get_eod_report_valid_date_format(historical_data):
    mock_response = {
        "header": {"format": ["date", "open", "high", "low", "close", "volume"]},
        "response": [
            ["20240101", "100.0", "101.0", "99.0", "100.5", "1000000"],
            ["20240102", "100.5", "102.0", "100.0", "101.5", "1100000"],
        ],
    }
    with patch.object(
        ThetaDataStocksHistorical, "send_request", return_value=mock_response
    ):
        result = historical_data.get_eod_report("AAPL", "20240101", "20240102")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["date", "open", "high", "low", "close", "volume"]


def test_get_eod_report_no_data(historical_data):
    with patch.object(ThetaDataStocksHistorical, "send_request", return_value=None):
        result = historical_data.get_eod_report("AAPL", "20240101", "20240102")

    assert result is None


def test_get_quotes(historical_data):
    mock_response = {
        "header": {"format": ["date", "time", "bid", "ask", "bidsize", "asksize"]},
        "response": [
            ["20240101", "093000", "100.0", "100.1", "100", "100"],
            ["20240101", "093001", "100.1", "100.2", "200", "200"],
        ],
    }
    with patch.object(
        ThetaDataStocksHistorical, "send_request", return_value=mock_response
    ):
        result = historical_data.get_quotes(
            "AAPL", "20240101", "20240102", interval="60000"
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["date", "time", "bid", "ask", "bidsize", "asksize"]


def test_get_ohlc(historical_data):
    mock_response = {
        "header": {
            "format": [
                "ms_of_day",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "count",
                "date",
            ]
        },
        "response": [
            ["33000000", "100.0", "101.0", "99.0", "100.5", "1000", "50", "20240101"],
            ["36000000", "100.5", "102.0", "100.0", "101.5", "1100", "55", "20240101"],
        ],
    }
    with patch.object(
        ThetaDataStocksHistorical, "send_request", return_value=mock_response
    ):
        result = historical_data.get_ohlc(
            "AAPL", "20240101", "20240102", interval="3600000"
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == [
        "ms_of_day",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "count",
        "date",
    ]


def test_get_trades(historical_data):
    mock_response = {
        "header": {"format": ["date", "time", "price", "size", "conditions"]},
        "response": [
            ["20240101", "093000", "100.0", "100", "@"],
            ["20240101", "093001", "100.1", "200", "@"],
        ],
    }
    with patch.object(
        ThetaDataStocksHistorical, "send_request", return_value=mock_response
    ):
        result = historical_data.get_trades("AAPL", "20240101", "20240102")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["date", "time", "price", "size", "conditions"]


def test_get_trade_quote(historical_data):
    mock_response = {
        "header": {
            "format": [
                "date",
                "time",
                "price",
                "size",
                "conditions",
                "bid",
                "ask",
                "bidsize",
                "asksize",
            ]
        },
        "response": [
            ["20240101", "093000", "100.0", "100", "@", "99.9", "100.1", "100", "100"],
            ["20240101", "093001", "100.1", "200", "@", "100.0", "100.2", "200", "200"],
        ],
    }
    with patch.object(
        ThetaDataStocksHistorical, "send_request", return_value=mock_response
    ):
        result = historical_data.get_trade_quote("AAPL", "20240101", "20240102")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == [
        "date",
        "time",
        "price",
        "size",
        "conditions",
        "bid",
        "ask",
        "bidsize",
        "asksize",
    ]


def test_get_splits(historical_data):
    mock_response = {
        "header": {"format": ["date", "ratio"]},
        "response": [
            ["20230701", "4:1"],
            ["20220801", "2:1"],
        ],
    }
    with patch.object(
        ThetaDataStocksHistorical, "send_request", return_value=mock_response
    ):
        result = historical_data.get_splits("AAPL", "20220101", "20240102")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["date", "ratio"]


def test_get_dividends(historical_data):
    mock_response = {
        "header": {"format": ["date", "amount"]},
        "response": [
            ["20230301", "0.23"],
            ["20230601", "0.24"],
        ],
    }
    with patch.object(
        ThetaDataStocksHistorical, "send_request", return_value=mock_response
    ):
        result = historical_data.get_dividends("AAPL", "20230101", "20240102")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["date", "amount"]


def test_send_request(historical_data):
    mock_response = {"key": "value"}
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.raise_for_status.return_value = None
        result = historical_data.send_request("/test_endpoint", {"param": "value"})

    assert result == mock_response


def test_send_request_error(historical_data):
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.RequestException("Test error")
        result = historical_data.send_request("/test_endpoint", {"param": "value"})

    assert result is None


def test_is_valid_date_format():
    assert not is_valid_date_format("2024-01-01")
    assert not is_valid_date_format("202401")
    assert is_valid_date_format("20240101")
    assert is_valid_date_format("20240132")
