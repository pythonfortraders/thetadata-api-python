import pytest
from unittest.mock import patch
from src.stocks import ThetaDataStocksSnapshot
import pandas as pd
import requests


@pytest.fixture
def snapshot_data():
    return ThetaDataStocksSnapshot(log_level="WARNING", output_dir="./")


def test_get_quotes(snapshot_data):
    mock_response = {
        "header": {
            "format": [
                "ms_of_day",
                "bid_size",
                "bid_exchange",
                "bid",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask",
                "ask_condition",
                "date",
            ]
        },
        "response": [
            [
                "36000000",
                "100",
                "N",
                "150.00",
                "R",
                "100",
                "N",
                "150.10",
                "R",
                "20240101",
            ],
            [
                "36001000",
                "200",
                "N",
                "150.05",
                "R",
                "200",
                "N",
                "150.15",
                "R",
                "20240101",
            ],
        ],
    }
    with patch.object(
        ThetaDataStocksSnapshot, "send_request", return_value=mock_response
    ):
        result = snapshot_data.get_quotes("AAPL")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == [
        "ms_of_day",
        "bid_size",
        "bid_exchange",
        "bid",
        "bid_condition",
        "ask_size",
        "ask_exchange",
        "ask",
        "ask_condition",
        "date",
    ]


def test_get_quotes_with_venue(snapshot_data):
    mock_response = {
        "header": {
            "format": [
                "ms_of_day",
                "bid_size",
                "bid_exchange",
                "bid",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask",
                "ask_condition",
                "date",
            ]
        },
        "response": [
            [
                "36000000",
                "100",
                "N",
                "150.00",
                "R",
                "100",
                "N",
                "150.10",
                "R",
                "20240101",
            ],
        ],
    }
    with patch.object(
        ThetaDataStocksSnapshot, "send_request", return_value=mock_response
    ):
        result = snapshot_data.get_quotes("AAPL", venue="nqb")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1


def test_get_quotes_invalid_venue(snapshot_data):
    with pytest.raises(ValueError):
        snapshot_data.get_quotes("AAPL", venue="invalid")


def test_get_bulk_quotes(snapshot_data):
    mock_response = {
        "header": {
            "format": [
                "ms_of_day",
                "bid_size",
                "bid_exchange",
                "bid",
                "bid_condition",
                "ask_size",
                "ask_exchange",
                "ask",
                "ask_condition",
                "date",
            ]
        },
        "response": [
            [
                "36000000",
                "100",
                "N",
                "150.00",
                "R",
                "100",
                "N",
                "150.10",
                "R",
                "20240101",
            ],
            [
                "36000000",
                "200",
                "N",
                "250.00",
                "R",
                "200",
                "N",
                "250.10",
                "R",
                "20240101",
            ],
        ],
    }
    with patch.object(
        ThetaDataStocksSnapshot, "send_request", return_value=mock_response
    ):
        result = snapshot_data.get_bulk_quotes(["AAPL", "MSFT"])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_get_ohlc(snapshot_data):
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
            [
                "36000000",
                "150.00",
                "151.00",
                "149.50",
                "150.50",
                "1000000",
                "5000",
                "20240101",
            ],
        ],
    }
    with patch.object(
        ThetaDataStocksSnapshot, "send_request", return_value=mock_response
    ):
        result = snapshot_data.get_ohlc("AAPL")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
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


def test_get_bulk_ohlc(snapshot_data):
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
            [
                "36000000",
                "150.00",
                "151.00",
                "149.50",
                "150.50",
                "1000000",
                "5000",
                "20240101",
            ],
            [
                "36000000",
                "250.00",
                "251.00",
                "249.50",
                "250.50",
                "2000000",
                "6000",
                "20240101",
            ],
        ],
    }
    with patch.object(
        ThetaDataStocksSnapshot, "send_request", return_value=mock_response
    ):
        result = snapshot_data.get_bulk_ohlc(["AAPL", "MSFT"])

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_get_trades(snapshot_data):
    mock_response = {
        "header": {
            "format": [
                "ms_of_day",
                "sequence",
                "ext_condition1",
                "ext_condition2",
                "ext_condition3",
                "ext_condition4",
                "condition",
                "size",
                "exchange",
                "price",
                "condition_flags",
                "price_flags",
                "volume_type",
                "records_back",
                "date",
            ]
        },
        "response": [
            [
                "36000000",
                "1",
                "",
                "",
                "",
                "",
                "@",
                "100",
                "N",
                "150.00",
                "",
                "",
                "",
                "0",
                "20240101",
            ],
        ],
    }
    with patch.object(
        ThetaDataStocksSnapshot, "send_request", return_value=mock_response
    ):
        result = snapshot_data.get_trades("AAPL")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert list(result.columns) == [
        "ms_of_day",
        "sequence",
        "ext_condition1",
        "ext_condition2",
        "ext_condition3",
        "ext_condition4",
        "condition",
        "size",
        "exchange",
        "price",
        "condition_flags",
        "price_flags",
        "volume_type",
        "records_back",
        "date",
    ]


def test_send_request(snapshot_data):
    mock_response = {"key": "value"}
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.raise_for_status.return_value = None
        result = snapshot_data.send_request("/test_endpoint", {"param": "value"})

    assert result == mock_response


def test_send_request_error(snapshot_data):
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.RequestException("Test error")
        result = snapshot_data.send_request("/test_endpoint", {"param": "value"})

    assert result is None
