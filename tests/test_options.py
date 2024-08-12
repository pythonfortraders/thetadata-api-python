import pytest
from unittest.mock import patch
from src.options import ThetaDataOptions
import pandas as pd
import requests


@pytest.fixture
def options_data():
    return ThetaDataOptions(log_level="WARNING", output_dir="./")


def test_get_historical_quotes(options_data):
    mock_response = {
        "header": {"format": ["ms_of_day", "bid", "ask", "bidsize", "asksize", "date"]},
        "response": [
            ["34200000", "100.0", "100.1", "10", "10", "20240101"],
            ["34200000", "100.1", "100.2", "20", "20", "20240102"],
        ],
    }
    with patch.object(ThetaDataOptions, "send_request", return_value=mock_response):
        result = options_data.get_historical_quotes(
            "AAPL", "20240119", 170000, "C", "20240101", "20240102"
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["ms_of_day", "bid", "ask", "bidsize", "asksize", "date"]


def test_get_historical_trades(options_data):
    mock_response = {
        "header": {"format": ["ms_of_day", "price", "size", "exchange", "condition", "date"]},
        "response": [
            ["34200000", "100.0", "10", "CBOE", "@", "20240101"],
            ["34200000", "100.1", "20", "CBOE", "@", "20240102"],
        ],
    }
    with patch.object(ThetaDataOptions, "send_request", return_value=mock_response):
        result = options_data.get_historical_trades(
            "AAPL", "20240119", 170000, "C", "20240101", "20240102"
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["ms_of_day", "price", "size", "exchange", "condition", "date"]


def test_get_historical_greeks(options_data):
    mock_response = {
        "header": {"format": ["ms_of_day", "delta", "gamma", "theta", "vega", "date"]},
        "response": [
            ["34200000", "0.5", "0.05", "-0.1", "0.2", "20240101"],
            ["34200000", "0.51", "0.06", "-0.11", "0.21", "20240102"],
        ],
    }
    with patch.object(ThetaDataOptions, "send_request", return_value=mock_response):
        result = options_data.get_historical_greeks(
            "AAPL", "20240119", 170000, "C", "20240101", "20240102"
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["ms_of_day", "delta", "gamma", "theta", "vega", "date"]


def test_get_historical_greeks_second_order(options_data):
    mock_response = {
        "header": {"format": ["ms_of_day", "charm", "vanna", "vomma", "date"]},
        "response": [
            ["34200000", "0.01", "0.02", "0.03", "20240101"],
            ["34200000", "0.011", "0.021", "0.031", "20240102"],
        ],
    }
    with patch.object(ThetaDataOptions, "send_request", return_value=mock_response):
        result = options_data.get_historical_greeks_second_order(
            "AAPL", "20240119", 170000, "C", "20240101", "20240102"
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["ms_of_day", "charm", "vanna", "vomma", "date"]


def test_get_historical_ohlc(options_data):
    mock_response = {
        "header": {"format": ["ms_of_day", "open", "high", "low", "close", "date"]},
        "response": [
            ["34200000", "100.0", "101.0", "99.0", "100.5", "20240101"],
            ["34200000", "100.5", "102.0", "100.0", "101.5", "20240102"],
        ],
    }
    with patch.object(ThetaDataOptions, "send_request", return_value=mock_response):
        result = options_data.get_historical_ohlc(
            "AAPL", "20240119", 170000, "C", "20240101", "20240102", 3000000
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["ms_of_day", "open", "high", "low", "close", "date"]


def test_get_historical_open_interest(options_data):
    mock_response = {
        "header": {"format": ["open_interest", "date"]},
        "response": [
            ["1000", "20240101"],
            ["1100", "20240102"],
        ],
    }
    with patch.object(ThetaDataOptions, "send_request", return_value=mock_response):
        result = options_data.get_historical_open_interest(
            "AAPL", "20240119", 170000, "C", "20240101", "20240102"
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["open_interest", "date"]



def test_get_bulk_quote(options_data):
    mock_response = {
        "header": {"format": ["ms_of_day", "strike", "right", "bid", "ask", "bidsize", "asksize", "date"]},
        "response": [
            ["34200000", "170000", "C", "10.0", "10.1", "10", "10", "20240101"],
            ["34200000", "170000", "P", "5.0", "5.1", "20", "20", "20240101"],
        ],
    }
    with patch.object(ThetaDataOptions, "send_request", return_value=mock_response):
        result = options_data.get_bulk_quote(
            "AAPL", "20240119", "20240101", "20240102", 3000000
        )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2
    assert list(result.columns) == ["ms_of_day", "strike", "right", "bid", "ask", "bidsize", "asksize", "date"]


def test_send_request(options_data):
    mock_response = {"key": "value"}
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = mock_response
        mock_get.return_value.raise_for_status.return_value = None
        result = options_data.send_request("/test_endpoint", {"param": "value"})

    assert result == mock_response


def test_send_request_error(options_data):
    with patch("requests.get") as mock_get:
        mock_get.side_effect = requests.RequestException("Test error")
        result = options_data.send_request("/test_endpoint", {"param": "value"})

    assert result is None
