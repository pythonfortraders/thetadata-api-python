import pytest
from unittest.mock import patch
from stocks_historical import ThetaDataStocksHistorical
import pandas as pd


@pytest.fixture
def historical_data():
    return ThetaDataStocksHistorical(enable_logging=False, use_df=True)


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


def test_get_eod_report_use_df_false(historical_data):
    historical_data.use_df = False
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

    assert isinstance(result, dict)
    assert result == mock_response
