import requests
import logging
import pandas as pd

import requests
import logging
import pandas as pd


class ThetaDataStocksHistorical:
    def __init__(self, log_level: int = logging.WARNING, use_df: bool = False) -> None:
        self.use_df = use_df

        # Configure logging
        logging.basicConfig(
            level=log_level, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def __init__(self, enable_logging: bool = False, use_df: bool = True) -> None:
        self.enable_logging = enable_logging
        self.use_df = use_df

        # Configure logging
        if self.enable_logging:
            logging.basicConfig(
                level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
            )
        self.logger = logging.getLogger(__name__)

    def send_request(self, endpoint: str, params: dict) -> dict | None:
        url = f"http://127.0.0.1:25510{endpoint}"
        headers = {"Accept": "application/json"}
        response = None

        try:
            if self.enable_logging:
                self.logger.info(f"Sending request to {url} with params: {params}")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            if self.enable_logging:
                self.logger.info("Request successful")
            return response.json()
        except requests.RequestException as e:
            if self.enable_logging:
                self.logger.error(f"An error occurred: {e}")
                self.logger.error(
                    f"Response text: {response.text if response else 'No response'}"
                )
            return None

    def get_eod_report(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | dict | None:
        # Check if dates have the correct format
        if not (
            self._is_valid_date_format(start_date)
            and self._is_valid_date_format(end_date)
        ):
            if self.enable_logging:
                self.logger.error(
                    f"Invalid date format. Expected format: 'YYYYMMDD'. Got start_date: {start_date}, end_date: {end_date}"
                )
            return None

        if self.enable_logging:
            self.logger.info(
                f"Getting EOD report for {symbol} from {start_date} to {end_date}"
            )
        endpoint = "/v2/hist/stock/eod"
        params = {"root": symbol, "start_date": start_date, "end_date": end_date}
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def _is_valid_date_format(self, date_string: str) -> bool:
        return len(date_string) == 8 and date_string.isdigit()

    def get_quotes(
        self, symbol: str, start_date: str, end_date: str, interval: str = "900000"
    ) -> pd.DataFrame | dict | None:
        if self.enable_logging:
            self.logger.info(
                f"Getting quotes for {symbol} from {start_date} to {end_date}"
            )
        endpoint = "/v2/hist/stock/quote"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": interval,
        }
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_ohlc(self) -> None:
        self.logger.info("get_ohlc function called")
        pass

    def get_trades(self) -> None:
        self.logger.info("get_trades function called")
        pass

    def get_trade_quote(self) -> None:
        self.logger.info("get_trade_quote function called")
        pass

    def get_splits(self) -> None:
        self.logger.info("get_splits function called")
        pass

    def get_dividends(self) -> None:
        self.logger.info("get_dividends function called")
        pass


# Usage examples

if __name__ == "__main__":
    # Toggle example cases
    run_apple_eod_example = True
    run_microsoft_quotes_example = True

    historical_data = ThetaDataStocksHistorical(enable_logging=True, use_df=True)

    # Example 1: Get EOD report for AAPL
    if run_apple_eod_example:
        apple_eod = historical_data.get_eod_report("AAPL", "20240101", "20240131")
        if not apple_eod.empty:
            historical_data.logger.info("Apple EOD Report received")
            print("Apple EOD Report:")
            print(apple_eod)
        else:
            historical_data.logger.warning("Failed to get Apple EOD Report")

    # Example 2: Get quotes for MSFT
    if run_microsoft_quotes_example:
        microsoft_quotes = historical_data.get_quotes(
            "MSFT", "20240101", "20240131", interval="3600000"
        )
        if not microsoft_quotes.empty:
            historical_data.logger.info("Microsoft Quotes received")
            print("\nMicrosoft Quotes:")
            print(microsoft_quotes)
        else:
            historical_data.logger.warning("Failed to get Microsoft Quotes")
