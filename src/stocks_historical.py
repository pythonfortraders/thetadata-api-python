import requests
import logging
import pandas as pd


class ThetaDataStocksHistorical:
    def __init__(self, log_level: int = logging.WARNING, use_df: bool = False) -> None:
        """
        Initialize the ThetaDataStocksHistorical class.

        Parameters:
        log_level (int): The logging level to use. Defaults to logging.WARNING.
        use_df (bool): Whether to return results as pandas DataFrames. Defaults to False.

        This constructor sets up logging and initializes the use_df attribute.
        """
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
        """
        Send a GET request to the specified endpoint with the given parameters.

        Args:
            endpoint (str): The API endpoint to send the request to.
            params (dict): A dictionary of query parameters to include in the request.

        Returns:
            dict | None: The JSON response from the API if successful, or None if an error occurs.

        This method handles the HTTP request, logging, and error handling for all API calls.
        It uses a base URL of 'http://127.0.0.1:25510'.
        If logging is enabled, it logs the request details and the outcome of the request.
        """
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
        """
        Get end-of-day report for a given symbol and date range.

        The returned data includes the following columns:
        - ms_of_day: Milliseconds since midnight for the start of the interval
        - ms_of_day2: Milliseconds since midnight for the end of the interval
        - open: Opening price for the interval
        - high: Highest price during the interval
        - low: Lowest price during the interval
        - close: Closing price for the interval
        - volume: Total trading volume during the interval
        - count: Number of trades during the interval
        - bid_size: Size of the best bid
        - bid_exchange: Exchange code for the best bid
        - bid: Best bid price
        - bid_condition: Condition code for the best bid
        - ask_size: Size of the best ask
        - ask_exchange: Exchange code for the best ask
        - ask: Best ask price
        - ask_condition: Condition code for the best ask
        - date: Date of the data point

        Args:
            symbol (str): The stock symbol
            start_date (str): Start date in 'YYYYMMDD' format
            end_date (str): End date in 'YYYYMMDD' format

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of EOD data, or None if request fails
        """
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
        """
        Get quotes for a given symbol and date range.

        The returned data includes the following columns:
        - ms_of_day: Milliseconds since midnight
        - bid_size: Size of the best bid
        - bid_exchange: Exchange code for the best bid
        - bid: Best bid price
        - bid_condition: Condition code for the best bid
        - ask_size: Size of the best ask
        - ask_exchange: Exchange code for the best ask
        - ask: Best ask price
        - ask_condition: Condition code for the best ask
        - date: Date of the data point

        Args:
            symbol (str): The stock symbol
            start_date (str): Start date in 'YYYYMMDD' format
            end_date (str): End date in 'YYYYMMDD' format
            interval (str): Interval in milliseconds (default: "900000")

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of quote data, or None if request fails
        """
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

    def get_ohlc(
        self, symbol: str, start_date: str, end_date: str, interval: str = "900000"
    ) -> pd.DataFrame | dict | None:
        """
        Get OHLC (Open, High, Low, Close) data for a given symbol and date range.

        The returned data includes the following columns:
        - ms_of_day: Milliseconds since midnight
        - open: Opening price
        - high: Highest price
        - low: Lowest price
        - close: Closing price
        - volume: Trading volume
        - count: Number of trades
        - date: Date of the data point

        Args:
            symbol (str): The stock symbol
            start_date (str): Start date in 'YYYYMMDD' format
            end_date (str): End date in 'YYYYMMDD' format
            interval (str): Interval in milliseconds (default: "900000")

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of OHLC data, or None if request fails
        """
        if self.enable_logging:
            self.logger.info(
                f"Getting OHLC for {symbol} from {start_date} to {end_date}"
            )
        endpoint = "/v2/hist/stock/ohlc"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": interval,
        }
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = [
                "ms_of_day",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "count",
                "date",
            ]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_trades(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | dict | None:
        """
        Get historical trade data for a given symbol and date range.

        The returned data includes the following columns:
        - ms_of_day: Milliseconds since midnight
        - sequence: Sequence number of the trade
        - ext_condition1: Extended trading condition 1
        - ext_condition2: Extended trading condition 2
        - ext_condition3: Extended trading condition 3
        - ext_condition4: Extended trading condition 4
        - condition: Trading condition
        - size: Trade size
        - exchange: Exchange where the trade occurred
        - price: Trade price
        - condition_flags: Condition flags
        - price_flags: Price flags
        - volume_type: Volume type
        - records_back: Number of records back
        - date: Date of the trade

        Args:
            symbol (str): The stock symbol
            start_date (str): Start date in 'YYYYMMDD' format
            end_date (str): End date in 'YYYYMMDD' format

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of trade data, or None if request fails
        """
        if self.enable_logging:
            self.logger.info(
                f"Getting trades for {symbol} from {start_date} to {end_date}"
            )
        endpoint = "/v2/hist/stock/trade"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
        }
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_trade_quote(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | dict | None:
        """
        Get historical trade and quote data for a given symbol and date range.

        The returned data includes the following columns:
        - ms_of_day: Milliseconds since midnight for the trade
        - sequence: Sequence number of the trade
        - ext_condition1: Extended trading condition 1
        - ext_condition2: Extended trading condition 2
        - ext_condition3: Extended trading condition 3
        - ext_condition4: Extended trading condition 4
        - condition: Trading condition
        - size: Trade size
        - exchange: Exchange where the trade occurred
        - price: Trade price
        - condition_flags: Condition flags
        - price_flags: Price flags
        - volume_type: Volume type
        - records_back: Number of records back
        - ms_of_day2: Milliseconds since midnight for the quote
        - bid_size: Size of the NBBO bid
        - bid_exchange: Exchange of the NBBO bid
        - bid: NBBO bid price
        - bid_condition: Condition of the NBBO bid
        - ask_size: Size of the NBBO ask
        - ask_exchange: Exchange of the NBBO ask
        - ask: NBBO ask price
        - ask_condition: Condition of the NBBO ask
        - date: Date of the trade and quote

        Args:
            symbol (str): The stock symbol
            start_date (str): Start date in 'YYYYMMDD' format
            end_date (str): End date in 'YYYYMMDD' format

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of trade and quote data, or None if request fails
        """
        if self.enable_logging:
            self.logger.info(
                f"Getting trade quotes for {symbol} from {start_date} to {end_date}"
            )
        endpoint = "/v2/hist/stock/trade_quote"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
        }
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_splits(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | dict | None:
        """
        Get stock split data for a given symbol and date range.

        The returned data includes the following columns:
        - ms_of_day: Milliseconds since midnight
        - split_date: Date of the stock split
        - before_shares: Number of shares before the split
        - after_shares: Number of shares after the split
        - date: Date of the record

        Args:
            symbol (str): The stock symbol
            start_date (str): Start date in 'YYYYMMDD' format
            end_date (str): End date in 'YYYYMMDD' format

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of stock split data, or None if request fails
        """
        if self.enable_logging:
            self.logger.info(
                f"Getting splits for {symbol} from {start_date} to {end_date}"
            )
        endpoint = "/v2/hist/stock/split"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
        }
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_dividends(
        self, symbol: str, start_date: str, end_date: str
    ) -> pd.DataFrame | dict | None:
        """
        Get dividend data for a given symbol and date range.

        The returned data includes the following columns:
        - ms_of_day: Milliseconds since midnight
        - ex_date: Ex-dividend date
        - record_date: Record date for the dividend
        - payment_date: Date when the dividend is paid
        - ann_date: Announcement date of the dividend
        - dividend_amount: Amount of the dividend
        - undefined: Undefined column
        - less_amount: Less amount (if applicable)
        - date: Date of the record

        Args:
            symbol (str): The stock symbol
            start_date (str): Start date in 'YYYYMMDD' format
            end_date (str): End date in 'YYYYMMDD' format

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of dividend data, or None if request fails
        """
        if self.enable_logging:
            self.logger.info(
                f"Getting dividends for {symbol} from {start_date} to {end_date}"
            )
        endpoint = "/v2/hist/stock/dividend"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
        }
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response


# Usage examples

if __name__ == "__main__":
    # Toggle example cases
    run_apple_eod_example = False
    run_microsoft_quotes_example = False
    run_google_ohlc_example = False
    run_tesla_trades_example = False
    run_amazon_trade_quote_example = True
    run_nvidia_splits_example = True
    run_intel_dividends_example = True

    historical_data = ThetaDataStocksHistorical(enable_logging=True, use_df=True)

    # Example 1: Get EOD report for AAPL
    if run_apple_eod_example:
        apple_eod = historical_data.get_eod_report("AAPL", "20240101", "20240131")
        if apple_eod is None:
            historical_data.logger.warning("Failed to get Apple EOD Report")
        elif apple_eod.empty:
            historical_data.logger.warning("Apple EOD Report is empty")
        else:
            historical_data.logger.info("Apple EOD Report received")
            print("Apple EOD Report:")
            print(apple_eod)

    # Example 2: Get quotes for MSFT
    if run_microsoft_quotes_example:
        microsoft_quotes = historical_data.get_quotes(
            "MSFT", "20240101", "20240131", interval="3600000"
        )
        if microsoft_quotes is None:
            historical_data.logger.warning("Failed to get Microsoft Quotes")
        elif microsoft_quotes.empty:
            historical_data.logger.warning("Microsoft Quotes data is empty")
        else:
            historical_data.logger.info("Microsoft Quotes received")
            print("\nMicrosoft Quotes:")
            print(microsoft_quotes)

    # Example 3: Get OHLC for GOOGL
    if run_google_ohlc_example:
        google_ohlc = historical_data.get_ohlc(
            "GOOGL", "20240101", "20240131", interval="3600000"
        )
        if google_ohlc is None:
            historical_data.logger.warning("Failed to get Google OHLC data")
        elif google_ohlc.empty:
            historical_data.logger.warning("Google OHLC data is empty")
        else:
            historical_data.logger.info("Google OHLC data received")
            print("\nGoogle OHLC data:")
            print(google_ohlc)

    # Example 4: Get trades for TSLA
    if run_tesla_trades_example:
        tesla_trades = historical_data.get_trades("TSLA", "20240101", "20240102")
        if tesla_trades is None:
            historical_data.logger.warning("Failed to get Tesla Trades data")
        elif tesla_trades.empty:
            historical_data.logger.warning("Tesla Trades data is empty")
        else:
            historical_data.logger.info("Tesla Trades data received")
            print("\nTesla Trades data:")
            print(tesla_trades)

    # Example 5: Get trade quotes for AMZN
    if run_amazon_trade_quote_example:
        amazon_trade_quotes = historical_data.get_trade_quote(
            "AMZN", "20240101", "20240102"
        )
        if amazon_trade_quotes is None:
            historical_data.logger.warning("Failed to get Amazon Trade Quotes data")
        elif amazon_trade_quotes.empty:
            historical_data.logger.warning("Amazon Trade Quotes data is empty")
        else:
            historical_data.logger.info("Amazon Trade Quotes data received")
            print("\nAmazon Trade Quotes data:")
            print(amazon_trade_quotes)

    # Example 6: Get splits for NVDA
    if run_nvidia_splits_example:
        nvidia_splits = historical_data.get_splits("NVDA", "20230101", "20240131")
        if nvidia_splits is None:
            historical_data.logger.warning("Failed to get NVIDIA Splits data")
        elif nvidia_splits.empty:
            historical_data.logger.warning("NVIDIA Splits data is empty")
        else:
            historical_data.logger.info("NVIDIA Splits data received")
            print("\nNVIDIA Splits data:")
            print(nvidia_splits)

    # Example 7: Get dividends for INTC
    if run_intel_dividends_example:
        intel_dividends = historical_data.get_dividends("INTC", "20230101", "20240131")
        if intel_dividends is None:
            historical_data.logger.warning("Failed to get Intel Dividends data")
        elif intel_dividends.empty:
            historical_data.logger.warning("Intel Dividends data is empty")
        else:
            historical_data.logger.info("Intel Dividends data received")
            print("\nIntel Dividends data:")
            print(intel_dividends)
