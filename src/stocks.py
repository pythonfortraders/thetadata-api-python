import requests
import logging
import pandas as pd


class ThetaDataStocksSnapshot:
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

    def get_quotes(self, symbol: str, venue: str = None) -> pd.DataFrame | dict | None:
        """
        Get real-time quotes for a given symbol.

        Args:
            symbol (str): The stock symbol
            venue (str, optional): The data venue. Must be either 'nqb' (NASDAQ Basic) or 'utp_cta' (merged UTP & CTA).

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of quote data, or None if request fails

        Output columns:
            ms_of_day: The time of the EOD report. Milliseconds since 00:00:00.000 (midnight) ET.
            bid_size: The last BBO bid size.
            bid_exchange: The last BBO bid exchange.
            bid: The last BBO bid price.
            bid_condition: The last BBO bid condition.
            ask_size: The last BBO ask size.
            ask_exchange: The last BBO ask exchange.
            ask: The last BBO ask price.
            ask_condition: The last BBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        if self.enable_logging:
            self.logger.info(f"Getting quotes for {symbol}")
        endpoint = "/v2/snapshot/stock/quote"
        params = {"root": symbol}
        if venue:
            if venue not in ["nqb", "utp_cta"]:
                raise ValueError("venue must be either 'nqb' or 'utp_cta'")
            params["venue"] = venue
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_bulk_quotes(
        self, symbols: list, venue: str = None
    ) -> pd.DataFrame | dict | None:
        """
        Get real-time quotes for multiple symbols.

        Args:
            symbols (list): List of stock symbols
            venue (str, optional): The data venue. Must be either 'nqb' (NASDAQ Basic) or 'utp_cta' (merged UTP & CTA).

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of quote data for multiple symbols, or None if request fails

        Output columns:
            ms_of_day: The time of the EOD report. Milliseconds since 00:00:00.000 (midnight) ET.
            bid_size: The last BBO bid size.
            bid_exchange: The last BBO bid exchange.
            bid: The last BBO bid price.
            bid_condition: The last BBO bid condition.
            ask_size: The last BBO ask size.
            ask_exchange: The last BBO ask exchange.
            ask: The last BBO ask price.
            ask_condition: The last BBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        """
        Get real-time quotes for multiple symbols.

        Args:
            symbols (list): List of stock symbols

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of quote data for multiple symbols, or None if request fails
        """
        if self.enable_logging:
            self.logger.info(f"Getting bulk quotes for {symbols}")
        endpoint = "/v2/snapshot/stock/quote"
        params = {"root": ",".join(symbols)}
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_ohlc(self, symbol: str) -> pd.DataFrame | dict | None:
        """
        Get real-time OHLC (Open, High, Low, Close) data for a given symbol.

        Args:
            symbol (str): The stock symbol

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of OHLC data, or None if request fails

        Output columns:
            ms_of_day: The time of the last OHLC report. Milliseconds since 00:00:00.000 (midnight) ET.
            open: The opening trade price.
            high: The highest traded price.
            low: The lowest traded price.
            close: The closing traded price.
            volume: The amount of contracts traded.
            count: The amount of trades.
            date: The date formatted as YYYYMMDD.
        """
        """
        Get real-time OHLC (Open, High, Low, Close) data for a given symbol.

        Args:
            symbol (str): The stock symbol

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of OHLC data, or None if request fails
        """
        if self.enable_logging:
            self.logger.info(f"Getting OHLC for {symbol}")
        endpoint = "/v2/snapshot/stock/ohlc"
        params = {"root": symbol}
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_bulk_ohlc(self, symbols: list) -> pd.DataFrame | dict | None:
        """
        Get real-time OHLC (Open, High, Low, Close) data for multiple symbols.

        Args:
            symbols (list): List of stock symbols

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of OHLC data for multiple symbols, or None if request fails

        Output columns:
            ms_of_day: The time of the last OHLC report. Milliseconds since 00:00:00.000 (midnight) ET.
            open: The opening trade price.
            high: The highest traded price.
            low: The lowest traded price.
            close: The closing traded price.
            volume: The amount of contracts traded.
            count: The amount of trades.
            date: The date formatted as YYYYMMDD.
        """
        if self.enable_logging:
            self.logger.info(f"Getting bulk OHLC for {symbols}")
        endpoint = "/v2/snapshot/stock/ohlc"
        params = {"root": ",".join(symbols)}
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response

    def get_trades(self, symbol: str) -> pd.DataFrame | dict | None:
        """
        Get real-time trade data for a given symbol.

        Args:
            symbol (str): The stock symbol

        Returns:
            pd.DataFrame | dict | None: DataFrame or dict of trade data, or None if request fails

        Output columns:
            ms_of_day: The exchange timestamp of the trade (the time the trade was reported). Milliseconds since 00:00:00.000 (midnight) ET.
            sequence: The exchange sequence.
            ext_condition1: Additional trade condition(s). These can be ignored for options.
            ext_condition2: Additional trade condition(s). These can be ignored for options.
            ext_condition3: Additional trade condition(s). These can be ignored for options.
            ext_condition4: Additional trade condition(s). These can be ignored for options.
            condition: The trade condition.
            size: The amount of contracts traded.
            exchange: The exchange the trade was executed.
            price: The trade price.
            condition_flags: Future use. These fields can be ignored.
            price_flags: Future use. These fields can be ignored.
            volume_type: Future use. These fields can be ignored.
            records_back: Non-zero for trade cancellations and insertions. The value represents the amount of trades prior to the current trade to delete or insert.
            date: The date formatted as YYYYMMDD.
        """
        if self.enable_logging:
            self.logger.info(f"Getting trades for {symbol}")
        endpoint = "/v2/snapshot/stock/trade"
        params = {"root": symbol}
        response = self.send_request(endpoint, params)

        if response and self.use_df:
            columns = response["header"]["format"]
            data = response["response"]
            return pd.DataFrame(data, columns=columns)
        else:
            return response
