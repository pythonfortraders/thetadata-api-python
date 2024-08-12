import requests
import logging
import pandas as pd
import os


class ThetaDataStocksSnapshot:
    def __init__(self, log_level: str = "WARNING", output_dir: str = "./") -> None:
        """
        Initialize the ThetaDataStocksSnapshot class.

        Parameters:
        log_level (str): The logging level. Defaults to "WARNING".
        output_dir (str): The directory to save output files. Defaults to "./".

        This constructor sets up logging and initializes the output directory.
        """
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format="%(asctime)s - %(levelname)s - %(message)s",
        )
        self.logger = logging.getLogger(__name__)
        self.output_dir = output_dir

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
            self.logger.debug(f"Sending request to {url} with params: {params}")
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            self.logger.info("Request successful")
            return response.json()
        except requests.RequestException as e:
            self.logger.error(f"An error occurred: {e}")
            self.logger.error(
                f"Response text: {response.text if response else 'No response'}"
            )
            return None

    def _process_response(
        self, response: dict | None, write_csv: bool, datatype: str, symbol: str
    ) -> pd.DataFrame | None:
        """
        Process the API response and return a DataFrame.

        Args:
            response (dict | None): The API response or None if the request failed.
            write_csv (bool): If True, write the DataFrame to a CSV file.
            datatype (str): Type of data (e.g., 'quotes', 'ohlc', 'trades').
            symbol (str): The stock symbol.

        Returns:
            pd.DataFrame | None: DataFrame of data, or None if response is None.
        """
        if response:
            columns = response["header"]["format"]
            data = response["response"]
            df = pd.DataFrame(data, columns=columns)

            if write_csv:
                self._write_csv(df, datatype, symbol)

            return df
        else:
            return None

    def _write_csv(
        self,
        df: pd.DataFrame,
        datatype: str,
        symbol: str,
    ) -> None:
        """
        Write DataFrame to CSV file.

        Args:
            df (pd.DataFrame): DataFrame to write
            datatype (str): Type of data (e.g., 'quotes', 'ohlc', 'trades')
            symbol (str): The stock symbol
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.logger.info(f"Created output directory: {self.output_dir}")

        filename = f"{datatype}_{symbol}.csv"
        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False)
        self.logger.info(f"CSV file written: {filepath}")

    def get_quotes(
        self, symbol: str, venue: str = None, write_csv: bool = False
    ) -> pd.DataFrame | None:
        """
        Get real-time quotes for a given symbol.

        Args:
            symbol (str): The stock symbol
            venue (str, optional): The data venue. Must be either 'nqb' (NASDAQ Basic) or 'utp_cta' (merged UTP & CTA).
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of quote data, or None if request fails

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
        self.logger.info(f"Getting quotes for {symbol}")
        endpoint = "/v2/snapshot/stock/quote"
        params = {"root": symbol}
        if venue:
            if venue not in ["nqb", "utp_cta"]:
                raise ValueError("venue must be either 'nqb' or 'utp_cta'")
            params["venue"] = venue
        response = self.send_request(endpoint, params)
        return self._process_response(response, write_csv, "quotes", symbol)

    def get_bulk_quotes(
        self, symbols: list, venue: str = None, write_csv: bool = False
    ) -> pd.DataFrame | None:
        """
        Get real-time quotes for multiple symbols.

        Args:
            symbols (list): List of stock symbols
            venue (str, optional): The data venue. Must be either 'nqb' (NASDAQ Basic) or 'utp_cta' (merged UTP & CTA).
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of quote data for multiple symbols, or None if request fails

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
        self.logger.info(f"Getting bulk quotes for {symbols}")
        endpoint = "/v2/snapshot/stock/quote"
        params = {"root": ",".join(symbols)}
        if venue:
            if venue not in ["nqb", "utp_cta"]:
                raise ValueError("venue must be either 'nqb' or 'utp_cta'")
            params["venue"] = venue
        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_quotes", "_".join(symbols)
        )

    def get_ohlc(self, symbol: str, write_csv: bool = False) -> pd.DataFrame | None:
        """
        Get real-time OHLC (Open, High, Low, Close) data for a given symbol.

        Args:
            symbol (str): The stock symbol
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of OHLC data, or None if request fails

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
        self.logger.info(f"Getting OHLC for {symbol}")
        endpoint = "/v2/snapshot/stock/ohlc"
        params = {"root": symbol}
        response = self.send_request(endpoint, params)
        return self._process_response(response, write_csv, "ohlc", symbol)

    def get_bulk_ohlc(
        self, symbols: list, write_csv: bool = False
    ) -> pd.DataFrame | None:
        """
        Get real-time OHLC (Open, High, Low, Close) data for multiple symbols.

        Args:
            symbols (list): List of stock symbols
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of OHLC data for multiple symbols, or None if request fails

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
        self.logger.info(f"Getting bulk OHLC for {symbols}")
        endpoint = "/v2/snapshot/stock/ohlc"
        params = {"root": ",".join(symbols)}
        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_ohlc", "_".join(symbols)
        )

    def get_trades(self, symbol: str, write_csv: bool = False) -> pd.DataFrame | None:
        """
        Get real-time trade data for a given symbol.

        Args:
            symbol (str): The stock symbol
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of trade data, or None if request fails

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
        self.logger.info(f"Getting trades for {symbol}")
        endpoint = "/v2/snapshot/stock/trade"
        params = {"root": symbol}
        response = self.send_request(endpoint, params)
        return self._process_response(response, write_csv, "trades", symbol)
