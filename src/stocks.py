from .base import ThetaDataBase
import pandas as pd


class ThetaDataStocksSnapshot(ThetaDataBase):
    def __init__(self, log_level: str = "WARNING", output_dir: str = "./") -> None:
        """
        Initialize the ThetaDataStocksSnapshot class.

        Parameters:
        log_level (str): The logging level. Defaults to "WARNING".
        output_dir (str): The directory to save output files. Defaults to "./".

        This constructor sets up logging and initializes the output directory.
        """
        super().__init__(log_level, output_dir)

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
