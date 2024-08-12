import pandas as pd
import os

from .utils import is_valid_date_format
from .base import ThetaDataBase


class ThetaDataStocksHistorical(ThetaDataBase):
    def __init__(self, log_level: str = "WARNING", output_dir: str = "./") -> None:
        """
        Initialize the ThetaDataStocksHistorical class.

        Parameters:
        log_level (str): The logging level. Defaults to "WARNING".
        output_dir (str): The directory to save output files. Defaults to "./".

        This constructor sets up logging and initializes the output directory.
        """
        super().__init__(log_level, output_dir)

    def _process_response(
        self,
        response: dict | None,
        write_csv: bool,
        datatype: str,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> pd.DataFrame | None:
        """
        Process the API response and return a DataFrame.

        Args:
            response (dict | None): The API response or None if the request failed.
            write_csv (bool): If True, write the DataFrame to a CSV file.
            datatype (str): Type of data (e.g., 'eod', 'quotes', 'ohlc').
            symbol (str): The stock symbol.
            start_date (str): Start date in 'YYYYMMDD' format.
            end_date (str): End date in 'YYYYMMDD' format.

        Returns:
            pd.DataFrame | None: DataFrame of data, or None if response is None.
        """
        if response:
            columns = response["header"]["format"]
            data = response["response"]
            df = pd.DataFrame(data, columns=columns)

            if write_csv:
                self._write_csv(df, datatype, symbol, start_date, end_date)

            return df
        else:
            return None

    def _write_csv(
        self,
        df: pd.DataFrame,
        datatype: str,
        symbol: str,
        start_date: str,
        end_date: str,
    ) -> None:
        """
        Write DataFrame to CSV file.

        Args:
            df (pd.DataFrame): DataFrame to write
            datatype (str): Type of data (e.g., 'eod', 'quotes', 'ohlc')
            symbol (str): The stock symbol
            start_date (str): Start date in 'YYYYMMDD' format
            end_date (str): End date in 'YYYYMMDD' format
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.logger.info(f"Created output directory: {self.output_dir}")

        filename = f"{datatype}_{symbol}_{start_date}_{end_date}.csv"
        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False)
        self.logger.info(f"CSV file written: {filepath}")

    def get_eod_report(
        self, symbol: str, start_date: str, end_date: str, write_csv: bool = False
    ) -> pd.DataFrame | None:
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
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of EOD data, or None if request fails
        """
        # Check if dates have the correct format
        if not (is_valid_date_format(start_date) and is_valid_date_format(end_date)):
            self.logger.error(
                f"Invalid date format. Expected format: 'YYYYMMDD'. Got start_date: {start_date}, end_date: {end_date}"
            )
            return None

        self.logger.info(
            f"Getting EOD report for {symbol} from {start_date} to {end_date}"
        )
        endpoint = "/v2/hist/stock/eod"
        params = {"root": symbol, "start_date": start_date, "end_date": end_date}
        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "eod", symbol, start_date, end_date
        )

    def get_quotes(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        interval: str = "900000",
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
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
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of quote data, or None if request fails
        """
        self.logger.info(f"Getting quotes for {symbol} from {start_date} to {end_date}")
        endpoint = "/v2/hist/stock/quote"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": interval,
        }
        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "quotes", symbol, start_date, end_date
        )

    def get_ohlc(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        interval: str = "900000",
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
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
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of OHLC data, or None if request fails
        """
        self.logger.info(f"Getting OHLC for {symbol} from {start_date} to {end_date}")
        endpoint = "/v2/hist/stock/ohlc"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": interval,
        }
        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "ohlc", symbol, start_date, end_date
        )

    def get_trades(
        self, symbol: str, start_date: str, end_date: str, write_csv: bool = False
    ) -> pd.DataFrame | None:
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
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of trade data, or None if request fails
        """
        self.logger.info(f"Getting trades for {symbol} from {start_date} to {end_date}")
        endpoint = "/v2/hist/stock/trade"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
        }
        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "trades", symbol, start_date, end_date
        )

    def get_trade_quote(
        self, symbol: str, start_date: str, end_date: str, write_csv: bool = False
    ) -> pd.DataFrame | None:
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
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of trade and quote data, or None if request fails
        """
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
        return self._process_response(
            response, write_csv, "trade_quote", symbol, start_date, end_date
        )

    def get_splits(
        self, symbol: str, start_date: str, end_date: str, write_csv: bool = False
    ) -> pd.DataFrame | None:
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
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of stock split data, or None if request fails
        """
        self.logger.info(f"Getting splits for {symbol} from {start_date} to {end_date}")
        endpoint = "/v2/hist/stock/split"
        params = {
            "root": symbol,
            "start_date": start_date,
            "end_date": end_date,
        }
        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "splits", symbol, start_date, end_date
        )

    def get_dividends(
        self, symbol: str, start_date: str, end_date: str, write_csv: bool = False
    ) -> pd.DataFrame | None:
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
            write_csv (bool): If True, write the DataFrame to a CSV file

        Returns:
            pd.DataFrame | None: DataFrame of dividend data, or None if request fails
        """
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
        return self._process_response(
            response, write_csv, "dividends", symbol, start_date, end_date
        )
