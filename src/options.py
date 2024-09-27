import pandas as pd

from .base import ThetaDataBase
from .utils import is_valid_right, is_valid_ivl


class ThetaDataOptions(ThetaDataBase):
    def __init__(self, log_level: str = "WARNING", output_dir: str = "./") -> None:
        """
        Initialize the ThetaDataOptionsSnapshot class.

        Parameters:
        log_level (str): The logging level. Defaults to "WARNING".
        output_dir (str): The directory to save output files. Defaults to "./".

        This constructor sets up logging and initializes the output directory.
        """
        super().__init__(log_level, output_dir)

    def get_quote_at_time(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int,
        rth: bool = True,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get the last NBBO quote reported by OPRA at a specified millisecond of the day.

        Args:
            root (str): The symbol of the security.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int): The interval size in milliseconds. Must be between 100 and 3600000.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of quote data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.
        """
        self.logger.info(f"Getting quote at time for {root} option")
        endpoint = "/v2/at_time/option/quote"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "option_quote", f"{root}_{exp}_{strike}_{right}"
        )

    def get_trade_at_time(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int,
        rth: bool = True,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get the last trade reported by OPRA at a specified millisecond of the day.

        Args:
            root (str): The symbol of the security.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int): The interval size in milliseconds. Must be between 100 and 3600000.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of trade data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.
        """
        self.logger.info(f"Getting trade at time for {root} option")
        endpoint = "/v2/at_time/option/trade"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "option_trade", f"{root}_{exp}_{strike}_{right}"
        )

    def get_bulk_quote_at_time(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        ivl: int,
        rth: bool = True,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get the last NBBO quote reported by OPRA at specified milliseconds of the day for all contracts
        that share the same provided root and expiration.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD. Set to '0' to retrieve data for every option that shares the same root.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int): The interval size in milliseconds. Must be between 100 and 3600000.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk quote data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Output columns:
            ms_of_day: Milliseconds since 00:00:00.000 (midnight) ET.
            bid_size: The last NBBO bid size.
            bid_exchange: The last NBBO bid exchange.
            bid: The last NBBO bid price.
            bid_condition: The last NBBO bid condition.
            ask_size: The last NBBO ask size.
            ask_exchange: The last NBBO ask exchange.
            ask: The last NBBO ask price.
            ask_condition: The last NBBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting bulk quote at time for {root} options")
        endpoint = "/v2/bulk_at_time/option/quote"

        if not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_quote", f"{root}_{exp}"
        )

    def get_bulk_trade_at_time(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        ivl: int,
        rth: bool = True,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get the last trade reported by OPRA at specified milliseconds of the day for all contracts
        that share the same provided root and expiration.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD. Set to '0' to retrieve data for every option that shares the same root.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int): The interval size in milliseconds. Must be between 100 and 3600000.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk trade data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Output columns:
            ms_of_day: Milliseconds since 00:00:00.000 (midnight) ET.
            price: The price of the trade.
            size: The size of the trade.
            exchange: The exchange where the trade occurred.
            conditions: The trade conditions.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting bulk trade at time for {root} options")
        endpoint = "/v2/bulk_at_time/option/trade"

        if not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_trade", f"{root}_{exp}"
        )

    def get_historical_eod_report(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get the historical end-of-day (EOD) report for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical EOD data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - OPRA does not provide a national EOD report for options.
            - Thetadata generates a national EOD report at 17:15 ET each day.
            - ms_of_day represents the time of day the report was generated.
            - ms_of_day2 represents the time of the last trade.
            - The quote in the response represents the last NBBO reported by OPRA at the time of report generation.
            - Quote fields (bid/ask info) may not be available prior to 2023-12-01.

        Output columns:
            ms_of_day: The time of the EOD report. Milliseconds since 00:00:00.000 (midnight) ET.
            ms_of_day2: The time of the closing trade. Milliseconds since 00:00:00.000 (midnight) ET.
            open: The opening trade price.
            high: The highest traded price.
            low: The lowest traded price.
            close: The closing traded price.
            volume: The amount of contracts traded.
            count: The amount of trades.
            bid_size: The last NBBO bid size.
            bid_exchange: The last NBBO bid exchange.
            bid: The last NBBO bid price.
            bid_condition: The last NBBO bid condition.
            ask_size: The last NBBO ask size.
            ask_exchange: The last NBBO ask exchange.
            ask: The last NBBO ask price.
            ask_condition: The last NBBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting historical EOD report for {root} option")
        endpoint = "/v2/hist/option/eod"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "historical_eod", f"{root}_{exp}_{strike}_{right}"
        )

    def get_historical_quotes(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int = 0,
        rth: bool = True,
        start_time: str = None,
        end_time: str = None,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical NBBO quotes reported by OPRA for a specific option.

        Args:
            root (str): The symbol of the security.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int, optional): The interval size in milliseconds. If 0 or omitted, provides tick-level data. Defaults to 0.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            start_time (str, optional): If specified, include all ticks on or after this time.
            end_time (str, optional): If specified, include all ticks on or before this time.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical quote data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - If ivl is specified, the quote for each interval represents the last quote at the interval's timestamp.
            - A Theta Data Options Value subscription is required to access this endpoint.

        Output columns:
            ms_of_day: The exchange timestamp or interval time of the option quote. Milliseconds since 00:00:00.000 (midnight) ET.
            bid_size: The last NBBO bid size.
            bid_exchange: The last NBBO bid exchange.
            bid: The last NBBO bid price.
            bid_condition: The last NBBO bid condition.
            ask_size: The last NBBO ask size.
            ask_exchange: The last NBBO ask exchange.
            ask: The last NBBO ask price.
            ask_condition: The last NBBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting historical quotes for {root} option")
        endpoint = "/v2/hist/option/quote"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if ivl != 0 and not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
        }

        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "historical_quotes", f"{root}_{exp}_{strike}_{right}"
        )

    def get_historical_ohlc(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int,
        rth: bool = True,
        start_time: str = None,
        end_time: str = None,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical OHLC (Open, High, Low, Close) data for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int): The interval size in milliseconds. Must be between 100 and 3600000.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            start_time (str, optional): If specified, include all ticks on or after this time.
            end_time (str, optional): If specified, include all ticks on or before this time.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical OHLC data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Value subscription is required to access this endpoint.
            - The time timestamp of the bar represents the opening time of the bar.
            - For a trade to be part of the bar: bar timestamp <= trade time < bar timestamp + ivl

        Output columns:
            ms_of_day: The opening time of the OHLC bar. Milliseconds since 00:00:00.000 (midnight) ET.
            open: The opening trade price.
            high: The highest traded price.
            low: The lowest traded price.
            close: The closing traded price.
            volume: The amount of contracts traded.
            count: The amount of trades.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting historical OHLC for {root} option")
        endpoint = "/v2/hist/option/ohlc"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
        }

        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "historical_ohlc", f"{root}_{exp}_{strike}_{right}"
        )

    def get_historical_open_interest(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical open interest data for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical open interest data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Value subscription is required to access this endpoint.
            - Open Interest is normally reported once per day by OPRA at approximately 06:30 ET.
            - A new open interest message might not be sent by OPRA if there is no open interest for the option contract.
            - The reported open interest represents the open interest at the end of the previous trading day.

        Output columns:
            ms_of_day: The time OI was reported. Milliseconds since 00:00:00.000 (midnight) ET.
            open_interest: The total amount of outstanding contracts.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting historical open interest for {root} option")
        endpoint = "/v2/hist/option/open_interest"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_open_interest",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_trades(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        start_time: str = None,
        end_time: str = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical trades reported by OPRA for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            start_time (str, optional): If specified, include all ticks on or after this time.
            end_time (str, optional): If specified, include all ticks on or before this time.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical trade data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - Trade condition mappings can be found in the Theta Data documentation.
            - Extended trade conditions are not reported by OPRA for options and can be ignored.

        Output columns:
            ms_of_day: The exchange timestamp of the trade in milliseconds since 00:00:00.000 (midnight) ET.
            sequence: The exchange sequence.
            ext_condition1: Additional trade condition(s). Can be ignored for options.
            ext_condition2: Additional trade condition(s). Can be ignored for options.
            ext_condition3: Additional trade condition(s). Can be ignored for options.
            ext_condition4: Additional trade condition(s). Can be ignored for options.
            condition: The trade condition.
            size: The amount of contracts traded.
            exchange: The exchange where the trade was executed.
            price: The price of the trade.
            condition_flags: Future use.
            price_flags: Future use.
            volume_type: Future use.
            records_back: Non-zero for trade cancellations and insertions. The value represents the amount of trades prior to the current trade to delete or insert.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting historical trades for {root} option")
        endpoint = "/v2/hist/option/trade"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "use_csv": str(use_csv).lower(),
        }

        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "historical_trades", f"{root}_{exp}_{strike}_{right}"
        )

    def get_historical_trade_quote(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        exclusive: bool = False,
        rth: bool = True,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical trade data paired with the last NBBO quote for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            exclusive (bool, optional): If True, match quotes with timestamps < trade timestamp. Defaults to False.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical trade-quote data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - Trade condition mappings can be found in the Theta Data documentation.
            - Extended trade conditions are not reported by OPRA for options and can be ignored.

        Output columns:
            ms_of_day: The exchange timestamp of the trade in milliseconds since 00:00:00.000 (midnight) ET.
            sequence: The exchange sequence.
            ext_condition1-4: Additional trade conditions (can be ignored for options).
            condition: The trade condition.
            size: The amount of contracts traded.
            exchange: The exchange where the trade was executed.
            price: The price of the trade.
            condition_flags: Future use.
            price_flags: Future use.
            volume_type: Future use.
            records_back: Non-zero for trade cancellations and insertions.
            ms_of_day2: The exchange timestamp of the quote in milliseconds since 00:00:00.000 (midnight) ET.
            bid_size: The last NBBO bid size.
            bid_exchange: The last NBBO bid exchange.
            bid: The last NBBO bid price.
            bid_condition: The last NBBO bid condition.
            ask_size: The last NBBO ask size.
            ask_exchange: The last NBBO ask exchange.
            ask: The last NBBO ask price.
            ask_condition: The last NBBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting historical trade-quote data for {root} option")
        endpoint = "/v2/hist/option/trade_quote"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "exclusive": str(exclusive).lower(),
            "rth": str(rth).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_trade_quote",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_implied_volatility(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int = 900000,
        rth: bool = True,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical implied volatilities for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int, optional): The interval size in milliseconds. Defaults to 900000 (15 minutes).
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical implied volatility data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - Requires a Theta Data Options Standard subscription.
            - The Theta Terminal must be running to make this request.

        Output columns:
            ms_of_day: The exchange timestamp or interval time of the option quote. Milliseconds since 00:00:00.000 (midnight) ET.
            bid: The national best bid price.
            bid_implied_vol: The implied volatility calculated using the bid price.
            midpoint: The midpoint calculated by averaging the bid & ask prices.
            mid_implied_vol: The implied volatility calculated using the mid price.
            ask: The national best offer price.
            ask_implied_vol: The implied volatility calculated using the ask price.
            iv_error: The average IV error for the bid / mid / ask.
            ms_of_day2: The exchange timestamp of the underlying quote used. Milliseconds since 00:00:00.000 (midnight) ET.
            underlying_price: The midpoint of the underlying at the time of the option quote.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(
            f"Getting historical implied volatility data for {root} option"
        )
        endpoint = "/v2/hist/option/implied_volatility"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_implied_volatility",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_greeks(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int = 0,
        rth: bool = True,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical Greeks data for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int, optional): The interval size in milliseconds. If 0 or omitted, provides tick-level data. Defaults to 0.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical Greeks data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - Requires a Theta Data Options Standard subscription.
            - The Theta Terminal must be running to make this request.

        Output columns:
            ms_of_day: The exchange timestamp or interval time of the option quote. Milliseconds since 00:00:00.000 (midnight) ET.
            bid: The national best bid price.
            ask: The national best ask price.
            delta: The delta.
            theta: The theta.
            vega: The vega.
            rho: The rho.
            epsilon: The epsilon.
            lambda: The lambda.
            implied_vol: The implied volatility calculated using the mid price.
            iv_error: IV Error: the value of the option calculated using the implied volatility divided by the actual value reported in the quote.
            ms_of_day2: The exchange timestamp of the underlying quote used. Milliseconds since 00:00:00.000 (midnight) ET.
            underlying_price: The midpoint of the underlying at the time of the option quote.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting historical Greeks data for {root} option")
        endpoint = "/v2/hist/option/greeks"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if ivl != 0 and not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_greeks",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_greeks_second_order(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int = 0,
        rth: bool = True,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical second-order Greeks data for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int, optional): The interval size in milliseconds. If 0 or omitted, provides tick-level data. Defaults to 0.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical second-order Greeks data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - Requires a Theta Data Options Pro subscription.
            - The Theta Terminal must be running to make this request.

        Output columns:
            ms_of_day: The exchange timestamp of the option quote used. Milliseconds since 00:00:00.000 (midnight) ET.
            bid: The national best bid price.
            ask: The national best ask price.
            gamma: The gamma.
            vanna: The vanna.
            charm: The charm.
            vomma: The vomma.
            veta: The veta.
            implied_vol: The implied volatility calculated using the mid price.
            iv_error: IV Error: the value of the option calculated using the implied volatility divided by the actual value reported in the quote.
            ms_of_day2: The exchange timestamp of the underlying quote used. Milliseconds since 00:00:00.000 (midnight) ET.
            underlying_price: The midpoint of the underlying at the time of the option quote.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(
            f"Getting historical second-order Greeks data for {root} option"
        )
        endpoint = "/v2/hist/option/greeks_second_order"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if ivl != 0 and not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_greeks_second_order",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_greeks_third_order(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int = 0,
        rth: bool = True,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical third-order Greeks data for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int, optional): The interval size in milliseconds. If 0 or omitted, provides tick-level data. Defaults to 0.
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical third-order Greeks data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - Requires a Theta Data Options Pro subscription.
            - The Theta Terminal must be running to make this request.

        Output columns:
            ms_of_day: The exchange timestamp of the option quote used. Milliseconds since 00:00:00.000 (midnight) ET.
            bid: The national best bid price.
            ask: The national best ask price.
            speed: The speed.
            zomma: The zomma.
            color: The color.
            ultima: The ultima.
            implied_vol: The implied volatility calculated using the mid price.
            iv_error: IV Error: the value of the option calculated using the implied volatility divided by the actual value reported in the quote.
            ms_of_day2: The exchange timestamp of the underlying quote used. Milliseconds since 00:00:00.000 (midnight) ET.
            underlying_price: The midpoint of the underlying at the time of the option quote.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(
            f"Getting historical third-order Greeks data for {root} option"
        )
        endpoint = "/v2/hist/option/greeks_third_order"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if ivl != 0 and not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_greeks_third_order",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_all_greeks(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        ivl: int = 900000,
        rth: bool = True,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical data for all Greeks for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int, optional): The interval size in milliseconds. Defaults to 900000 (15 minutes).
            rth (bool, optional): If False, include data outside regular trading hours. Defaults to True.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical all Greeks data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - Requires a Theta Data Options Pro subscription.
            - The Theta Terminal must be running to make this request.
            - Greeks are calculated using the option and underlying midpoint price.
            - For more information on how Thetadata calculates Greeks, refer to their documentation.

        Output columns:
            [List of output columns would be here, but they are not provided in the original docstring]
        """
        self.logger.info(f"Getting historical all Greeks data for {root} option")
        endpoint = "/v2/hist/option/all_greeks"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        if ivl != 0 and not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "rth": str(rth).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_all_greeks",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_trade_greeks(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        perf_boost: bool = False,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical trade Greeks data for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            perf_boost (bool, optional): If True, use 1-second intervals for underlying quotes instead of tick-level. Defaults to False.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical trade Greeks data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - Requires a Theta Data Options Pro subscription.
            - The Theta Terminal must be running to make this request.
            - Calculates Greeks for every trade reported by OPRA.
            - The underlying price represents the last price at the ms_of_day field.

        Output columns:
            ms_of_day: The exchange timestamp of the trade Greeks (milliseconds since midnight ET).
            sequence: The exchange sequence.
            ext_condition1-4: Additional trade conditions (can be ignored for options).
            condition: The trade condition.
            size: The amount of contracts traded.
            exchange: The exchange where the trade was executed.
            price: The price of the trade.
            condition_flags: Future use.
            price_flags: Future use.
            volume_type: Future use.
            records_back: Non-zero for trade cancellations and insertions.
            delta: The delta.
            theta: The theta.
            vega: The vega.
            rho: The rho.
            epsilon: The epsilon.
            lamba: The lambda.
            implied_vol: The implied volatility calculated using the trade price.
            iv_error: IV Error (option value using implied volatility / actual reported value).
            ms_of_day2: The exchange timestamp of the underlying quote used.
            underlying_price: The midpoint of the underlying at the time of the option trade.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting historical trade Greeks data for {root} option")
        endpoint = "/v2/hist/option/trade_greeks"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "perf_boost": str(perf_boost).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_trade_greeks",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_trade_greeks_second_order(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        perf_boost: bool = False,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical trade Greeks second order data for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            perf_boost (bool, optional): If True, use 1 second intervals for underlying quotes instead of tick-level. Defaults to False.
            use_csv (bool, optional): If True, request CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical trade Greeks second order data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Pro subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.

        Output columns:
            ms_of_day: The exchange timestamp of the option quote used (milliseconds since midnight ET).
            sequence: The exchange sequence.
            ext_condition1-4: Additional trade conditions (can be ignored for options).
            condition: The trade condition.
            size: The amount of contracts traded.
            exchange: The exchange where the trade was executed.
            price: The price of the trade.
            condition_flags: Future use.
            price_flags: Future use.
            volume_type: Future use.
            records_back: Non-zero for trade cancellations and insertions.
            gamma: The gamma.
            vanna: The vanna.
            charm: The charm.
            vomma: The vomma.
            veta: The veta.
            implied_vol: The implied volatility calculated using the trade price.
            iv_error: IV Error (option value using implied volatility / actual reported value).
            ms_of_day2: The exchange timestamp of the underlying quote used.
            underlying_price: The midpoint of the underlying at the time of the option trade.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(
            f"Getting historical trade Greeks second order data for {root} option"
        )
        endpoint = "/v2/hist/option/trade_greeks_second_order"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "perf_boost": str(perf_boost).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_trade_greeks_second_order",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_historical_trade_greeks_third_order(
        self,
        root: str,
        exp: str,
        strike: int,
        right: str,
        start_date: str,
        end_date: str,
        perf_boost: bool = False,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get historical trade Greeks third order data for a specific option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            right (str): The right of the option. 'C' for call; 'P' for put.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            perf_boost (bool, optional): If True, use 1 second intervals for underlying quotes instead of tick-level quotes. Defaults to False.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of historical trade Greeks third order data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Pro subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.

        Output columns:
            ms_of_day: The exchange timestamp of the option quote used (milliseconds since midnight ET).
            sequence: The exchange sequence.
            ext_condition1-4: Additional trade conditions (can be ignored for options).
            condition: The trade condition.
            size: The amount of contracts traded.
            exchange: The exchange where the trade was executed.
            price: The price of the trade.
            condition_flags: Future use.
            price_flags: Future use.
            volume_type: Future use.
            records_back: Non-zero for trade cancellations and insertions.
            speed: The speed.
            zomma: The zomma.
            color: The color.
            ultima: The ultima.
            implied_vol: The implied volatility calculated using the trade price.
            iv_error: IV Error (option value using implied volatility / actual reported value).
            ms_of_day2: The exchange timestamp of the underlying quote used.
            underlying_price: The midpoint of the underlying at the time of the option trade.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(
            f"Getting historical trade Greeks third order data for {root} option"
        )
        endpoint = "/v2/hist/option/trade_greeks_third_order"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "strike": strike,
            "right": right,
            "start_date": start_date,
            "end_date": end_date,
            "perf_boost": str(perf_boost).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "historical_trade_greeks_third_order",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_bulk_eod(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        annual_div: float = 0,
        rate: str = "SOFR",
        rate_value: float = None,
        under_price: float = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get the historical end-of-day (EOD) report for all contracts that share the same provided root and expiration.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD. Set to '0' to retrieve data for every option that shares the same root.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            annual_div (float, optional): The annualized expected dividend amount to be used in Greeks calculations. Defaults to 0.
            rate (str, optional): The interest rate type to be used in Greeks calculations. Defaults to "SOFR".
            rate_value (float, optional): The annualized interest rate value to be used in Greeks calculations. Overrides 'rate' if specified.
            under_price (float, optional): The underlying price to be used in the Greeks calculation for EOD data.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk EOD data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Value subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - Thetadata generates a national EOD report at 17:15 ET each day.
            - Quote fields (bid/ask info) may not be available prior to 2023-12-01.

        Output columns:
            ms_of_day: The time of the EOD report. Milliseconds since 00:00:00.000 (midnight) ET.
            ms_of_day2: The time of the closing trade. Milliseconds since 00:00:00.000 (midnight) ET.
            open: The opening trade price.
            high: The highest traded price.
            low: The lowest traded price.
            close: The closing traded price.
            volume: The amount of contracts traded.
            count: The amount of trades.
            bid_size: The last NBBO bid size.
            bid_exchange: The last NBBO bid exchange.
            bid: The last NBBO bid price.
            bid_condition: The last NBBO bid condition.
            ask_size: The last NBBO ask size.
            ask_exchange: The last NBBO ask exchange.
            ask: The last NBBO ask price.
            ask_condition: The last NBBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting bulk EOD data for {root} options")
        endpoint = "/v2/bulk_hist/option/eod"

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "annual_div": annual_div,
            "rate": rate,
            "use_csv": str(use_csv).lower(),
        }

        if rate_value is not None:
            params["rate_value"] = rate_value
        if under_price is not None:
            params["under_price"] = under_price

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_eod", f"{root}_{exp}"
        )

    def get_bulk_quote(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        ivl: int,
        start_time: str = None,
        end_time: str = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get bulk quote data for all contracts with the same root and expiration.

        Args:
            root (str): The symbol of the security.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int): The interval size in milliseconds. Must be between 100 and 3600000.
            start_time (str, optional): If specified, include all ticks on or after this time.
            end_time (str, optional): If specified, include all ticks on or before this time.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk quote data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Value subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - We do not recommend omitting the ivl parameter or using a date range over 1 day.
            - Set exp to '0' to retrieve data for every option that shares the same root (only supported for 1 minute intervals, ivl=60000).

        Output columns:
            ms_of_day: Milliseconds since 00:00:00.000 (midnight) ET.
            bid_size: The last NBBO bid size.
            bid_exchange: The last NBBO bid exchange.
            bid: The last NBBO bid price.
            bid_condition: The last NBBO bid condition.
            ask_size: The last NBBO ask size.
            ask_exchange: The last NBBO ask exchange.
            ask: The last NBBO ask price.
            ask_condition: The last NBBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting bulk quote data for {root} options")
        endpoint = "/v2/bulk_hist/option/quote"

        if not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "use_csv": str(use_csv).lower(),
        }

        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_quote", f"{root}_{exp}"
        )

    def get_bulk_ohlc(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        ivl: int,
        start_time: str = None,
        end_time: str = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get bulk OHLC (Open, High, Low, Close) data for all option contracts that share the same root and expiration.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int): The interval size in milliseconds. Must be between 100 and 3600000.
            start_time (str, optional): If specified, include all ticks on or after this time.
            end_time (str, optional): If specified, include all ticks on or before this time.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk OHLC data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Value subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - Omitting ivl or setting it to 0 will provide tick-level data instead of aggregated data.

        Output columns:
            ms_of_day: The opening time of the OHLC bar. Milliseconds since 00:00:00.000 (midnight) ET.
            open: The opening trade price.
            high: The highest traded price.
            low: The lowest traded price.
            close: The closing traded price.
            volume: The amount of contracts traded.
            count: The amount of trades.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting bulk OHLC data for {root} options")
        endpoint = "/v2/bulk_hist/option/ohlc"

        if not is_valid_ivl(ivl):
            raise ValueError("ivl must be between 100 and 3600000")

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "ivl": ivl,
            "use_csv": str(use_csv).lower(),
        }

        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_ohlc", f"{root}_{exp}"
        )

    def get_bulk_open_interest(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get bulk open interest data for all contracts that share the same provided root and expiration.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD. Set to '0' to retrieve data for every option that shares the same root.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk open interest data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - Open Interest is reported by OPRA at approximately 06:30 ET.
            - A new open interest message might not be sent by OPRA if there is no open interest for the option contract.
            - The reported open interest represents the open interest at the end of the previous trading day.

        Output columns:
            ms_of_day: The time OI was reported. Milliseconds since 00:00:00.000 (midnight) ET.
            open_interest: The total amount of outstanding contracts.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting bulk open interest data for {root} options")
        endpoint = "/v2/bulk_hist/option/open_interest"

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_open_interest", f"{root}_{exp}"
        )

    def get_bulk_trade(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        ivl: int = None,
        start_time: str = None,
        end_time: str = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get bulk trade data for all contracts with the same root and expiration.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD. Set to '0' to retrieve data for every option that shares the same root.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            ivl (int, optional): The interval size in milliseconds. Must be between 100 and 3600000.
            start_time (str, optional): If specified, include all ticks on or after this time.
            end_time (str, optional): If specified, include all ticks on or before this time.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk trade data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - Returns every trade reported by OPRA for all contracts that share the same provided root and expiration.
            - Trade condition mappings can be found in the Theta Data documentation.
            - Extended trade conditions are not reported by OPRA for options, so they can be ignored.

        Output columns:
            ms_of_day: Milliseconds since 00:00:00.000 (midnight) ET.
            price: The price of the trade.
            size: The size of the trade.
            exchange: The exchange where the trade occurred.
            conditions: The trade conditions.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting bulk trade data for {root} options")
        endpoint = "/v2/bulk_hist/option/trade"

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "use_csv": str(use_csv).lower(),
        }

        if ivl is not None:
            if not is_valid_ivl(ivl):
                raise ValueError("ivl must be between 100 and 3600000")
            params["ivl"] = ivl

        if start_time:
            params["start_time"] = start_time
        if end_time:
            params["end_time"] = end_time

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_trade", f"{root}_{exp}"
        )

    def get_bulk_trade_quote(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        exclusive: bool = False,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get bulk trade and quote data for all contracts with the same root and expiration.

        Args:
            root (str): The symbol of the security.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            exclusive (bool, optional): If True, match quotes with timestamps < trade timestamp. Defaults to False.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk trade and quote data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - Returns every trade reported by OPRA paired with the last NBBO quote at the time of trade.
            - Trade condition mappings can be found in the Theta Data documentation.

        Output columns:
            ms_of_day: The exchange timestamp of the trade (milliseconds since midnight ET).
            sequence: The exchange sequence.
            ext_condition1-4: Additional trade conditions (can be ignored for options).
            condition: The trade condition.
            size: The amount of contracts traded.
            exchange: The exchange where the trade was executed.
            price: The price of the trade.
            condition_flags: Future use.
            price_flags: Future use.
            volume_type: Future use.
            records_back: Non-zero for trade cancellations and insertions.
            ms_of_day2: The exchange timestamp of the quote (milliseconds since midnight ET).
            bid_size: The last NBBO bid size.
            bid_exchange: The last NBBO bid exchange.
            bid: The last NBBO bid price.
            bid_condition: The last NBBO bid condition.
            ask_size: The last NBBO ask size.
            ask_exchange: The last NBBO ask exchange.
            ask: The last NBBO ask price.
            ask_condition: The last NBBO ask condition.
            date: The date formatted as YYYYMMDD.
        """
        self.logger.info(f"Getting bulk trade and quote data for {root} options")
        endpoint = "/v2/bulk_hist/option/trade_quote"

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "exclusive": str(exclusive).lower(),
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_trade_quote", f"{root}_{exp}"
        )

    def get_bulk_eod_greeks(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        annual_div: float = None,
        rate: str = None,
        rate_value: float = None,
        under_price: float = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get bulk EOD Greeks data for all contracts with the same root and expiration.

        Args:
            root (str): The symbol of the security.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD. Use '0' for all expirations.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            annual_div (float, optional): The annualized expected dividend amount for Greeks calculations.
            rate (str, optional): The interest rate type for Greeks calculations. Defaults to SOFR if omitted.
            rate_value (float, optional): The annualized interest rate value for Greeks calculations. Overrides 'rate' if specified.
            under_price (float, optional): The underlying price to be used in the Greeks calculation.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk EOD Greeks data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - Data is based on Theta Data's EOD reports generated at 17:15 ET each day.
            - Quote fields (bid/ask info) may not be available prior to 2023-12-01.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting bulk EOD Greeks data for {root} options")
        endpoint = "/v2/bulk_hist/option/eod_greeks"

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "use_csv": str(use_csv).lower(),
        }

        if annual_div is not None:
            params["annual_div"] = annual_div
        if rate is not None:
            params["rate"] = rate
        if rate_value is not None:
            params["rate_value"] = rate_value
        if under_price is not None:
            params["under_price"] = under_price

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_eod_greeks", f"{root}_{exp}"
        )

    def get_bulk_trade_greeks(
        self,
        root: str,
        exp: str,
        start_date: str,
        end_date: str,
        annual_div: float = None,
        rate: str = None,
        rate_value: float = None,
        use_csv: bool = False,
        perf_boost: bool = False,
        under_price: float = None,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Get bulk trade Greeks data for all contracts with the same root and expiration.

        Args:
            root (str): The symbol of the security.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD.
            start_date (str): The start date (inclusive) of the request formatted as YYYYMMDD.
            end_date (str): The end date (inclusive) of the request formatted as YYYYMMDD.
            annual_div (float, optional): The annualized expected dividend amount for Greeks calculations.
            rate (str, optional): The interest rate type for Greeks calculations. Defaults to SOFR if omitted.
            rate_value (float, optional): The annualized interest rate value for Greeks calculations. Overrides 'rate' if specified.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            perf_boost (bool, optional): If True, use 1-second intervals for underlying equity prices. Defaults to False.
            under_price (float, optional): The underlying price to be used in the Greeks calculation.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk trade Greeks data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Pro subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - Calculates Greeks for every trade reported by OPRA.
            - The underlying price represents the last underlying price at the ms_of_day field.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting bulk trade Greeks data for {root} options")
        endpoint = "/v2/bulk_hist/option/trade_greeks"

        params = {
            "root": root,
            "exp": exp,
            "start_date": start_date,
            "end_date": end_date,
            "use_csv": str(use_csv).lower(),
            "perf_boost": str(perf_boost).lower(),
        }

        if annual_div is not None:
            params["annual_div"] = annual_div
        if rate is not None:
            params["rate"] = rate
        if rate_value is not None:
            params["rate_value"] = rate_value
        if under_price is not None:
            params["under_price"] = under_price

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_trade_greeks", f"{root}_{exp}"
        )

    def get_quote_snapshot(
        self,
        root: str,
        exp: str,
        right: str,
        strike: int,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve a real-time last NBBO quote of an option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            right (str): The right of the option. 'C' for call; 'P' for put.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of quote snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Value subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting quote snapshot for {root} option")
        endpoint = "/v2/snapshot/option/quote"

        if not is_valid_right(right):
            raise ValueError(f"right must be either 'C' or 'P', got {right}")

        params = {
            "root": root,
            "exp": exp,
            "right": right,
            "strike": strike,
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "option_quote_snapshot",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_ohlc_snapshot(
        self,
        root: str,
        exp: str,
        right: str,
        strike: int,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve a real-time last OHLC (Open, High, Low, Close) of an option contract for the trading day.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            right (str): The right of the option. 'C' for call; 'P' for put.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of OHLC snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Value subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting OHLC snapshot for {root} option")
        endpoint = "/v2/snapshot/option/ohlc"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "right": right,
            "strike": strike,
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "option_ohlc_snapshot",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_trade_snapshot(
        self,
        root: str,
        exp: str,
        right: str,
        strike: int,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve the real-time last trade of an option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            right (str): The right of the option. 'C' for call; 'P' for put.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of trade snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting trade snapshot for {root} option")
        endpoint = "/v2/snapshot/option/trade"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "right": right,
            "strike": strike,
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "option_trade_snapshot",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_trade_open_interest_snapshot(
        self,
        root: str,
        exp: str,
        right: str,
        strike: int,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve the real-time open interest of an option contract.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contract formatted as YYYYMMDD.
            right (str): The right of the option. 'C' for call; 'P' for put.
            strike (int): The strike price in 1/10ths of a cent. A $170.00 strike price would be 170000.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of open interest snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Value subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - Open interest is reported around 06:30 ET every morning by OPRA and reflects the open interest at the beginning of the trading day.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting open interest snapshot for {root} option")
        endpoint = "/v2/snapshot/option/open_interest"

        if not is_valid_right(right):
            raise ValueError("right must be either 'C' or 'P'")

        params = {
            "root": root,
            "exp": exp,
            "right": right,
            "strike": strike,
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "option_open_interest_snapshot",
            f"{root}_{exp}_{strike}_{right}",
        )

    def get_bulk_quotes_snapshot(
        self,
        root: str,
        exp: str,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve a real-time last quote for all option contracts that share the same expiration and root.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD. Set to '0' to retrieve data for every option that shares the same root.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk quotes snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting bulk quotes snapshot for {root} options")
        endpoint = "/v2/bulk_snapshot/option/quote"

        params = {
            "root": root,
            "exp": exp,
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_quotes_snapshot", f"{root}_{exp}"
        )

    def get_bulk_open_interest_snapshot(
        self,
        root: str,
        exp: str,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve the last OPRA reported open interest message for all option contracts that share the same expiration and root.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD. Set to '0' to retrieve data for every expiration chain for the underlying.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk open interest snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting bulk open interest snapshot for {root} options")
        endpoint = "/v2/bulk_snapshot/option/open_interest"

        params = {
            "root": root,
            "exp": exp,
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_open_interest_snapshot", f"{root}_{exp}"
        )

    def get_bulk_ohlc_snapshot(
        self,
        root: str,
        exp: str,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve a real-time last SIP corrected session OHLC for all option contracts that share the same expiration and root.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD. Set to '0' to retrieve data for every expiration chain for the underlying.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk OHLC snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting bulk OHLC snapshot for {root} options")
        endpoint = "/v2/bulk_snapshot/option/ohlc"

        params = {
            "root": root,
            "exp": exp,
            "use_csv": str(use_csv).lower(),
        }

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_ohlc_snapshot", f"{root}_{exp}"
        )

    def get_bulk_greeks_snapshot(
        self,
        root: str,
        exp: str,
        annual_div: float = None,
        rate: str = None,
        rate_value: float = None,
        under_price: float = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve a real-time last Greeks calculation for all option contracts that share the same expiration and root.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD. Set to '0' to retrieve data for every expiration chain for the underlying.
            annual_div (float, optional): The annualized expected dividend amount to be used in Greeks calculations. Must be >= 0.
            rate (str, optional): The interest rate type to be used in a Greeks calculation. Defaults to SOFR or 0 if no rate exists for the date in question.
            rate_value (float, optional): The annualized interest rate value to be used in a Greeks calculation. A 3.42% interest rate would be represented as 0.0342. This will override the rate parameter if specified.
            under_price (float, optional): The underlying price to be used in the Greeks calculation.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk Greeks snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Standard subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting bulk Greeks snapshot for {root} options")
        endpoint = "/v2/bulk_snapshot/option/greeks"

        params = {
            "root": root,
            "exp": exp,
            "use_csv": str(use_csv).lower(),
        }

        if annual_div is not None:
            params["annual_div"] = annual_div
        if rate is not None:
            params["rate"] = rate
        if rate_value is not None:
            params["rate_value"] = rate_value
        if under_price is not None:
            params["under_price"] = under_price

        response = self.send_request(endpoint, params)
        return self._process_response(
            response, write_csv, "bulk_option_greeks_snapshot", f"{root}_{exp}"
        )

    def get_bulk_greeks_second_order_snapshot(
        self,
        root: str,
        exp: str,
        annual_div: float | None = None,
        rate: str | None = None,
        rate_value: float | None = None,
        under_price: float | None = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve a real-time last second order Greeks calculation for all option contracts that share the same expiration and root.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD. Set to '0' to retrieve data for every expiration chain for the underlying.
            annual_div (float, optional): The annualized expected dividend amount to be used in Greeks calculations. Must be >= 0.
            rate (str, optional): The interest rate type to be used in a Greeks calculation. Defaults to SOFR or 0 if no rate exists for the date in question.
            rate_value (float, optional): The annualized interest rate value to be used in a Greeks calculation. A 3.42% interest rate would be represented as 0.0342. This will override the rate parameter if specified.
            under_price (float, optional): The underlying price to be used in the Greeks calculation.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk second order Greeks snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Pro subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(
            f"Getting bulk second order Greeks snapshot for {root} options"
        )
        endpoint = "/v2/bulk_snapshot/option/greeks_second_order"

        params = {
            "root": root,
            "exp": exp,
            "use_csv": str(use_csv).lower(),
        }

        if annual_div is not None:
            params["annual_div"] = annual_div
        if rate is not None:
            params["rate"] = rate
        if rate_value is not None:
            params["rate_value"] = rate_value
        if under_price is not None:
            params["under_price"] = under_price

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "bulk_option_greeks_second_order_snapshot",
            f"{root}_{exp}",
        )

    def get_bulk_greeks_third_order_snapshot(
        self,
        root: str,
        exp: str,
        annual_div: float = None,
        rate: str = None,
        rate_value: float = None,
        under_price: float = None,
        use_csv: bool = False,
        write_csv: bool = False,
    ) -> pd.DataFrame | None:
        """
        Retrieve a real-time last third order Greeks calculation for all option contracts that share the same expiration and root.

        Args:
            root (str): The symbol of the security. Option underlyings for indices might have special tickers.
            exp (str): The expiration date of the option contracts formatted as YYYYMMDD. Set to '0' to retrieve data for every expiration chain for the underlying.
            annual_div (float, optional): The annualized expected dividend amount to be used in Greeks calculations. Must be >= 0.
            rate (str, optional): The interest rate type to be used in a Greeks calculation. Defaults to SOFR or 0 if no rate exists for the date in question.
            rate_value (float, optional): The annualized interest rate value to be used in a Greeks calculation. A 3.42% interest rate would be represented as 0.0342. This will override the rate parameter if specified.
            under_price (float, optional): The underlying price to be used in the Greeks calculation.
            use_csv (bool, optional): If True, request a CSV response instead of JSON. Defaults to False.
            write_csv (bool, optional): If True, write the DataFrame to a CSV file. Defaults to False.

        Returns:
            pd.DataFrame | None: DataFrame of bulk third order Greeks snapshot data, or None if request fails

        Raises:
            ValueError: If invalid parameters are provided.

        Note:
            - A Theta Data Options Pro subscription is required to access this endpoint.
            - The Theta Terminal must be running to make this request.
            - This endpoint will return no data if the market was closed for the day.
            - Theta Data resets the snapshot cache at midnight ET every night.

        Output columns:
            [List of output columns would go here, based on the actual API response]
        """
        self.logger.info(f"Getting bulk third order Greeks snapshot for {root} options")
        endpoint = "/v2/bulk_snapshot/option/greeks_third_order"

        params = {
            "root": root,
            "exp": exp,
            "use_csv": str(use_csv).lower(),
        }

        if annual_div is not None:
            params["annual_div"] = annual_div
        if rate is not None:
            params["rate"] = rate
        if rate_value is not None:
            params["rate_value"] = rate_value
        if under_price is not None:
            params["under_price"] = under_price

        response = self.send_request(endpoint, params)
        return self._process_response(
            response,
            write_csv,
            "bulk_option_greeks_third_order_snapshot",
            f"{root}_{exp}",
        )
