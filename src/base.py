import logging
import os
import pandas as pd
import requests


class ThetaDataBase:
    def __init__(self, log_level: str = "WARNING", output_dir: str = "./") -> None:
        """
        Initialize the ThetaDataBase class.

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
        self, response: dict | None, write_csv: bool, datatype: str, identifier: str
    ) -> pd.DataFrame | None:
        """
        Process the API response and return a DataFrame.

        Args:
            response (dict | None): The API response or None if the request failed.
            write_csv (bool): If True, write the DataFrame to a CSV file.
            datatype (str): Type of data (e.g., 'quotes', 'ohlc', 'trades').
            identifier (str): The data identifier (e.g., symbol or option identifier).

        Returns:
            pd.DataFrame | None: DataFrame of data, or None if response is None.
        """
        if response:
            columns = response["header"]["format"]
            data = response["response"]
            df = pd.DataFrame(data, columns=columns)

            if write_csv:
                self._write_csv(df, datatype, identifier)

            return df
        else:
            return None

    def _write_csv(
        self,
        df: pd.DataFrame,
        datatype: str,
        identifier: str,
    ) -> None:
        """
        Write DataFrame to CSV file.

        Args:
            df (pd.DataFrame): DataFrame to write
            datatype (str): Type of data (e.g., 'quotes', 'ohlc', 'trades')
            identifier (str): The data identifier (e.g., symbol or option identifier)
        """
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
            self.logger.info(f"Created output directory: {self.output_dir}")

        filename = f"{datatype}_{identifier}.csv"
        filepath = os.path.join(self.output_dir, filename)
        df.to_csv(filepath, index=False)
        self.logger.info(f"CSV file written: {filepath}")
