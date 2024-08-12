import sys
import os
from functools import wraps

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.stocks_historical import ThetaDataStocksHistorical

historical_data = ThetaDataStocksHistorical(log_level="DEBUG")


def example_runner(func):
    """
    A decorator that wraps functions to provide consistent logging and output formatting.

    This decorator does the following:
    1. Generates a human-readable description from the function name.
    2. Executes the wrapped function and captures its result.
    3. Logs appropriate messages based on the result:
       - Warns if the result is None (indicating a failure to retrieve data).
       - Warns if the result is an empty DataFrame.
       - Logs an info message if data was successfully received.
    4. Prints the description and the result to the console if data was received.

    Args:
        func (callable): The function to be wrapped.

    Returns:
        callable: The wrapped function with added logging and output formatting.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        description = func.__name__.replace("_", " ").title()
        result = func(*args, **kwargs)

        if result is None:
            historical_data.logger.warning(f"Failed to get {description}")
        elif result.empty:
            historical_data.logger.warning(f"{description} is empty")
        else:
            historical_data.logger.info(f"{description} received")
            print(f"\n{description}:")
            print(result)

    return wrapper


@example_runner
def apple_eod_example():
    return historical_data.get_eod_report("AAPL", "20240101", "20240131")


@example_runner
def microsoft_quotes_example():
    return historical_data.get_quotes(
        "MSFT", "20240101", "20240131", interval="3600000"
    )


@example_runner
def google_ohlc_example():
    return historical_data.get_ohlc("GOOGL", "20240101", "20240131", interval="3600000")


@example_runner
def tesla_trades_example():
    return historical_data.get_trades("TSLA", "20240101", "20240102")


@example_runner
def amazon_trade_quote_example():
    return historical_data.get_trade_quote("AMZN", "20240101", "20240102")


@example_runner
def nvidia_splits_example():
    return historical_data.get_splits("NVDA", "20230101", "20240131")


@example_runner
def intel_dividends_example():
    return historical_data.get_dividends("INTC", "20230101", "20240131")


# Toggle example cases
if __name__ == "__main__":
    run_examples = {
        "apple_eod_example": True,
        "microsoft_quotes_example": True,
        "google_ohlc_example": True,
        "tesla_trades_example": True,
        "amazon_trade_quote_example": True,
        "nvidia_splits_example": True,
        "intel_dividends_example": True,
    }

    for example, should_run in run_examples.items():
        if should_run:
            globals()[example]()
