import sys
import os
from functools import wraps
from datetime import datetime, timedelta

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.options import ThetaDataOptions

options_data = ThetaDataOptions(log_level="DEBUG")


def example_runner(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        description = func.__name__.replace("_", " ").title()
        result = func(*args, **kwargs)

        if result is None:
            options_data.logger.warning(f"Failed to get {description}")
        elif result.empty:
            options_data.logger.warning(f"{description} is empty")
        else:
            options_data.logger.info(f"{description} received")
            print(f"\n{description}:")
            print(result)

    return wrapper


# Calculate dates for the last 7 days
end_date = datetime.now().date()
start_date = end_date - timedelta(days=6)

# Format dates as strings
start_date_str = start_date.strftime("%Y%m%d")
end_date_str = end_date.strftime("%Y%m%d")


@example_runner
def aapl_historical_greeks_example():
    return options_data.get_historical_greeks(
        "AAPL", "20240119", 170000, "C", start_date_str, end_date_str
    )


@example_runner
def spy_historical_greeks_second_order_example():
    return options_data.get_historical_greeks_second_order(
        "SPY", "20240119", 450000, "P", start_date_str, end_date_str
    )


@example_runner
def tsla_historical_greeks_third_order_example():
    return options_data.get_historical_greeks_third_order(
        "TSLA", "20240119", 200000, "C", start_date_str, end_date_str
    )


@example_runner
def amzn_historical_trade_greeks_example():
    return options_data.get_historical_trade_greeks(
        "AMZN", "20240119", 130000, "P", start_date_str, end_date_str
    )


@example_runner
def msft_historical_trade_greeks_second_order_example():
    return options_data.get_historical_trade_greeks_second_order(
        "MSFT", "20240119", 350000, "C", start_date_str, end_date_str
    )


@example_runner
def googl_historical_trade_greeks_third_order_example():
    return options_data.get_historical_trade_greeks_third_order(
        "GOOGL", "20240119", 120000, "C", start_date_str, end_date_str
    )


@example_runner
def nvda_bulk_trade_greeks_example():
    return options_data.get_bulk_trade_greeks(
        "NVDA", "20240119", start_date_str, end_date_str
    )


# Toggle example cases
if __name__ == "__main__":
    run_examples = {
        "aapl_historical_greeks_example": True,
        "spy_historical_greeks_second_order_example": True,
        "tsla_historical_greeks_third_order_example": True,
        "amzn_historical_trade_greeks_example": True,
        "msft_historical_trade_greeks_second_order_example": True,
        "googl_historical_trade_greeks_third_order_example": True,
        "nvda_bulk_trade_greeks_example": True,
    }

    for example, should_run in run_examples.items():
        if should_run:
            globals()[example]()
