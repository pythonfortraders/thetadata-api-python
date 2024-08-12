import sys
import os
from functools import wraps

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


@example_runner
def aapl_quote_snapshot_example():
    return options_data.get_quote_snapshot(
        root="AAPL", exp="20240119", right="C", strike=170000
    )


@example_runner
def googl_bulk_quote_snapshot_example():
    return options_data.get_bulk_quotes_snapshot(root="GOOGL", exp="20240119")


@example_runner
def intc_bulk_greeks_snapshot_example():
    return options_data.get_bulk_greeks_snapshot(root="INTC", exp="20240119")


@example_runner
def meta_bulk_open_interest_snapshot_example():
    return options_data.get_bulk_open_interest_snapshot(root="META", exp="20240119")


@example_runner
def nflx_bulk_ohlc_snapshot_example():
    return options_data.get_bulk_ohlc_snapshot(root="NFLX", exp="20240119")


@example_runner
def tsla_bulk_greeks_second_order_snapshot_example():
    return options_data.get_bulk_greeks_second_order_snapshot(
        root="TSLA", exp="20240119"
    )


# Toggle example cases
if __name__ == "__main__":
    run_examples = {
        "aapl_quote_snapshot_example": True,
        "googl_bulk_quote_snapshot_example": True,
        "intc_bulk_greeks_snapshot_example": True,
        "meta_bulk_open_interest_snapshot_example": True,
        "nflx_bulk_ohlc_snapshot_example": True,
        "tsla_bulk_greeks_second_order_snapshot_example": True,
    }

    for example, should_run in run_examples.items():
        if should_run:
            globals()[example]()
