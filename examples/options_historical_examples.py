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
def aapl_historical_trades_example():
    return options_data.get_historical_trades(
        "AAPL", "20240119", "170000", "C", "20240101", "20240105"
    )


@example_runner
def spy_historical_trade_quote_example():
    return options_data.get_historical_trade_quote(
        "SPY", "20240119", "450000", "P", "20240101", "20240105"
    )


@example_runner
def tsla_historical_quotes_example():
    return options_data.get_historical_quotes(
        "TSLA", "20240119", "200000", "C", "20240101", "20240105", ivl=60000
    )


@example_runner
def amzn_historical_ohlc_example():
    return options_data.get_historical_ohlc(
        "AMZN", "20240119", "130000", "P", "20240101", "20240105", ivl=3600000
    )


@example_runner
def msft_historical_greeks_example():
    return options_data.get_historical_greeks(
        "MSFT", "20240119", "350000", "C", "20240101", "20240105", ivl=3600000
    )


@example_runner
def googl_historical_all_greeks_example():
    return options_data.get_historical_all_greeks(
        "GOOGL", "20240119", "140000", "P", "20240101", "20240105", ivl=3600000
    )


@example_runner
def nvda_historical_trade_greeks_example():
    return options_data.get_historical_trade_greeks(
        "NVDA", "20240119", "500000", "C", "20240101", "20240105"
    )


@example_runner
def intc_historical_trade_greeks_second_order_example():
    return options_data.get_historical_trade_greeks_second_order(
        "INTC", "20240119", "45000", "P", "20240101", "20240105"
    )


# Toggle example cases
if __name__ == "__main__":
    run_examples = {
        "aapl_historical_trades_example": True,
        "spy_historical_trade_quote_example": True,
        "tsla_historical_quotes_example": True,
        "amzn_historical_ohlc_example": True,
        "msft_historical_greeks_example": True,
        "googl_historical_all_greeks_example": True,
        "nvda_historical_trade_greeks_example": True,
        "intc_historical_trade_greeks_second_order_example": True,
    }

    for example, should_run in run_examples.items():
        if should_run:
            globals()[example]()
