import sys
import os

from stocks_historical_examples import example_runner

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.stocks import ThetaDataStocksSnapshot

snapshot_data = ThetaDataStocksSnapshot(log_level="INFO", output_dir="./output")


@example_runner
def apple_quotes_example():
    return snapshot_data.get_quotes("AAPL", write_csv=True)


@example_runner
def microsoft_quotes_nqb_example():
    return snapshot_data.get_quotes("MSFT", venue="nqb", write_csv=True)


@example_runner
def bulk_quotes_example():
    return snapshot_data.get_bulk_quotes(["GOOGL", "AMZN", "TSLA"], write_csv=True)


@example_runner
def nvidia_ohlc_example():
    return snapshot_data.get_ohlc("NVDA", write_csv=True)


@example_runner
def bulk_ohlc_example():
    return snapshot_data.get_bulk_ohlc(["INTC", "AMD", "QCOM"], write_csv=True)


@example_runner
def tesla_trades_example():
    return snapshot_data.get_trades("TSLA", write_csv=True)


# Toggle example cases
if __name__ == "__main__":
    run_examples = {
        "apple_quotes_example": True,
        "microsoft_quotes_nqb_example": True,
        "bulk_quotes_example": True,
        "nvidia_ohlc_example": True,
        "bulk_ohlc_example": True,
        "tesla_trades_example": True,
    }

    for example, should_run in run_examples.items():
        if should_run:
            globals()[example]()
