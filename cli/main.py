"""
Example usage commands:

Stocks Historical Data:
Get end-of-day report:
   python main.py stocks historical eod-report AAPL 20240101 20240131

Get quotes:
   python main.py stocks historical quotes MSFT 20240101 20240131 --interval 3600000

Save output to a file:
   python main.py stocks historical eod-report AAPL 20240101 20240131 --output-file aapl_eod.csv
"""

import typer
import pandas as pd
import sys
import os
from functools import wraps
from rich.progress import Progress, SpinnerColumn, TextColumn

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.stocks_historical import ThetaDataStocksHistorical
from typing import Optional

app = typer.Typer()
stocks_app = typer.Typer()
options_app = typer.Typer()
app.add_typer(stocks_app, name="stocks")
app.add_typer(options_app, name="options")

historical_data = ThetaDataStocksHistorical(enable_logging=True, use_df=True)


def save_output(result: pd.DataFrame | dict | None, output_file: Optional[str]):
    if isinstance(result, pd.DataFrame):
        if output_file:
            result.to_csv(output_file, index=False)
            typer.echo(f"Data saved to {output_file}")
        else:
            typer.echo(result.to_string())
    else:
        typer.echo(result)


def with_spinner(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            transient=True,
        ) as progress:
            task = progress.add_task(description="Loading data...", total=None)
            result = func(*args, **kwargs)
            progress.update(task, completed=True)
        return result

    return wrapper


@stocks_app.command(name="eod-report")
@with_spinner
def eod_report(
    symbol: str, start_date: str, end_date: str, output_file: Optional[str] = None
):
    """Get end-of-day report for a given symbol and date range."""
    result = historical_data.get_eod_report(symbol, start_date, end_date)
    save_output(result, output_file)


@stocks_app.command(name="quotes")
@with_spinner
def quotes(
    symbol: str,
    start_date: str,
    end_date: str,
    interval: str = "900000",
    output_file: Optional[str] = None,
):
    """Get quotes for a given symbol and date range."""
    result = historical_data.get_quotes(symbol, start_date, end_date, interval)
    save_output(result, output_file)


@stocks_app.command(name="ohlc")
@with_spinner
def ohlc(
    symbol: str,
    start_date: str,
    end_date: str,
    interval: str = "900000",
    output_file: Optional[str] = None,
):
    """Get OHLC data for a given symbol and date range."""
    result = historical_data.get_ohlc(symbol, start_date, end_date, interval)
    save_output(result, output_file)


@stocks_app.command(name="trades")
@with_spinner
def trades(
    symbol: str, start_date: str, end_date: str, output_file: Optional[str] = None
):
    """Get historical trade data for a given symbol and date range."""
    result = historical_data.get_trades(symbol, start_date, end_date)
    save_output(result, output_file)


@stocks_app.command(name="trade-quote")
@with_spinner
def trade_quote(
    symbol: str, start_date: str, end_date: str, output_file: Optional[str] = None
):
    """Get historical trade and quote data for a given symbol and date range."""
    result = historical_data.get_trade_quote(symbol, start_date, end_date)
    save_output(result, output_file)


@stocks_app.command(name="splits")
@with_spinner
def splits(
    symbol: str, start_date: str, end_date: str, output_file: Optional[str] = None
):
    """Get stock split data for a given symbol and date range."""
    result = historical_data.get_splits(symbol, start_date, end_date)
    save_output(result, output_file)


@stocks_app.command(name="dividends")
@with_spinner
def dividends(
    symbol: str, start_date: str, end_date: str, output_file: Optional[str] = None
):
    """Get dividend data for a given symbol and date range."""
    result = historical_data.get_dividends(symbol, start_date, end_date)
    save_output(result, output_file)


if __name__ == "__main__":
    app()
