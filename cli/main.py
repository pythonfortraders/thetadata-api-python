"""
Example usage commands:

Stocks Historical Data:
Get end-of-day report:
   python main.py stocks historical eod-report AAPL 20240101 20240131

Get quotes:
   python main.py stocks historical quotes MSFT 20240101 20240131 --interval 3600000

Stocks Snapshot Data:
Get real-time quotes:
   python main.py stocks snapshot quotes AAPL

Get real-time OHLC:
   python main.py stocks snapshot ohlc NVDA

Get real-time trades:
   python main.py stocks snapshot trades TSLA
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
from src.stocks import ThetaDataStocksSnapshot
from typing import Optional, List

app = typer.Typer()
stocks_app = typer.Typer()
historical_app = typer.Typer()
snapshot_app = typer.Typer()
options_app = typer.Typer()
app.add_typer(stocks_app, name="stocks")
stocks_app.add_typer(historical_app, name="historical")
stocks_app.add_typer(snapshot_app, name="snapshot")
app.add_typer(options_app, name="options")

historical_data = ThetaDataStocksHistorical()
snapshot_data = ThetaDataStocksSnapshot()


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


# Historical commands
@historical_app.command(name="eod-report")
@with_spinner
def eod_report(symbol: str, start_date: str, end_date: str):
    """Get end-of-day report for a given symbol and date range."""
    result = historical_data.get_eod_report(
        symbol, start_date, end_date, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@historical_app.command(name="quotes")
@with_spinner
def historical_quotes(
    symbol: str,
    start_date: str,
    end_date: str,
    interval: str = "900000",
):
    """Get historical quotes for a given symbol and date range."""
    result = historical_data.get_quotes(
        symbol, start_date, end_date, interval, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@historical_app.command(name="ohlc")
@with_spinner
def historical_ohlc(
    symbol: str,
    start_date: str,
    end_date: str,
    interval: str = "900000",
):
    """Get historical OHLC data for a given symbol and date range."""
    result = historical_data.get_ohlc(
        symbol, start_date, end_date, interval, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@historical_app.command(name="trades")
@with_spinner
def historical_trades(symbol: str, start_date: str, end_date: str):
    """Get historical trade data for a given symbol and date range."""
    result = historical_data.get_trades(symbol, start_date, end_date, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@historical_app.command(name="trade-quote")
@with_spinner
def trade_quote(symbol: str, start_date: str, end_date: str):
    """Get historical trade and quote data for a given symbol and date range."""
    result = historical_data.get_trade_quote(
        symbol, start_date, end_date, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@historical_app.command(name="splits")
@with_spinner
def splits(symbol: str, start_date: str, end_date: str):
    """Get stock split data for a given symbol and date range."""
    result = historical_data.get_splits(symbol, start_date, end_date, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@historical_app.command(name="dividends")
@with_spinner
def dividends(symbol: str, start_date: str, end_date: str):
    """Get dividend data for a given symbol and date range."""
    result = historical_data.get_dividends(symbol, start_date, end_date, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


# Snapshot commands
@snapshot_app.command(name="quotes")
@with_spinner
def snapshot_quotes(symbol: str, venue: Optional[str] = None):
    """Get real-time quotes for a given symbol."""
    result = snapshot_data.get_quotes(symbol, venue, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@snapshot_app.command(name="bulk-quotes")
@with_spinner
def bulk_quotes(symbols: List[str], venue: Optional[str] = None):
    """Get real-time quotes for multiple symbols."""
    result = snapshot_data.get_bulk_quotes(symbols, venue, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@snapshot_app.command(name="ohlc")
@with_spinner
def snapshot_ohlc(symbol: str):
    """Get real-time OHLC data for a given symbol."""
    result = snapshot_data.get_ohlc(symbol, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@snapshot_app.command(name="bulk-ohlc")
@with_spinner
def bulk_ohlc(symbols: List[str]):
    """Get real-time OHLC data for multiple symbols."""
    result = snapshot_data.get_bulk_ohlc(symbols, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@snapshot_app.command(name="trades")
@with_spinner
def snapshot_trades(symbol: str):
    """Get real-time trade data for a given symbol."""
    result = snapshot_data.get_trades(symbol, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


if __name__ == "__main__":
    app()
