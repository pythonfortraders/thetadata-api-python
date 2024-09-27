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

Options Data:
Historical:
Get historical EOD report:
   python main.py options historical eod-report AAPL 20240119 170000 C 20240101 20240131

Get historical quotes:
   python main.py options historical quotes AAPL 20240119 170000 C 20240101 20240131

Get historical trades:
   python main.py options historical trades AAPL 20240119 170000 C 20240101 20240131

Get historical trade quote:
   python main.py options historical trade-quote AAPL 20240119 170000 C 20240101 20240131

Get historical Greeks:
   python main.py options historical greeks AAPL 20240119 170000 C 20240101 20240131

Get historical third-order Greeks:
   python main.py options historical greeks-third-order AAPL 20240119 170000 C 20240101 20240131

Get historical trade Greeks:
   python main.py options historical trade-greeks AAPL 20240119 170000 C 20240101 20240131

Get historical trade Greeks third order:
   python main.py options historical trade-greeks-third-order AAPL 20240119 170000 C 20240101 20240131

Bulk:
Get bulk EOD:
   python main.py options bulk eod AAPL 20240119 20240101 20240131

Get bulk OHLC:
   python main.py options bulk ohlc AAPL 20240119 20240101 20240131

Get bulk trade:
   python main.py options bulk trade AAPL 20240119 20240101 20240131

Get bulk trade quote:
   python main.py options bulk trade-quote AAPL 20240119 20240101 20240131

Get bulk trade Greeks:
   python main.py options bulk trade-greeks AAPL 20240119 20240101 20240131

Snapshot:
Get quote snapshot:
   python main.py options snapshot quote AAPL 20240119 170000 C

Get OHLC snapshot:
   python main.py options snapshot ohlc AAPL 20240119 C 170000

Get bulk quote snapshot:
   python main.py options snapshot bulk-quote AAPL 20240119

Get bulk OHLC snapshot:
   python main.py options snapshot bulk-ohlc AAPL 20240119

Get bulk open interest snapshot:
   python main.py options snapshot bulk-open-interest AAPL 20240119
"""

import typer
import sys
import os
from functools import wraps
from rich.progress import Progress, SpinnerColumn, TextColumn

# Add the parent directory to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.stocks_historical import ThetaDataStocksHistorical
from src.stocks import ThetaDataStocksSnapshot
from src.options import ThetaDataOptions
from typing import Optional, List

app = typer.Typer(no_args_is_help=True)
stocks_app = typer.Typer(no_args_is_help=True)
historical_app = typer.Typer(no_args_is_help=True)
snapshot_app = typer.Typer(no_args_is_help=True)
options_app = typer.Typer(no_args_is_help=True)
options_historical_app = typer.Typer(no_args_is_help=True)
options_bulk_app = typer.Typer(no_args_is_help=True)
options_snapshot_app = typer.Typer(no_args_is_help=True)
app.add_typer(stocks_app, name="stocks")
stocks_app.add_typer(historical_app, name="historical")
stocks_app.add_typer(snapshot_app, name="snapshot")
app.add_typer(options_app, name="options")
options_app.add_typer(options_historical_app, name="historical")
options_app.add_typer(options_bulk_app, name="bulk")
options_app.add_typer(options_snapshot_app, name="snapshot")

historical_data = ThetaDataStocksHistorical()
snapshot_data = ThetaDataStocksSnapshot()
options_data = ThetaDataOptions()


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


# Options commands
# Historical
@options_historical_app.command(name="eod-report")
@with_spinner
def historical_eod_report(
    root: str, exp: str, strike: int, right: str, start_date: str, end_date: str
):
    """Get historical end-of-day report for a specific option contract."""
    result = options_data.get_historical_eod_report(
        root, exp, strike, right, start_date, end_date, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@options_historical_app.command(name="quotes")
@with_spinner
def historical_option_quotes(
    root: str,
    exp: str,
    strike: int,
    right: str,
    start_date: str,
    end_date: str,
    ivl: int = 0,
):
    """Get historical NBBO quotes for a specific option."""
    result = options_data.get_historical_quotes(
        root, exp, strike, right, start_date, end_date, ivl, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@options_historical_app.command(name="trades")
@with_spinner
def historical_option_trades(
    root: str, exp: str, strike: int, right: str, start_date: str, end_date: str
):
    """Get historical trades for a specific option."""
    result = options_data.get_historical_trades(
        root, exp, strike, right, start_date, end_date, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@options_historical_app.command(name="trade-quote")
@with_spinner
def historical_option_trade_quote(
    root: str, exp: str, strike: int, right: str, start_date: str, end_date: str
):
    """Get historical trade and quote data for a specific option."""
    result = options_data.get_historical_trade_quote(
        root, exp, strike, right, start_date, end_date, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@options_historical_app.command(name="greeks")
@with_spinner
def historical_greeks(
    root: str,
    exp: str,
    strike: int,
    right: str,
    start_date: str,
    end_date: str,
    ivl: int = 0,
):
    """Get historical Greeks data for a specific option."""
    result = options_data.get_historical_greeks(
        root, exp, strike, right, start_date, end_date, ivl, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@options_historical_app.command(name="greeks-third-order")
@with_spinner
def historical_greeks_third_order(
    root: str,
    exp: str,
    strike: int,
    right: str,
    start_date: str,
    end_date: str,
    ivl: int = 0,
):
    """Get historical third-order Greeks data for a specific option."""
    result = options_data.get_historical_greeks_third_order(
        root, exp, strike, right, start_date, end_date, ivl, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@options_historical_app.command(name="trade-greeks")
@with_spinner
def historical_trade_greeks(
    root: str, exp: str, strike: int, right: str, start_date: str, end_date: str
):
    """Get historical trade Greeks data for a specific option."""
    result = options_data.get_historical_trade_greeks(
        root, exp, strike, right, start_date, end_date, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@options_historical_app.command(name="trade-greeks-third-order")
@with_spinner
def historical_trade_greeks_third_order(
    root: str, exp: str, strike: int, right: str, start_date: str, end_date: str
):
    """Get historical trade Greeks third order data for a specific option."""
    result = options_data.get_historical_trade_greeks_third_order(
        root, exp, strike, right, start_date, end_date, write_csv=True
    )
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


# Bulk
@options_bulk_app.command(name="eod")
@with_spinner
def bulk_eod(root: str, exp: str, start_date: str, end_date: str):
    """Get bulk end-of-day data for options with the same root and expiration."""
    result = options_data.get_bulk_eod(root, exp, start_date, end_date, write_csv=True)
    if result is not None:
        typer.echo("Data retrieved successfully")
    else:
        typer.echo("Failed to retrieve data")


@options_app.command(name="bulk-ohlc")
@with_spinner
def bulk_option_ohlc(root: str, exp: str, start_date: str, end_date: str, ivl: int = 0):
    """Get bulk OHLC data for options with the same root and expiration."""
    options_data.get_bulk_ohlc(root, exp, start_date, end_date, ivl, write_csv=True)


if __name__ == "__main__":
    app()
