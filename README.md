# thetadata-api-python

A simple, easy-to-use, unofficial Python wrapper for the [ThetaData REST API](https://http-docs.thetadata.us/docs/theta-data-rest-api-v2/4g9ms9h4009k0-getting-started).

> #### Bonus: New ThetaData customers can get 30% off their first month with code **PYTHON4TRADERS**.

## Context
The original [thetadata-python](https://github.com/ThetaData-API/thetadata-python) library is deprecated and it's preferred to use the REST API directly. 

Their documentation provides Python examples for the REST API that are useful and comprehensive and you'll find these on every endpoint page ([example](https://http-docs.thetadata.us/docs/theta-data-rest-api-v2/a38vp739baoch-quote-snapshot)). This library is a simple wrapper on those examples into neat classes and functions that anyone can download and use. It provides 2 additional conveniences:

1. Integration with pandas, so data is returned to your program directly in a DataFrame.
2. A CLI (command-line-interface) wrapper for downloading data directly without writing any code yourself

## Usage

> ### Make sure [ThetaTerminal](https://http-docs.thetadata.us/docs/theta-data-rest-api-v2/4g9ms9h4009k0-getting-started#what-is-theta-terminal-and-why-do-i-need-it) is running - nothing will work without it!

This library currently provides 3 classes for core operations: 
* `ThetaDataStocksHistorical`
* `ThetaDataStocksSnapshot`
* `ThetaDataOptions`

Here's an example of using the snapshot object to get current market quotes:

```
stocks_snapshot = ThetaDataStocksSnapshot(log_level="INFO", output_dir="./output")
quotes_df = stocks_snapshot.get_quotes("AAPL")
print(quotes_df.head())
```

Several code examples are available [here](https://github.com/pythonfortraders/thetadata-api-python/tree/main/examples).

There's also a command line interface available in `cli`. You can use it as follows: 

```
(pft) ➜  cli git:(main) python thetadata_cli.py 
                                                                                                                                     
 Usage: thetadata_cli.py [OPTIONS] COMMAND [ARGS]...                                                                                 
                                                                                                                                     
╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --install-completion          Install completion for the current shell.                                                           │
│ --show-completion             Show completion for the current shell, to copy it or customize the installation.                    │
│ --help                        Show this message and exit.                                                                         │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ options                                                                                                                           │
│ stocks                                                                                                                            │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
``` 

Subcommands nest downwards naturally. For example, let's say you want to get historical OHLC data for a stock:
```
(pft) ➜  cli git:(main) python thetadata_cli.py stocks historical ohlc AAPL 20240101 20240201
⠸ Loading data...Data retrieved successfully
```
This will save the data as a local CSV named `ohlc_AAPL_20240101_20240201.csv`. Many examples of CLI usage can be found [here](https://github.com/pythonfortraders/thetadata-api-python/blob/08ec0160da2519d5a0de73d8ec29ab8dd0c8d98c/cli/thetadata_cli.py#L1-L78).

## More Resources

If you want to learn more about working with market data in Python, here are some resources for you: 
* [Free Algo Trading Academy](https://www.skool.com/algo-trading-academy-4983)
* [Free Tutorial Videos](https://www.youtube.com/@PythonforTraders)
* [Course: Financial Data Mastery](https://skool.com/pythonfortraders)
  * Learn to acquire, augment, analyze, and automate the way you work with financial data.
* [Project: Securities Master Database and ETL Pipeline](https://skool.com/pythonfortraders):
  * Build a securities database and data pipeline that you can apply and adopt for your own market data needs right away.

---
### Need help? The best way to get support is to ask a question [here](https://www.skool.com/algo-trading-academy-4983). 
