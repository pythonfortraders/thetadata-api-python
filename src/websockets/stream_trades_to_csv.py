import asyncio
import json
from typing import Literal
from common import (
    create_subfolder_and_header,
    write_to_file,
    update_progress,
    stream_bulk_data,
)


def format_trade_data(trade, contract):
    if contract["security_type"] == "OPTION":
        return f"{trade['date']},{trade['ms_of_day']},{trade['sequence']},{trade['size']},{trade['condition']},{trade['price']},{trade['exchange']},{contract['expiration']},{contract['strike']},{contract['right']}\n"
    elif contract["security_type"] == "STOCK":
        return f"{trade['date']},{trade['ms_of_day']},{trade['sequence']},{trade['size']},{trade['condition']},{trade['price']},{trade['exchange']}\n"
    else:
        return None


async def handle_response(response: str, download_stats, last_log_time, progress_bar):
    response_data = json.loads(response)
    if response_data["header"]["type"] != "TRADE":
        return download_stats, last_log_time

    contract = response_data["contract"]
    trade = response_data["trade"]

    subfolder, header = create_subfolder_and_header(contract, "trades")
    if not subfolder:
        return download_stats, last_log_time

    data = format_trade_data(trade, contract)
    if not data:
        return download_stats, last_log_time

    file_name = f"{subfolder}/{contract['root']}.csv"
    write_to_file(file_name, header, data)

    # Update download stats
    download_stats[contract["root"]] += 1
    last_log_time = update_progress(
        "trades", download_stats, last_log_time, progress_bar
    )

    return download_stats, last_log_time


async def stream_bulk_trades(
    sec_type: Literal["OPTION", "STOCK"],
    id: int = 0,
) -> None:
    await stream_bulk_data(sec_type, "TRADE", id, handle_response)


if __name__ == "__main__":
    asyncio.run(stream_bulk_trades("STOCK", 1))
