import asyncio
import json
import os
from typing import Literal
from common import (
    create_subfolder_and_header,
    write_to_file,
    update_progress,
    stream_bulk_data,
)


def format_quote_data(quote, contract):
    if contract["security_type"] == "OPTION":
        return f"{quote['date']},{quote['ms_of_day']},{quote['bid_size']},{quote['bid_exchange']},{quote['bid']},{quote['bid_condition']},{quote['ask_size']},{quote['ask_exchange']},{quote['ask']},{quote['ask_condition']},{contract['expiration']},{contract['strike']},{contract['right']}\n"
    elif contract["security_type"] == "STOCK":
        return f"{quote['date']},{quote['ms_of_day']},{quote['bid_size']},{quote['bid_exchange']},{quote['bid']},{quote['bid_condition']},{quote['ask_size']},{quote['ask_exchange']},{quote['ask']},{quote['ask_condition']}\n"
    else:
        return None


async def handle_response(response: str, download_stats, last_log_time, progress_bar):
    response_data = json.loads(response)
    if response_data["header"]["type"] != "QUOTE":
        return download_stats, last_log_time

    contract = response_data["contract"]
    quote = response_data["quote"]

    subfolder, header = create_subfolder_and_header(contract, "quotes")
    if not subfolder:
        return download_stats, last_log_time

    data = format_quote_data(quote, contract)
    if not data:
        return download_stats, last_log_time

    file_name = os.path.join(subfolder, f"{contract['root']}.csv")
    write_to_file(file_name, header, data)

    # Update download stats
    download_stats[contract["root"]] += 1
    last_log_time = update_progress(
        "quotes", download_stats, last_log_time, progress_bar
    )

    return download_stats, last_log_time


async def stream_bulk_quotes(
    sec_type: Literal["OPTION", "STOCK"],
    id: int = 0,
) -> None:
    await stream_bulk_data(sec_type, "QUOTE", id, handle_response)


if __name__ == "__main__":
    asyncio.run(stream_bulk_quotes("STOCK", 0))
