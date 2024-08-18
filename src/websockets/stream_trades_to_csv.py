import asyncio
import json
import os
from typing import Literal
from datetime import datetime, timedelta
from collections import defaultdict
from tqdm import tqdm
import websockets

WS_URL = "ws://127.0.0.1:25520/v1/events"

# Global variables for tracking progress
download_stats = defaultdict(int)
last_log_time = datetime.now()
progress_bar = None


def create_subfolder_and_header(contract):
    if contract["security_type"] == "OPTION":
        subfolder = os.path.join("trades", "options")
        header = "date,ms_of_day,sequence,size,condition,price,exchange,expiration,strike,right\n"
    elif contract["security_type"] == "STOCK":
        subfolder = os.path.join("trades", "stocks")
        header = "date,ms_of_day,sequence,size,condition,price,exchange\n"
    else:
        return None, None  # Unsupported security type

    os.makedirs(subfolder, exist_ok=True)
    return subfolder, header


def format_trade_data(trade, contract):
    if contract["security_type"] == "OPTION":
        return f"{trade['date']},{trade['ms_of_day']},{trade['sequence']},{trade['size']},{trade['condition']},{trade['price']},{trade['exchange']},{contract['expiration']},{contract['strike']},{contract['right']}\n"
    elif contract["security_type"] == "STOCK":
        return f"{trade['date']},{trade['ms_of_day']},{trade['sequence']},{trade['size']},{trade['condition']},{trade['price']},{trade['exchange']}\n"
    else:
        return None


def write_to_file(file_name, header, data):
    if not os.path.exists(file_name):
        with open(file_name, "w") as output_file:
            output_file.write(header)

    with open(file_name, "a") as output_file:
        output_file.write(data)


def update_progress():
    global download_stats, last_log_time, progress_bar

    if progress_bar:
        progress_bar.update(1)

    current_time = datetime.now()
    if current_time - last_log_time >= timedelta(seconds=60):
        print(
            f"\nDownload progress as of {current_time.strftime('%Y-%m-%d %H:%M:%S')}:"
        )
        for symbol, count in download_stats.items():
            print(f"  {symbol}: {count} trades")
        print(f"Total trades downloaded: {sum(download_stats.values())}")
        last_log_time = current_time


async def handle_response(response: str) -> None:
    global download_stats

    response_data = json.loads(response)
    if response_data["header"]["type"] != "TRADE":
        return

    contract = response_data["contract"]
    trade = response_data["trade"]

    subfolder, header = create_subfolder_and_header(contract)
    if not subfolder:
        return

    data = format_trade_data(trade, contract)
    if not data:
        return

    file_name = os.path.join(subfolder, f"{contract['root']}.csv")
    write_to_file(file_name, header, data)

    # Update download stats
    download_stats[contract["root"]] += 1
    update_progress()


async def stream_bulk_trades(
    sec_type: Literal["OPTION", "STOCK"],
    req_type: Literal["TRADE", "QUOTE"],
    id: int = 0,
) -> None:
    global progress_bar

    req = {
        "msg_type": "STREAM_BULK",
        "sec_type": sec_type,
        "req_type": req_type,
        "add": True,
        "id": id,
    }

    progress_bar = tqdm(desc="Downloading trades", unit="trade")

    async with websockets.connect(WS_URL) as websocket:
        await websocket.send(json.dumps(req))
        while True:
            response = await websocket.recv()
            await handle_response(response)


if __name__ == "__main__":
    asyncio.run(stream_bulk_trades("STOCK", "TRADE", 0))
