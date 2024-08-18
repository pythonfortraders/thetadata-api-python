import asyncio
import json
import os
from typing import Literal
from datetime import datetime, timedelta
from collections import defaultdict
from tqdm import tqdm
import websockets

WS_URL = "ws://127.0.0.1:25520/v1/events"


def create_subfolder_and_header(contract, data_type):
    if contract["security_type"] == "OPTION":
        subfolder = os.path.join(data_type, "options")
        if data_type == "trades":
            header = "date,ms_of_day,sequence,size,condition,price,exchange,expiration,strike,right\n"
        else:  # quotes
            header = "date,ms_of_day,bid_size,bid_exchange,bid,bid_condition,ask_size,ask_exchange,ask,ask_condition,expiration,strike,right\n"
    elif contract["security_type"] == "STOCK":
        subfolder = os.path.join(data_type, "stocks")
        if data_type == "trades":
            header = "date,ms_of_day,sequence,size,condition,price,exchange\n"
        else:  # quotes
            header = "date,ms_of_day,bid_size,bid_exchange,bid,bid_condition,ask_size,ask_exchange,ask,ask_condition\n"
    else:
        return None, None  # Unsupported security type

    os.makedirs(subfolder, exist_ok=True)
    return subfolder, header


def write_to_file(file_name, header, data):
    if not os.path.exists(file_name):
        with open(file_name, "w") as output_file:
            output_file.write(header)

    with open(file_name, "a") as output_file:
        output_file.write(data)


def update_progress(data_type, download_stats, last_log_time, progress_bar):
    if progress_bar is not None:
        progress_bar.update(1)

    current_time = datetime.now()
    if current_time - last_log_time >= timedelta(seconds=60):
        print(
            f"\nDownload progress as of {current_time.strftime('%Y-%m-%d %H:%M:%S')}:"
        )
        for symbol, count in download_stats.items():
            print(f"  {symbol}: {count} {data_type}")
        print(f"Total {data_type} downloaded: {sum(download_stats.values())}")
        return current_time
    return last_log_time


async def stream_bulk_data(
    sec_type: Literal["OPTION", "STOCK"],
    req_type: Literal["TRADE", "QUOTE"],
    id: int,
    handle_response,
) -> None:
    req = {
        "msg_type": "STREAM_BULK",
        "sec_type": sec_type,
        "req_type": req_type,
        "add": True,
        "id": id,
    }

    progress_bar = tqdm(
        desc=f"Downloading {req_type.lower()}s", unit=req_type.lower(), total=None
    )
    download_stats = defaultdict(int)
    last_log_time = datetime.now()

    async with websockets.connect(WS_URL) as websocket:
        await websocket.send(json.dumps(req))
        while True:
            response = await websocket.recv()
            download_stats, last_log_time = await handle_response(
                response, download_stats, last_log_time, progress_bar
            )
