import asyncio
import csv
import os
import time
from bittensor.core.async_subtensor import AsyncSubtensor
from dotenv import load_dotenv
import os

load_dotenv()
WS_URI = os.getenv("WS_URI", "wss://archive.chain.opentensor.ai:443")
BATCH_SIZE = 2 ^ 4  # max concurrent fetches
OUTPUT_DIR = "backtest/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def format_seconds(seconds: float) -> str:
    """Format elapsed seconds into H:MM:SS.ss"""
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return f"{int(h)}:{int(m):02d}:{s:05.2f}"


async def fetch_block_data(
    subtensor: AsyncSubtensor, block: int, semaphore: asyncio.Semaphore
) -> list:
    """
    Fetch all subnet info for a given block and return a list of (netuid, row_dict).
    """
    async with semaphore:
        try:
            ts_task = asyncio.create_task(subtensor.get_timestamp(block))
            infos_task = asyncio.create_task(subtensor.all_subnets(block))
            timestamp, infos = await asyncio.gather(ts_task, infos_task)

        except Exception as e:
            print(f"⚠️  Skipping block {block} due error: {e}")
            return []

        ts_iso = timestamp.isoformat()
        results = []
        for info in infos:
            row = {
                "timestamp": ts_iso,
                "block": block,
                "alpha_in": info.alpha_in.tao,
                "alpha_out": info.alpha_out.tao,
                "tao_in": info.tao_in.tao,
                "price": info.price.tao,
                "subnet_volume": info.subnet_volume.tao,
            }
            results.append((info.netuid, row))
        return results


async def main():
    print("Start")
    print(f"Using ws uri:{WS_URI}")
    existing_blocks = {}
    writers = {}
    files = {}

    print("Scanning existing CSV files...")
    for fname in os.listdir(OUTPUT_DIR):
        if fname.startswith("subtensor_") and fname.endswith(".csv"):
            netuid = int(fname.split("_")[1])
            existing_blocks[netuid] = set()
            path = os.path.join(OUTPUT_DIR, fname)
            with open(path, newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        existing_blocks[netuid].add(int(row["block"]))
                    except:
                        continue

    fetched_blocks = set()
    for netuid_blocks in existing_blocks.values():
        fetched_blocks |= netuid_blocks

    semaphore = asyncio.Semaphore(BATCH_SIZE)
    async with AsyncSubtensor(
        WS_URI,
        log_verbose=True,
        # fallback_endpoints=["wss://archive.chain.opentensor.ai:443"], 
    ) as subtensor:
        head = await subtensor.get_current_block()
        print(f"Current head block: {head}")

        missing_blocks = [
            blk for blk in range(head, 0, -1) if blk not in fetched_blocks
        ]
        if not missing_blocks:
            print("No block to download. Ending.")
            return

        print(f"Total missing blocks to fetch: {len(missing_blocks)}")

        total = len(missing_blocks)
        for i in range(0, total, BATCH_SIZE):
            batch = missing_blocks[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            print(f"\nProcessing batch {batch_num}: {batch[0]} … {batch[-1]}")

            start = time.perf_counter()
            results = await asyncio.gather(
                *[fetch_block_data(subtensor, blk, semaphore) for blk in batch]
            )
            elapsed = time.perf_counter() - start

            written = 0
            for block_rows in results:
                for netuid, row in block_rows:
                    blk = row["block"]
                    if blk in existing_blocks.get(netuid, set()):
                        continue

                    if netuid not in writers:
                        filename = os.path.join(
                            OUTPUT_DIR, f"subtensor_{netuid}_data.csv"
                        )
                        write_header = not os.path.isfile(filename)
                        f = open(filename, "a", newline="")
                        fieldnames = list(row.keys())
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        if write_header:
                            writer.writeheader()
                        writers[netuid] = writer
                        files[netuid] = f
                        existing_blocks.setdefault(netuid, set())

                    writers[netuid].writerow(row)
                    existing_blocks[netuid].add(blk)
                    written += 1

            print(
                f"Batch {batch_num}: wrote {written} rows in {format_seconds(elapsed)}"
            )
            print(
                f"Throughput: {written/elapsed if elapsed>0 else float('inf'):.2f} rows/sec"
            )

    for f in files.values():
        f.close()

    print("Data update complete. CSV files are up to date.")


if __name__ == "__main__":
    asyncio.run(main())
