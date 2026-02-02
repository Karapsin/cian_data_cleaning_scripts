import os
import json
import asyncio
from pathlib import Path

import nest_asyncio
import pandas as pd
import yadisk
from tqdm.asyncio import tqdm_asyncio
import aiohttp
from yadisk.exceptions import YaDiskError

CHECKPOINT_PATH = Path("yadisk_dirs_checkpoint.json")
RESULT_CSV_PATH = Path("yadisk_dirs.csv")


async def get_dirs_async(
    token: str,
    path: str = "/cian_project_photos",
    batch: int = 5_000,
    concurrency: int = 20,
    max_retries: int = 10,
    base_delay: float = 1.0,
    processed_offsets: set[int] | None = None,
) -> tuple[list[str], set[int]]:
    """
    Returns:
        dirs_this_run: list of folder names fetched in THIS run
        all_processed_offsets: updated set of processed offsets (including previous + this run)
    """

    if processed_offsets is None:
        processed_offsets = set()

    async with yadisk.AsyncClient(token=token) as y:

        # get total items
        meta = await y.get_meta(path, fields=["_embedded.total"])
        total = meta["embedded"]["total"]  

        sem = asyncio.Semaphore(concurrency)

        # all possible offsets
        all_offsets = list(range(0, total, batch))

        # only fetch those that are NOT processed yet
        offsets_to_fetch = [off for off in all_offsets if off not in processed_offsets]

        print(f"Total: {total}, pages: {len(all_offsets)}, "
              f"already done: {len(processed_offsets)}, "
              f"to fetch now: {len(offsets_to_fetch)}")

        async def fetch_page(off: int) -> tuple[int, list[str]]:
            """One page with retry + backoff."""
            attempt = 0
            delay = base_delay

            while True:
                attempt += 1
                try:
                    async with sem:
                        names = [
                            obj["name"]
                            async for obj in y.listdir(
                                path,
                                type="dir",
                                limit=batch,
                                offset=off,
                                fields=["name"],
                            )
                        ]
                    return off, names

                except (aiohttp.ClientError, YaDiskError, asyncio.TimeoutError) as e:
                    if attempt >= max_retries:
                        # final failure – propagate
                        print(f"Offset {off}: giving up after {attempt} attempts ({e})")
                        raise
                    print(f"Offset {off}: attempt {attempt} failed ({e}), "
                          f"retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
                    delay *= 2  # exponential backoff

        # create tasks only for offsets we still need
        tasks = [asyncio.create_task(fetch_page(off)) for off in offsets_to_fetch]

        dirs_this_run: list[str] = []
        all_processed_offsets = set(processed_offsets)

        # consume tasks as they complete, with progress bar
        for coro in tqdm_asyncio.as_completed(tasks, desc="Fetching pages"):
            off, names = await coro
            dirs_this_run.extend(names)
            all_processed_offsets.add(off)

            # update checkpoint on every page (cheap and safe)
            CHECKPOINT_PATH.write_text(
                json.dumps(sorted(all_processed_offsets), ensure_ascii=False)
            )

        return dirs_this_run, all_processed_offsets


def refresh_yadisk_dirs():
    nest_asyncio.apply()

    # 1. Load which offsets we already processed (if any)
    if CHECKPOINT_PATH.exists():
        processed_offsets = set(json.loads(CHECKPOINT_PATH.read_text()))
    else:
        processed_offsets = set()

    # 2. Run async fetch (will only fetch missing offsets)
    new_dirs, all_processed_offsets = asyncio.run(
        get_dirs_async(
            os.environ["YANDEX_DISK_TOKEN"],
            "/cian_project_photos",
            processed_offsets=processed_offsets,
        )
    )

    # 3. Load existing dirs from previous runs
    if RESULT_CSV_PATH.exists():
        old_df = pd.read_csv(RESULT_CSV_PATH)
        old_dirs = set(old_df["dir"].astype(str))
    else:
        old_dirs = set()

    # 4. Merge old + new and build final DataFrame
    all_dirs = old_dirs.union(new_dirs)

    df = pd.DataFrame(sorted(all_dirs), columns=["dir"])
    df["offer_id"] = (
        df["dir"]
        .str.split("_")
        .apply(lambda x: x[0])
        .str.replace("rentflat", "", regex=False)
        .str.replace("saleflat", "", regex=False)
    )

    # 5. Save final CSV
    df.to_csv(RESULT_CSV_PATH, index=False)

    # 6. Run completed successfully: checkpoint is no longer needed
    try:
        CHECKPOINT_PATH.unlink(missing_ok=True)
    except FileNotFoundError:
        pass

    print(
        f"Saved {len(df)} dirs to {RESULT_CSV_PATH}, "
        f"{len(all_processed_offsets)} pages marked done."
    )
