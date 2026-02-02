#!/usr/bin/env python3
# thanks Chat-GPT for sponsoring this script

import os
import asyncio
import random
import requests
import pandas as pd
from tqdm.auto import tqdm
import nest_asyncio
import yadisk

from py.utils.yadisk.yadisk_utils import get_dir_names


nest_asyncio.apply()

# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #

def _field(obj, key, default=None):
    """Safely get attribute/field from YaDiskObject or dict."""
    if obj is None:
        return default
    try:
        return getattr(obj, key)
    except AttributeError:
        try:
            return obj[key]
        except Exception:
            return default

async def _retry(coro_factory, *, max_tries=5, base=0.5, cap=5.0):
    """
    Retry an awaited call with exponential backoff + jitter.
    Respects HTTP 429 Retry-After if available on exception.response.
    """
    for attempt in range(1, max_tries + 1):
        try:
            return await coro_factory()
        except Exception as e:
            if attempt == max_tries:
                raise
            retry_after = None
            try:
                resp = getattr(e, "response", None)
                if resp is not None:
                    ra = resp.headers.get("Retry-After")
                    if ra:
                        retry_after = float(ra)
            except Exception:
                pass
            delay = retry_after if retry_after is not None else min(cap, base * (2 ** (attempt - 1)))
            # jitter ±40% around delay
            jitter = delay * (0.6 + 0.8 * random.random())
            await asyncio.sleep(jitter)

async def is_published(y, path):
    info = await y.get_meta(path, fields=["public_url", "public_key", "type"])
    return bool(_field(info, "public_url") or _field(info, "public_key"))

# --------------------------------------------------------------------------- #
# Core                                                                        #
# --------------------------------------------------------------------------- #

async def _gather_links_for_offer(y, offer_id, dir_path, files_concurrency: int = 8):
    """
    For a single offer directory:
      - Skip non-existent dir
      - Publish dir once if needed (non-fatal on failure)
      - List files, then process files concurrently (bounded)
      - Use listing's public_url when present (0 extra calls)
      - Only publish files when needed; then fetch public_url once
    """
    links = []

    if not await y.exists(dir_path):
        print(f"path {dir_path} does not exist")
        return offer_id, []

    try:
        if not await is_published(y, dir_path):
            await _retry(lambda: y.publish(dir_path))
    except Exception as e:
        # Not fatal; files may still be publishable
        print(f"warn: could not publish dir {dir_path}: {e!r}")

    items = []
    try:
        async for item in y.listdir(dir_path, fields=["type", "path", "name", "public_url"]):
            if (_field(item, "type", "") or "").lower() != "file":
                continue
            items.append(item)
    except Exception as e:
        print(f"[listdir error] dir={dir_path}: {e!r}")
        return offer_id, links

    if not items:
        return offer_id, links

    sem = asyncio.Semaphore(files_concurrency)

    async def process_item(item):
        async with sem:
            # Fast path: listing already provided a public URL
            existing = _field(item, "public_url")
            if existing:
                return existing

            file_path = _field(item, "path")
            if not file_path:
                return None

            try:
                # Check if already public
                meta = await _retry(lambda: y.get_meta(file_path, fields=["public_url", "public_key"]))
                already = _field(meta, "public_url")
                if already:
                    return already

                # Publish then fetch public_url once
                await _retry(lambda: y.publish(file_path))
                meta2 = await _retry(lambda: y.get_meta(file_path, fields=["public_url"]))
                url = _field(meta2, "public_url")
                if url:
                    return url

                print(f"No public URL after publish for {_field(item,'name')} in {dir_path}")
                return None

            except Exception as e:
                print(f"[file error] {dir_path}/{_field(item,'name')}: {e!r}")
                return None

    tasks = [asyncio.create_task(process_item(it)) for it in items]
    for fut in asyncio.as_completed(tasks):
        url = await fut
        if url:
            links.append(url)

    # Dedup, preserve order
    seen, deduped = set(), []
    for u in links:
        if u not in seen:
            seen.add(u)
            deduped.append(u)

    return offer_id, deduped

async def _run_async(df_in, dt_type, concurrency, token, files_concurrency=8):
    # Resolve directory names for unique offers
    unique_offers = pd.Series(df_in["offer_id"].unique(), name="offer_id")
    dir_df = get_dir_names(unique_offers.tolist(), dt_type=dt_type)
    dir_df["dir_path"] = "/cian_project_photos/" + dir_df["dir"].astype(str) + "/photos"

    offer_to_dir = (
        dir_df.dropna(subset=["dir_path"])
              .set_index("offer_id")["dir_path"]
              .to_dict()
    )

    sem = asyncio.Semaphore(concurrency)
    results = []

    if not offer_to_dir:
        out = df_in.copy()
        out["photo_urls"] = [[] for _ in range(len(out))]
        return out

    async with yadisk.AsyncClient(token=token) as y:
        async def guarded_task(offer_id, dir_path):
            async with sem:
                try:
                    return await _gather_links_for_offer(y, offer_id, dir_path, files_concurrency=files_concurrency)
                except Exception as e:
                    print(f"[task error] offer_id={offer_id} dir={dir_path}: {e!r}")
                    return (offer_id, [])

        tasks = [asyncio.create_task(guarded_task(oid, dpath))
                 for oid, dpath in offer_to_dir.items()]

        if tasks:
            for coro in tqdm(asyncio.as_completed(tasks),
                             total=len(tasks),
                             desc="Fetching photo links"):
                try:
                    res = await coro
                except Exception as e:
                    print(f"[await error] {e!r}")
                    res = (None, [])
                results.append(res)

    # Filter out None keys
    results = [r for r in results if r and r[0] is not None]

    offer_to_links = {offer_id: links for offer_id, links in results}

    out = df_in.copy()
    out["photo_urls"] = out["offer_id"].map(offer_to_links).apply(
        lambda x: x if isinstance(x, list) else []
    )
    return out

# --------------------------------------------------------------------------- #
# Public API                                                                  #
# --------------------------------------------------------------------------- #

def get_public_links_for_photos_df(
    df,
    dt_type='first',
    concurrency=20,
    token=os.environ["YANDEX_DISK_TOKEN"],
    files_concurrency=8,  # per-directory parallelism, tune 4..16
):
    # In notebook/REPL environments, nest_asyncio patches asyncio.run safely.
    return asyncio.run(
        _run_async(df, dt_type=dt_type, concurrency=concurrency,
                   token=token, files_concurrency=files_concurrency)
    )

def get_img_link(public_url):
    """
    Return a one-time direct download URL via the official download endpoint.
    """

    API_URL = "https://cloud-api.yandex.net/v1/disk/public/resources"

    resp = requests.get(
        f"{API_URL}/download",
        params={"public_key": public_url},
        timeout=30,
    )
    resp.raise_for_status()
    href = resp.json().get("href")
    if not href:
        raise RuntimeError("No download href in Yandex response")
    return href
