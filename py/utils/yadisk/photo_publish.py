import asyncio
import nest_asyncio
import aiohttp


API_URL = "https://cloud-api.yandex.net/v1/disk/public/resources"

nest_asyncio.apply()
async def _fetch_img_link(
    session,
    public_url,
    *,
    timeout,
):
    params = {
        "public_key": public_url,
        "path": "/"  
    }
    async with session.get(API_URL, params=params, timeout=timeout) as resp:
        resp.raise_for_status()
        data = await resp.json()
        
        direct_link = [x for x in data["sizes"] if x['name'] == 'ORIGINAL'][0]['url']
        return direct_link

async def get_img_links(
    public_urls,
    *,
    timeout,
    max_concurrency,
    return_errors,
):

    urls = list(public_urls)
    sem = asyncio.Semaphore(max_concurrency)

    async with aiohttp.ClientSession() as session:
        async def bound_task(idx, url):
            try:
                async with sem:
                    href = await _fetch_img_link(session, url, timeout=timeout)
                return idx, href, None
            except Exception as e:
                return idx, None, e

        tasks = [asyncio.create_task(bound_task(i, url)) for i, url in enumerate(urls)]
        results = [None] * len(urls)

        for task in asyncio.as_completed(tasks):
            idx, href, err = await task
            if err is None:
                results[idx] = href
            else:
                results[idx] = (None, err) if return_errors else None

        return results

def get_img_links_sync(
    public_urls,
    *,
    timeout = 30,
    max_concurrency = 40,
    return_errors = True,
):

    return asyncio.run(
        get_img_links(
            public_urls,
            timeout=timeout,
            max_concurrency=max_concurrency,
            return_errors=return_errors,
        )
    )
