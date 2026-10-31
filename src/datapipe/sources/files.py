import asyncio
import csv
import json
import os
import random
from collections.abc import AsyncIterator, Callable, Iterator
from pathlib import Path

import httpx

from datapipe.config.schema import HttpSource, Settings
from datapipe.core.protocols import Record


def csv_records(path: Path) -> Iterator[Record]:
    with path.open("r", encoding="utf-8-sig", newline="") as stream:
        yield from csv.DictReader(stream)


def jsonl_records(path: Path) -> Iterator[Record]:
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            if not line.strip():
                continue
            value = json.loads(line)
            if not isinstance(value, dict):
                raise TypeError(f"{path}:{line_number}: expected a JSON object")
            yield value


def _nested_value(data: object, path: str) -> object:
    value = data
    for part in path.split(".") if path else []:
        if not isinstance(value, dict):
            return None
        value = value.get(part)
    return value


async def http_records(
    source: HttpSource,
    settings: Settings,
    client: httpx.AsyncClient,
    semaphore: asyncio.Semaphore,
    resume: str | None = None,
    cursor_callback: Callable[[str | None], None] | None = None,
) -> AsyncIterator[Record]:
    token = os.getenv(source.auth_env) if source.auth_env else None
    if source.auth_env and not token:
        raise ValueError(f"environment variable {source.auth_env!r} is not set")
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    url = resume or str(source.url)
    page = source.start_page
    if resume and resume.isdigit() and source.pagination == "page_number":
        page = int(resume)
    for page_index in range(source.max_pages):
        params = {source.page_param: page} if source.pagination == "page_number" else None
        async with semaphore:
            for attempt in range(settings.max_retries + 1):
                try:
                    response = await client.get(url, params=params, headers=headers)
                    response.raise_for_status()
                    break
                except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                    retryable = not isinstance(exc, httpx.HTTPStatusError) or exc.response.status_code >= 500 or exc.response.status_code == 429
                    if not retryable or attempt >= settings.max_retries:
                        raise
                    await asyncio.sleep(min(8.0, 0.25 * (2**attempt)) * random.uniform(0.7, 1.3))
        data = response.json()
        records = _nested_value(data, source.records_path)
        if not isinstance(records, list) or any(not isinstance(item, dict) for item in records):
            raise ValueError(f"HTTP response from {url} must contain an array of objects at {source.records_path!r}")
        for item in records:
            yield item
        if source.pagination == "none":
            if cursor_callback:
                cursor_callback(None)
            return
        if source.pagination == "next_url":
            next_url = _nested_value(data, source.next_url_path)
            if cursor_callback:
                cursor_callback(str(httpx.URL(str(source.url)).join(str(next_url))) if next_url else None)
            if not next_url:
                return
            url = str(httpx.URL(str(source.url)).join(str(next_url)))
        else:
            if not records:
                if cursor_callback:
                    cursor_callback(str(page))
                return
            page += 1
            if cursor_callback:
                cursor_callback(str(page))
        if page_index + 1 >= source.max_pages:
            raise ValueError(f"HTTP pagination exceeded max_pages={source.max_pages}")
