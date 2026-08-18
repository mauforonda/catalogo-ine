#!/usr/bin/env python3
"""Build a resilient catalogue of public INE document metadata.

The catalogue is a snapshot of the metadata that can be retrieved during this
run.  A failed WordPress page or WebDAV resource is skipped; it never aborts
the rest of the build.  The output is replaced atomically only after at least
one valid record has been collected.
"""

from __future__ import annotations

import asyncio
import csv
import os
import random
import sys
import tempfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from xml.parsers.expat import ExpatError
from zoneinfo import ZoneInfo

import httpx
import xmltodict
from bs4 import BeautifulSoup


WORDPRESS_SOURCES = (
    "https://www.ine.gob.bo/wp-json/wp/v2/pages",
    "https://cpv2024.ine.gob.bo/index.php/wp-json/wp/v2/pages",
)
SUPPORTED_HOSTS = frozenset({"nube.ine.gob.bo", "nimbus.ine.gob.bo"})
OUTPUT_FILE = Path("catalogo.csv")
PER_PAGE = 100
PAGE_CONCURRENCY = 6
METADATA_CONCURRENCY = 20
PER_HOST_CONCURRENCY = 8
MAX_ATTEMPTS = 3
RETRYABLE_STATUS_CODES = frozenset({429, 500, 502, 503, 504})
# A WebDAV metadata response is small.  Fail fast enough to keep a stalled
# service from monopolising a worker, while allowing normal cross-region I/O.
TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=15.0, pool=5.0)
RETRY_BACKOFF_SECONDS = 0.5
TIME_ZONE = ZoneInfo("America/La_Paz")

TYPE_MAP = {
    "application/pdf": "pdf",
    "application/vnd.ms-excel": "excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "spreadsheet",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "word",
    "application/x-rar-compressed": "rar",
    "application/zip": "zip",
    "application/octet-stream": "stream",
}


@dataclass(frozen=True)
class Document:
    host: str
    token: str
    name: str

    @property
    def link(self) -> str:
        return f"https://{self.host}/index.php/s/{self.token}/download"


@dataclass
class BuildStats:
    pages: int = 0
    documents: int = 0
    metadata: int = 0


def request_url(host: str) -> str:
    return f"https://{host}/public.php/webdav"


async def request_with_retries(
    client: httpx.AsyncClient, method: str, url: str, **kwargs: Any
) -> httpx.Response | None:
    """Return a successful response, or None after bounded retries."""
    for attempt in range(MAX_ATTEMPTS):
        try:
            response = await client.request(method, url, **kwargs)
        except httpx.RequestError:
            response = None
        else:
            if response.is_success:
                return response
            if response.status_code not in RETRYABLE_STATUS_CODES:
                return None

        if attempt + 1 < MAX_ATTEMPTS:
            await asyncio.sleep(RETRY_BACKOFF_SECONDS * (2**attempt) + random.uniform(0, 0.1))
    return None


async def fetch_source_pages(client: httpx.AsyncClient, source: str) -> list[dict[str, Any]]:
    """Fetch all WordPress pages from one source, tolerating individual failures."""
    params = {"orderby": "modified", "per_page": PER_PAGE, "offset": 0}
    first = await request_with_retries(client, "GET", source, params=params)
    if first is None:
        return []
    try:
        pages = first.json()
    except ValueError:
        return []
    if not isinstance(pages, list):
        return []

    try:
        total = int(first.headers.get("X-WP-Total", len(pages)))
    except ValueError:
        total = len(pages)
    offsets = range(PER_PAGE, total, PER_PAGE)
    semaphore = asyncio.Semaphore(PAGE_CONCURRENCY)

    async def fetch_offset(offset: int) -> list[dict[str, Any]]:
        async with semaphore:
            response = await request_with_retries(
                client, "GET", source,
                params={"orderby": "modified", "per_page": PER_PAGE, "offset": offset},
            )
        if response is None:
            return []
        try:
            result = response.json()
        except ValueError:
            return []
        return result if isinstance(result, list) else []

    batches = await asyncio.gather(*(fetch_offset(offset) for offset in offsets))
    return pages + [page for batch in batches for page in batch]


def extract_documents(pages: Iterable[dict[str, Any]]) -> dict[tuple[str, str], Document]:
    """Extract known public-share links, keeping one useful name per document."""
    documents: dict[tuple[str, str], Document] = {}
    for page in pages:
        content = page.get("content", {}).get("rendered", "")
        for anchor in BeautifulSoup(content, "html.parser").select("a[href]"):
            parsed = urlparse(anchor["href"])
            host = (parsed.hostname or "").lower()
            parts = [part for part in parsed.path.split("/") if part]
            try:
                token = parts[parts.index("s") + 1]
            except (ValueError, IndexError):
                continue
            if host not in SUPPORTED_HOSTS or not token:
                continue
            name = anchor.get_text(" ", strip=True) or token
            key = (host, token)
            existing = documents.get(key)
            if existing is None or len(name) > len(existing.name):
                documents[key] = Document(host=host, token=token, name=name)
    return documents


def normalise_keys(values: dict[str, Any]) -> dict[str, Any]:
    return {key.rsplit(":", 1)[-1]: value for key, value in values.items()}


def find_properties(node: Any) -> dict[str, Any] | None:
    """Find the WebDAV property set that describes a downloadable resource."""
    if isinstance(node, dict):
        properties = normalise_keys(node)
        if "getcontentlength" in properties and "getlastmodified" in properties:
            return properties
        for value in node.values():
            found = find_properties(value)
            if found is not None:
                return found
    elif isinstance(node, list):
        for value in node:
            found = find_properties(value)
            if found is not None:
                return found
    return None


def metadata_record(document: Document, xml: str) -> dict[str, str | float] | None:
    try:
        properties = find_properties(xmltodict.parse(xml))
        if properties is None:
            return None
        modified = parsedate_to_datetime(str(properties["getlastmodified"]))
        if modified.tzinfo is None:
            modified = modified.replace(tzinfo=ZoneInfo("UTC"))
        size = int(properties["getcontentlength"])
    except (ExpatError, TypeError, ValueError, OverflowError):
        return None

    content_type = str(properties.get("getcontenttype") or "")
    return {
        "modificado": modified.astimezone(TIME_ZONE).isoformat(sep=" ", timespec="seconds"),
        "nombre": document.name,
        "tipo": TYPE_MAP.get(content_type, content_type or "unknown"),
        "kb": round(size / 1024, 2),
        "link": document.link,
    }


async def fetch_metadata(
    client: httpx.AsyncClient, document: Document, semaphore: asyncio.Semaphore
) -> dict[str, str | float] | None:
    async with semaphore:
        response = await request_with_retries(
            client,
            "PROPFIND",
            request_url(document.host),
            auth=(document.token, ""),
            headers={"Depth": "1"},
        )
    return None if response is None else metadata_record(document, response.text)


def write_catalog(records: list[dict[str, str | float]], output: Path) -> None:
    """Atomically replace the final CSV, never exposing a half-written file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    records.sort(
        key=lambda row: (
            str(row["modificado"]),
            str(row["link"]),
            str(row["nombre"]),
            str(row["tipo"]),
        )
    )
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="", dir=output.parent, delete=False
    ) as temporary:
        writer = csv.DictWriter(
            temporary,
            fieldnames=["modificado", "nombre", "tipo", "kb", "link"],
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(records)
        temporary_name = temporary.name
    os.replace(temporary_name, output)


async def build_catalog() -> tuple[list[dict[str, str | float]], BuildStats]:
    stats = BuildStats()
    limits = httpx.Limits(max_connections=METADATA_CONCURRENCY, max_keepalive_connections=10)
    async with httpx.AsyncClient(timeout=TIMEOUT, limits=limits, follow_redirects=True) as client:
        source_pages = await asyncio.gather(*(fetch_source_pages(client, source) for source in WORDPRESS_SOURCES))
        pages = [page for source in source_pages for page in source]
        stats.pages = len(pages)
        documents = extract_documents(pages)
        stats.documents = len(documents)
        semaphores = {host: asyncio.Semaphore(PER_HOST_CONCURRENCY) for host in SUPPORTED_HOSTS}
        results = await asyncio.gather(
            *(
                fetch_metadata(client, document, semaphores[document.host])
                for _, document in sorted(documents.items())
            )
        )
    records = [record for record in results if record is not None]
    stats.metadata = len(records)
    return records, stats


def main() -> int:
    records, stats = asyncio.run(build_catalog())
    if not records:
        print(f"No se pudo construir un catálogo válido (páginas={stats.pages}, enlaces={stats.documents}).")
        return 1
    write_catalog(records, OUTPUT_FILE)
    print(f"Catálogo actualizado: {stats.metadata}/{stats.documents} enlaces, {stats.pages} páginas.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
