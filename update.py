#!/usr/bin/env python3
"""Build a durable catalogue of public INE document metadata.

Each run discovers the current links and merges them with the previous CSV.
Rows no longer discovered are retained, but marked unavailable.
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
CATALOG_FIELDS = ("modificado", "nombre", "pagina", "tipo", "kb", "link", "disponible")
CatalogRecord = dict[str, str | float | bool]


@dataclass(frozen=True)
class Document:
    host: str
    token: str
    name: str
    page: str

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
        page_title = BeautifulSoup(page.get("title", {}).get("rendered", ""), "html.parser").get_text(" ", strip=True)
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
            if existing is None or (len(name), len(page_title), page_title) > (len(existing.name), len(existing.page), existing.page):
                documents[key] = Document(host=host, token=token, name=name, page=page_title)
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


def metadata_record(document: Document, xml: str) -> CatalogRecord | None:
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
        "pagina": document.page,
        "tipo": TYPE_MAP.get(content_type, content_type or "unknown"),
        "kb": round(size / 1024, 2),
        "link": document.link,
        "disponible": True,
    }


async def fetch_metadata(
    client: httpx.AsyncClient, document: Document, semaphore: asyncio.Semaphore
) -> CatalogRecord | None:
    async with semaphore:
        response = await request_with_retries(
            client,
            "PROPFIND",
            request_url(document.host),
            auth=(document.token, ""),
            headers={"Depth": "1"},
        )
    return None if response is None else metadata_record(document, response.text)


def load_catalog(source: Path) -> dict[str, CatalogRecord]:
    """Load the prior index, accepting catalogues from older schema versions."""
    if not source.exists():
        return {}
    with source.open(encoding="utf-8", newline="") as file:
        return {
            row["link"]: {
                "modificado": row.get("modificado", ""),
                "nombre": row.get("nombre", ""),
                "pagina": row.get("pagina", ""),
                "tipo": row.get("tipo", ""),
                "kb": row.get("kb", ""),
                "link": row["link"],
                "disponible": False,
            }
            for row in csv.DictReader(file)
            if row.get("link")
        }


def consolidate_catalog(
    previous: dict[str, CatalogRecord],
    documents: Iterable[Document],
    metadata: Iterable[CatalogRecord],
) -> list[CatalogRecord]:
    """Retain every known link and mark this run's discovered links available."""
    catalog = {link: {**record, "disponible": False} for link, record in previous.items()}
    available = {str(record["link"]): record for record in metadata}

    for document in documents:
        record = available.get(document.link)
        if record is not None:
            catalog[document.link] = record
            continue
        prior = catalog.get(document.link, {})
        catalog[document.link] = {
            **prior,
            "modificado": prior.get("modificado", ""),
            "nombre": document.name,
            "pagina": document.page,
            "tipo": prior.get("tipo", ""),
            "kb": prior.get("kb", ""),
            "link": document.link,
            "disponible": True,
        }
    return list(catalog.values())


def write_catalog(records: list[CatalogRecord], output: Path) -> None:
    """Atomically replace the final CSV, never exposing a half-written file."""
    output.parent.mkdir(parents=True, exist_ok=True)
    records.sort(
        key=lambda row: (
            str(row["modificado"]),
            str(row["link"]),
            str(row["nombre"]),
            str(row["pagina"]),
            str(row["tipo"]),
        )
    )
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", newline="", dir=output.parent, delete=False
    ) as temporary:
        writer = csv.DictWriter(
            temporary,
            fieldnames=CATALOG_FIELDS,
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(records)
        temporary_name = temporary.name
    os.replace(temporary_name, output)


async def build_catalog() -> tuple[list[CatalogRecord], list[Document], BuildStats]:
    stats = BuildStats()
    limits = httpx.Limits(max_connections=METADATA_CONCURRENCY, max_keepalive_connections=10)
    async with httpx.AsyncClient(timeout=TIMEOUT, limits=limits, follow_redirects=True) as client:
        print("Páginas: consultando fuentes…", flush=True)
        source_pages = await asyncio.gather(*(fetch_source_pages(client, source) for source in WORDPRESS_SOURCES))
        pages = [page for source in source_pages for page in source]
        stats.pages = len(pages)
        documents = extract_documents(pages)
        stats.documents = len(documents)
        print(f"Páginas: {stats.pages}; enlaces: {stats.documents}", flush=True)
        semaphores = {host: asyncio.Semaphore(PER_HOST_CONCURRENCY) for host in SUPPORTED_HOSTS}
        tasks = [
            asyncio.create_task(fetch_metadata(client, document, semaphores[document.host]))
            for _, document in sorted(documents.items())
        ]
        print(f"Metadatos: 0/{len(tasks)}", flush=True)
        results = []
        for completed, task in enumerate(asyncio.as_completed(tasks), start=1):
            results.append(await task)
            if completed % 100 == 0 or completed == len(tasks):
                print(f"Metadatos: {completed}/{len(tasks)}", flush=True)
    records = [record for record in results if record is not None]
    stats.metadata = len(records)
    return records, list(documents.values()), stats


def main() -> int:
    records, documents, stats = asyncio.run(build_catalog())
    if not documents:
        print(f"No se descubrieron enlaces (páginas={stats.pages}); se conserva el catálogo anterior.")
        return 1
    catalog = consolidate_catalog(load_catalog(OUTPUT_FILE), documents, records)
    write_catalog(catalog, OUTPUT_FILE)
    available = sum(record["disponible"] is True for record in catalog)
    print(
        f"Catálogo consolidado: {available}/{len(catalog)} disponibles; "
        f"{stats.metadata} metadatos actualizados; {stats.pages} páginas."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
