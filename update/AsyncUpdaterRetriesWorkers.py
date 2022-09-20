#!/bin/env python3.10

# Thanks to https://github.com/mauforonda/catalogo-ine
# and https://github.com/talkpython/async-techniques-python-course

# This Module retrieves a (possibly generic) list of WordPress site JSON files
# asynchronously and gathers the file modification times and sizes
# using the WP Webdav endpoint provided.

# Todo:
# precise timing tests
# async write buffer for very large responses
# Test time: 1371 files took 170.03997470800095s to download with 3 workers
#  155s with 4 workers
# It took 113.98988858300436s to download with 6 workers

import os
import random
import sys
import time
import asyncio
from collections import defaultdict
from typing import List, Dict, Union, Tuple, Optional, Any, DefaultDict
from pydantic import BaseModel, HttpUrl
import httpx
import pandas as pd
from bs4 import BeautifulSoup
import datetime as dt
import xmltodict
# simple StreamHandler log with custom format
# from fast_tc.logging import create_logger

# AsyncUpdater.workers should be a divisor of MAX_PAGES and per_page

SLEEP_T = 0.001
SLEEP_BACKOFF = 0.2
# multiple of per_page
MAX_PAGES = 360
PER_PAGE = 30
TIMEOUT = 20
KEEPALIVES = 30
MAX_CONNECTIONS = 100
MAX_WRITES = 200
TIPO_MAP = {
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'spreadsheet',
    'application/pdf': 'pdf',
    'application/vnd.ms-excel': 'excel',
    'application/x-rar-compressed': 'rar',
    'application/zip': 'zip',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'word',
    'application/octet-stream': 'stream'
}


class TaskException(Exception):
    def __init__(self, value: Union[int, str], action: str = Union["GET", "PROPFIND"]):
        self.action = action
        self.value = value


class UpdaterConfig(BaseModel):
    csv_file: str = "catalogo_ine.csv"
    keepalives: int = 5
    log_level: str = "INFO"
    max_pages: int = MAX_PAGES
    max_connections: int = MAX_CONNECTIONS
    per_page: int = PER_PAGE
    time_zone: str = "America/La_Paz"
    url_domain: HttpUrl
    url_download: HttpUrl
    url_json: HttpUrl
    url_webdav: HttpUrl


class AsyncUpdater:

    def __init__(self, conf: UpdaterConfig, workers: int = 2) -> None:
        self.active_retries = 0
        self.conf = conf
        self.limits: httpx.Limits = httpx.Limits(
            max_keepalive_connections=self.conf.keepalives,
            max_connections=self.conf.max_connections)
        self.loop = asyncio.new_event_loop()
        self.clients = []
        self.count_files = 0
        self.df = None
        self.file_number = 0
        self.catalog = []
        self.count_meta = 0
        self.count_pages = 0
        self.count_writes = 0
        self.error_404 = 0
        self.error_429 = 0
        self.error_no_metastatus = 0
        self.error_request = 0
        self.error_status = 0
        self.first_write: bool = True
        self.frame_buffer = []
        self.log_level = self.conf.log_level
        name = type(self).__name__
        # self.log = create_logger(name, log_level=self.log_level)
        self.tasks_get: DefaultDict[int, List[asyncio.Task]] = defaultdict(list)
        self.tasks_meta: DefaultDict[int, List[asyncio.Task]] = defaultdict(list)
        self.timeout_connect = 0
        self.timeout_read = 0
        self.workers = workers
        self.worker_tasks: List[asyncio.Task] = []

    @property
    def failures(self):
        """ Sum of all httpx errors """
        # 404 and 429 are counted separately but included in error_request
        return self.error_status + self.error_request

    @property
    def num_offsets(self):
        return self.conf.max_pages // self.conf.per_page

    async def limit(self, backoff=False, on_retry: int = 0) -> None:
        # very simple request limit
        if not backoff and not on_retry:
            await asyncio.sleep(SLEEP_T)
            return
        if not backoff:
            wait_time = SLEEP_T * (2 ** on_retry)
            await asyncio.sleep(wait_time)
        else:
            wait_time = SLEEP_BACKOFF * (2 ** on_retry)
            await asyncio.sleep(wait_time)
        self.active_retries -= 1
        return

    def format_catalog(self) -> None:
        """
        Crea un dataframe con los metadatos en el catálogo
        """

        self.df = pd.DataFrame(self.catalog)
        self.df = self.df[['name', 'token', 'getlastmodified', 'getcontentlength', 'getcontenttype']]
        self.df.columns = ['nombre', 'token', 'modificado', 'largo', 'tipo']

        self.df.insert(2, 'link',
                       self.df.token.apply(lambda x: self.conf.url_download.format(x)))
        self.df.insert(3, 'kb', self.df.largo / 1024)
        self.df.drop(columns=['largo'], inplace=True)
        self.df.loc[:, 'tipo'] = self.df.tipo.map(TIPO_MAP)
        self.df['modificado'] = self.df.set_index('modificado').tz_localize("UTC").index.tz_convert(self.conf.time_zone)
        self.df = self.df.sort_values('modificado', ascending=False)
        return

    def save_csv(self, file_name: str = None) -> None:
        """
        Guarda resultados
        """
        if file_name:
            file = file_name
        else:
            file = self.conf.csv_file

        catalog = self.df[['modificado', 'nombre', 'tipo', 'kb', 'link']]
        if os.path.exists(file):
            old = pd.read_csv(file, parse_dates=['modificado'])
            catdf_historial = pd.concat([old, catalog]).drop_duplicates(subset=['modificado', 'nombre', 'tipo', 'link'],
                                                                        keep='first')
            catdf_historial.sort_values('modificado').to_csv(
                '_'.join([file[:file.index('.')], 'historial']) + '.csv',
                index=False, float_format="%.2f")
        catalog.sort_values(['modificado', 'link']).to_csv(file, index=False, float_format="%.2f")

    def parse_pages(self, pages: Dict) -> Optional[List[Dict]]:
        """
        Recolecta enlaces a files en nube.ine.gob.bo del contenido de una página
        """
        pre_frame: List = []
        for page in pages:
            content = BeautifulSoup(page['content']['rendered'], 'html.parser')
            file_links = [{'pageid': page['id'],
                           'pagecreated': page['date'],
                           'pagemodified': page['modified'],
                           'pagelink': page['link'],
                           'pagetitle': page['title']['rendered'],
                           'link': a['href'],
                           'name': a.get_text()}
                          for a in content.select('a')
                          if a.has_attr('href') and self.conf.url_domain in a['href']]
            if not file_links:
                continue
            pre_frame.extend(file_links)
        return pre_frame if pre_frame else None

    async def http_request(self,
                           worker: int,
                           method: str,
                           item: Union[int, str],
                           tries: int = 2,
                           timeout: int = TIMEOUT
                           ) -> Optional[httpx.Response]:

        client = self.clients[worker]
        resp, auth = (None,) * 2
        if method == "GET":
            url = self.conf.url_json.format(self.conf.per_page, item)
            item_error = f"Offset: {item}"

        elif method == "PROPFIND":
            auth = (item, '')
            url = self.conf.url_webdav
            item_error = f"Token: {item}"
        else:
            raise TaskException("Invalid http method requested", method)

        total_tries = tries
        while tries:
            tries -= 1
            try:
                resp = await client.request(method=method, url=url, auth=auth, timeout=timeout)
                resp.raise_for_status()
                await self.limit()
            # except httpx.RequestError as exc:
                # handled by httpx transport retry
                # if tries:
                #     self.log.info(f"Retry {debug_string}")
                #     await self.limit(backoff=True, on_retry=total_tries - tries)
                #     continue
                # self.error_request += 1
                # raise TaskException(debug_string, action=method)
            except httpx.HTTPStatusError as exc:
                code = exc.response.status_code
                debug_string = f"{item_error} {code} {exc.request.url}"
                if tries and code in [413, 429, 503, 504]:
                    self.log.info(f"Retry {debug_string}")
                    await self.limit(backoff=True, on_retry=total_tries - tries)
                    continue
                error_attr = f"error_{code}"
                error_cnt = getattr(self, error_attr)
                if error_cnt is not None:
                    setattr(self, error_attr, error_cnt + 1)
                self.error_status += 1
                raise TaskException(debug_string, action=method)
            break
        return resp

    async def get_request(self, offset: int, worker: int) -> Tuple[Optional[httpx.Response], int]:
        resp = await self.http_request(method="GET", item=offset, tries=2, worker=worker)
        return resp, offset

    async def meta_request(self, token: str, worker: int) -> Optional[httpx.Response]:
        resp = await self.http_request(method="PROPFIND", item=token, tries=2, worker=worker)
        return resp

    @staticmethod
    def format_frame(frame_list: List[Dict]) -> pd.DataFrame:
        """
        Crea un dataframe con resultados de la recolección de enlaces
        """

        df = pd.DataFrame(frame_list)
        df = df.drop_duplicates(subset=['link'])
        df['token'] = df.link.apply(lambda x: x.replace('/download', '').split('/')[-1])
        df = df.sort_values('pagecreated').reset_index(drop=True)
        return df

    async def get_token_meta(self, token: str, worker: int) -> Optional[httpx.Response]:
        """
        Consulta metadatos para un file en URL webdav según su token
        """
        resp = await self.meta_request(token, worker=worker)
        return resp

    @staticmethod
    def process_file_meta(meta_text: str) -> Optional[Any]:
        meta_xml = xmltodict.parse(meta_text)
        if meta_xml.__contains__('d:multistatus'):
            metadata = meta_xml['d:multistatus']['d:response']['d:propstat']['d:prop']
            metadata = {k.replace('d:', ''): metadata[k] for k in metadata.keys()}
            metadata['getlastmodified'] = dt.datetime.strptime(metadata['getlastmodified'], '%a, %d %b %Y %H:%M:%S %Z')
            metadata['getcontentlength'] = int(metadata['getcontentlength'])
            metadata['getetag'] = metadata['getetag'].replace('"', '')
            return metadata
        return None

    async def build_catalog(self, offsets: range, worker: int) -> None:
        for offset in offsets:
            self.tasks_get[worker].append(
                asyncio.create_task(
                    (self.get_request(offset=offset, worker=worker)), name=f"Get Links at Offset {offset}"))
        for task in self.tasks_get[worker]:
            task_result, task_offset = None, None
            try:
                task_result, task_offset = await task
                if task_result is None:
                    continue
            except TaskException as texc:
                #self.log.debug(f"{texc.action} {texc.value}")
                continue
            finally:
                pass
                #self.log.debug(f"Processed get result for {task_offset}")
            json = task_result.json()
            self.count_pages += len(json)
            parsed_file_links = self.parse_pages(json)
            if parsed_file_links is None:
                continue
            frame = self.format_frame(parsed_file_links)
            rows = len(frame.index)
            for i in range(rows):
                self.count_files += 1
                row = frame.iloc[i]
                token = row.token
                try:
                    meta_resp = await self.get_token_meta(token, worker=worker)
                    if meta_resp is None:
                        self.error_no_metastatus += 1
                        continue
                except TaskException as texc:
                    #self.log.debug(f"{texc.action} {texc.value}")
                    self.error_no_metastatus += 1
                    continue
                finally:
                    pass
                    #self.log.debug(f"Processed token result {token}")
                metadata = xmltodict.parse(meta_resp.text)
                if metadata is None:
                    self.error_no_metastatus += 1
                    continue
                processed_meta = self.process_file_meta(meta_resp.text)
                if processed_meta is None:
                    self.error_no_metastatus += 1
                    continue
                self.count_meta += 1
                self.catalog.append({**row.to_dict(), **processed_meta})
                del row
                del metadata
                del meta_resp
                del processed_meta
                #self.log.debug(f"Added token row: {token}")
            del task_result
            del json
            del frame
            del parsed_file_links
            # self.log.info(sys.getsizeof(self.catalog))

    async def close_clients(self):
        [await c.aclose() for c in self.clients]

    def run_loop(self) -> None:
        t0 = time.monotonic()
        batch_size = self.conf.max_pages // self.workers
        self.worker_tasks = []

        for i in range(self.workers):
            transport = httpx.AsyncHTTPTransport(retries=2)
            client = httpx.AsyncClient(http2=True, limits=self.limits, transport=transport)
            self.clients.append(client)
            rng = range(i * batch_size, (i + 1) * batch_size, self.conf.per_page)
            task = self.loop.create_task(self.build_catalog(rng, worker=i), name=f"Worker {i} range: {rng}")
            self.worker_tasks.append(task)
        workers = asyncio.gather(*self.worker_tasks)
        self.loop.run_until_complete(workers)
        self.loop.run_until_complete(self.close_clients())
        self.format_catalog()
        t1 = time.monotonic()
        self.save_csv()
        t2 = time.monotonic()
        print(f"It took {t1 - t0}s to download and {t2 - t1}s to write")


def main():
    url = 'https://www.ine.gob.bo/wp-json/wp/v2/pages?orderby=modified&per_page={}&offset={}'
    webdav_url = 'https://nube.ine.gob.bo/public.php/webdav'
    download_url = 'https://nube.ine.gob.bo/index.php/s/{}/download'
    domain = 'https://nube.ine.gob.bo'
    ine_config = UpdaterConfig(url_domain=domain,
                               url_json=url,
                               url_download=download_url,
                               url_webdav=webdav_url,
                               log_level="INFO")
    ine_files = AsyncUpdater(ine_config, workers=2)
    ine_files.run_loop()


if __name__ == '__main__':
    main()
