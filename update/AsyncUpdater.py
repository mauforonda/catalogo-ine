#!/bin/env python3.10

# Thanks to https://github.com/mauforonda/catalogo-ine
# and https://github.com/talkpython/async-techniques-python-course

# This Module retrieves a (possibly generic) list of Wordpress site JSON files
# asynchonously and gathers the file modification times and sizes
# using the WP Webdav endpoint provided.

# Todos: 
# Handle Exceptions, more precise timing tests
# Debug asyncio, optimize memory and try different Event Loops frameworks
#
# Test time: 1000 pages, 1264 file metadata scanned in 38 seconds.

import os
import asyncio
import random
from typing import List, Dict, Union, Tuple
from pydantic import HttpUrl, AnyHttpUrl
import httpx
import pandas as pd
from bs4 import BeautifulSoup
import datetime as dt
import xmltodict

MAX_PAGES = 1000
TIMEOUT = 20
TIPO_MAP = {
    'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'spreadsheet',
    'application/pdf': 'pdf',
    'application/vnd.ms-excel': 'excel',
    'application/x-rar-compressed': 'rar',
    'application/zip': 'zip',
    'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'word',
    'application/octet-stream': 'stream'
}


class AsyncUpdater:
    loop: asyncio.AbstractEventLoop
    client: httpx.AsyncClient
    url: Union[HttpUrl, AnyHttpUrl]
    domain: Union[HttpUrl, AnyHttpUrl]
    webdav_url: Union[HttpUrl, AnyHttpUrl]
    download_url: Union[HttpUrl, AnyHttpUrl]
    get_tasks: List[asyncio.Task]
    meta_tasks: List[asyncio.Task]
    pages: asyncio.Queue
    catalog: List[Dict]
    df: pd.DataFrame
    flag: asyncio.Event
    time_zone: str
    csv_file: str

    def __init__(
            self,
            u,
            d,
            w,
            du,
            time_zone="America/La_Paz",
            csv_file="catalogo_ine.csv"
    ) -> None:
        self.loop = asyncio.new_event_loop()
        self.url = u
        self.domain = d
        self.webdav_url = w
        self.download_url = du
        self.get_tasks = []
        self.meta_tasks = []
        self.pages = asyncio.Queue()
        self.catalog = []
        self.client = httpx.AsyncClient()
        self.offsets = range(0, MAX_PAGES, 20)
        self.time_zone = time_zone
        self.csv_file = csv_file

    async def close(self):
        await self.client.aclose()

    def __del__(self):
        """
        A destructor is provided to ensure that the client and the event loop are closed at exit.
        """
        # Use the loop to call async close, then stop/close loop.
        self.loop.run_until_complete(self.close())
        self.loop.close()

    def format_catalog(self) -> None:
        """
        Crea un dataframe con los metadatos en el catálogo
        """

        self.df = pd.DataFrame(self.catalog)
        self.df = self.df[['name', 'token', 'getlastmodified', 'getcontentlength', 'getcontenttype']]
        self.df.columns = ['nombre', 'token', 'modificado', 'largo', 'tipo']

        self.df.insert(2, 'link',
                       self.df.token.apply(lambda x: self.download_url.format(x)))
        self.df.insert(3, 'kb', self.df.largo / 1024)
        self.df.drop(columns=['largo'], inplace=True)
        self.df.loc[:, 'tipo'] = self.df.tipo.map(TIPO_MAP)
        self.df['modificado'] = self.df.set_index('modificado').tz_localize("UTC").index.tz_convert(self.time_zone)
        self.df = self.df.sort_values('modificado', ascending=False)
        return

    def save_csv(self, file_name: str = None):
        """
        Guarda resultados
        """
        if file_name:
            file = file_name
        else:
            file = self.csv_file

        catalog = self.df[['modificado', 'nombre', 'tipo', 'kb', 'link']]
        if os.path.exists(file):
            old = pd.read_csv(file, parse_dates=['modificado'])
            catdf_historial = pd.concat([old, catalog]).drop_duplicates(subset=['modificado', 'nombre', 'tipo', 'link'],
                                                                        keep='first')
            catdf_historial.sort_values('modificado').to_csv(
                '_'.join([file[:file.index('.')], 'historial']) + '.csv',
                index=False, float_format="%.2f")
        catalog.sort_values(['modificado', 'link']).to_csv(file, index=False, float_format="%.2f")

    def parse_pages(self, pages: Dict) -> Union[List[Dict], None]:
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
                          if a.has_attr('href') and self.domain in a['href']]
            if not file_links:
                continue
            pre_frame.extend(file_links)
        return pre_frame if pre_frame else None

    async def get_links(self, offset: int = 0, per_page: int = 20) -> Tuple[int, Union[Dict, None]]:
        """
        Explora asincronamente la página solicitada y devuelve el json
        """
        r = await self.client.get(self.url.format(per_page, offset))
        await asyncio.sleep(random.random())
        if r.status_code != 200:
            err = f"Offset {offset} failed to download with code {r.status_code}"
            raise Exception(err)
            # we could raise an Exception for gather
        r = r.json()
        if not r or len(r) < per_page:
            return offset, None
        return offset, r

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

    async def get_file_meta(self, frame: pd.DataFrame, offset: int, task_num: str) -> None:
        """
        Consulta metadatos para un file en URL webdav según su token
        """
        token = frame.iloc[0].token
        r = await self.client.request(method='PROPFIND',
                                      url=self.webdav_url,
                                      auth=(token, ''),
                                      timeout=TIMEOUT)

        metadata = xmltodict.parse(r.text)
        if metadata.__contains__('d:multistatus'):
            metadata = metadata['d:multistatus']['d:response']['d:propstat']['d:prop']
            metadata = {k.replace('d:', ''): metadata[k] for k in metadata.keys()}
            metadata['getlastmodified'] = dt.datetime.strptime(metadata['getlastmodified'], '%a, %d %b %Y %H:%M:%S %Z')
            metadata['getcontentlength'] = int(metadata['getcontentlength'])
            metadata['getetag'] = metadata['getetag'].replace('"', '')
            row = {**frame.iloc[0].to_dict(), **metadata}
            self.catalog.append(row)
        if bool(random.getrandbits(1)):
            await asyncio.sleep(random.random())

    async def build_catalog(self) -> None:
        for offset in self.offsets:
            self.get_tasks.append(
                asyncio.create_task(
                    (self.get_links(offset=offset)),
                    name=f"Get Links at Offset {offset}"))

        for outcome in await asyncio.gather(*self.get_tasks, return_exceptions=True):
            if isinstance(outcome, Exception):
                continue
            offset, result = outcome
            if result is None:
                continue
            parsed_file_links = self.parse_pages(result)
            if not parsed_file_links:
                continue
            frame = self.format_frame(parsed_file_links)
            rows = len(frame.index)
            for i in range(rows):
                task_num = f"{i}/{rows}"
                self.meta_tasks.append(asyncio.create_task(self.get_file_meta(frame.iloc[[i]], offset, task_num)))
            await asyncio.gather(*self.meta_tasks, return_exceptions=True)
            self.format_catalog()

    def run_loop(self) -> None:
        self.loop.run_until_complete(self.build_catalog())


def main():
    url = 'https://www.ine.gob.bo/wp-json/wp/v2/pages?orderby=modified&per_page={}&offset={}'
    webdav_url = 'https://nube.ine.gob.bo/public.php/webdav'
    download_url = 'https://nube.ine.gob.bo/index.php/s/{}/download'
    domain = 'nube.ine.gob.bo'
    ine_files = AsyncUpdater(url, domain, webdav_url, download_url)
    ine_files.run_loop()
    ine_files.save_csv()


if __name__ == '__main__':
    main()
