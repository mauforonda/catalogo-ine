#!/usr/bin/env python3

import requests
import pandas as pd
from bs4 import BeautifulSoup
import datetime as dt
import xmltodict
import pytz
import locale
import time

def parse_page(page):
    """
    Recolecta enlaces a files en nube.ine.gob.bo del contenido de una página
    """
    pageid = page['id']
    created = page['date']
    modified = page['modified']
    link = page['link']
    title = page['title']['rendered']
    content = BeautifulSoup(page['content']['rendered'], 'html.parser')
    return [{'pageid':pageid, 'pagecreated': created, 'pagemodified':modified, 'pagelink': link, 'pagetitle': title, 'link': a['href'], 'name': a.get_text()} for a in content.select('a') if a.has_attr('href') and 'nube.ine.gob.bo' in a['href']]

def get_links(offset=0, per_page=20):
    """
    Explora todas las páginas disponibles en ine.gob.bo y recolecta sus enlaces relevantes en la variable `sharelinks`
    """

    print("Recolectar enlaces")
    url = 'https://www.ine.gob.bo/wp-json/wp/v2/pages?orderby=modified&per_page={}&offset={}'

    while True:
        print('offset: {}'.format(offset))
        time.sleep(.1)
        r = requests.get(url.format(per_page, offset))
        offset += per_page
        if r.status_code != 200:
            break
        else:
            r = r.json()
            for page in r:
                sharelinks.extend(parse_page(page))
            if len(r) < per_page:
                break

def format_sharelinks(sharelinks):
    """
    Crea un dataframe con resultados de la recolección de enlaces
    """
    
    sharedf = pd.DataFrame(sharelinks)
    sharedf = sharedf.drop_duplicates(subset=['link'])
    sharedf['token'] = sharedf.link.apply(lambda x: x.replace('/download', '').split('/')[-1])
    sharedf = sharedf.sort_values('pagecreated').reset_index(drop=True)
    return sharedf

def get_filemeta(share_token):
    """
    Consulta metadatos para un file en nube.ine.gob.bo según su token
    """
    
    s = requests.Session()
    s.auth = ('admin', 'admin')
    r = s.request(method='PROPFIND', url='https://nube.ine.gob.bo/public.php/webdav', auth=(share_token, ''))
    
    metadata = xmltodict.parse(r.text)
    if metadata.__contains__('d:multistatus'):
        metadata = metadata['d:multistatus']['d:response']['d:propstat']['d:prop']
        metadata = {k.replace('d:', ''): metadata[k] for k in metadata.keys()}
        metadata['getlastmodified'] = dt.datetime.strptime(metadata['getlastmodified'], '%a, %d %b %Y %H:%M:%S %Z')
        metadata['getcontentlength'] = int(metadata['getcontentlength'])
        metadata['getetag'] = metadata['getetag'].replace('"', '')
        return metadata
    else:
        return None

def catalogo_ine(sharedf, offset=0):
    """
    Consulta metadatos para todos los files en sharedf y los almacena en la variable `catalogo`
    """
    
    sharedfi = sharedf.iloc[offset:]
    total = len(sharedf)
    print("Consultar metadatos de enlaces")
    for i, row in sharedfi.iterrows():
        print('{}/{}'.format(i, total))
        time.sleep(0.1)
        metadata = get_filemeta(row['token'])
        if metadata != None:
            catalogo.append({**row.to_dict(), **metadata})

def format_catalogo(catalogo):
    """
    Crea un dataframe con los metadatos en el catálogo
    """
    
    tipomap = {
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'spreadsheet',
        'application/pdf': 'pdf',
        'application/vnd.ms-excel': 'excel',
        'application/x-rar-compressed': 'rar',
        'application/zip': 'zip',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'word',
        'application/octet-stream': 'stream'
    }

    catdf = pd.DataFrame(catalogo)
    catdf = catdf[['name', 'token', 'getlastmodified', 'getcontentlength', 'getcontenttype']]
    catdf.columns = ['nombre', 'token', 'modificado', 'largo', 'tipo']

    catdf.insert(2, 'link', catdf.token.apply(lambda x: 'https://nube.ine.gob.bo/index.php/s/{}/download'.format(x)))
    catdf.insert(3, 'kb', catdf.largo / 1024)
    catdf.drop(columns=['largo'], inplace=True)
    catdf.loc[:,'tipo'] = catdf.tipo.map(tipomap)
    catdf['modificado'] = catdf.set_index('modificado').tz_localize("UTC").index.tz_convert('America/La_Paz')
    catdf = catdf.sort_values('modificado', ascending=False)
    
    return catdf

def save_catalogo(catdf):
    """
    Guarda resultados
    """
    
    old = pd.read_csv('catalogo_ine.csv', parse_dates=['modificado'])
    catdf = catdf[['modificado', 'nombre', 'tipo', 'kb', 'link']]
    catdf_historial = pd.concat([old, catdf]).drop_duplicates(subset=['modificado', 'nombre', 'tipo', 'link'], keep='first')
    catdf_historial.sort_values('modificado').to_csv('catalogo_ine_historial.csv', index=False, float_format="%.2f")
    catdf.sort_values('modificado').to_csv('catalogo_ine.csv', index=False, float_format="%.2f")


# Coleccionar enlaces a files
sharelinks = []
get_links()
sharedf = format_sharelinks(sharelinks)

# Consultar metadatos para cada file
catalogo = []
catalogo_ine(sharedf)
catdf = format_catalogo(catalogo)
save_catalogo(catdf)
