#!/usr/bin/env python3

import requests
import pandas as pd
from bs4 import BeautifulSoup
import datetime as dt
import xmltodict
import pytz
import locale
import time
import re

SLEEP_T = .01
TIMEOUT = 20

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

def get_links(domain, offset=0, per_page=20):
    """
    Explora todas las páginas disponibles en ine.gob.bo y recolecta sus enlaces relevantes en la variable `sharelinks`
    """

    print("Recolectar enlaces")
    url = 'https://{}/wp-json/wp/v2/pages?orderby=modified&per_page={}&offset={}'

    while True:
        print('offset: {}'.format(offset))
        r = requests.get(url.format(domain, per_page, offset), timeout=TIMEOUT)
        offset += per_page
        if r.status_code != 200:
            break
        else:
            r = r.json()
            for page in r:
                sharelinks.extend(parse_page(page))
            if len(r) < per_page:
                break
            time.sleep(SLEEP_T)

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
    r = s.request(method='PROPFIND', url='https://nube.ine.gob.bo/public.php/webdav', auth=(share_token, ''), timeout=TIMEOUT)
    
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
        time.sleep(SLEEP_T)
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
    catdf = catdf[['pagetitle', 'pagelink', 'name', 'token', 'getlastmodified', 'getcontentlength', 'getcontenttype']]
    catdf.columns = ['pagetitle', 'pagelink', 'nombre', 'token', 'modificado', 'largo', 'tipo']

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

    catdf[['pagetitle', 'pagelink', 'modificado', 'nombre', 'tipo', 'kb', 'link']].sort_values(['modificado', 'link']).to_csv('catalogo_ine.csv', index=False, float_format="%.2f")
    with open('update_time', 'w+') as f:
        f.write(dt.datetime.now(pytz.timezone('America/La_Paz')).strftime('%Y-%m-%d %H:%M'))
    
    # old = pd.read_csv('catalogo_ine.csv', parse_dates=['modificado'])
    # newlines = len(pd.concat([df.sort_values(['link']) for df in [catdf, old]]).drop_duplicates(keep=False))
    # print("{} nuevas líneas".format(newlines))
    # if newlines > 0:
    #     catdf = catdf[['modificado', 'nombre', 'tipo', 'kb', 'link']]
    #     catdf_historial = pd.concat([old, catdf]).drop_duplicates(subset=['modificado', 'nombre', 'tipo', 'link'], keep='first')
    #     catdf_historial.sort_values('modificado').to_csv('catalogo_ine_historial.csv', index=False, float_format="%.2f")
    #     catdf.sort_values('modificado').to_csv('catalogo_ine.csv', index=False, float_format="%.2f")

def update_data():
    catdf = pd.read_csv('catalogo_ine.csv', parse_dates=['modificado'])
    newlines = len(pd.concat([df.sort_values(['link']) for df in [catdf, old]]).drop_duplicates(keep=False))
    if newlines > 0:
        catdf = catdf[['pagetitle', 'pagelink', 'modificado', 'nombre', 'tipo', 'kb', 'link']]
        old_historial = pd.read_csv('catalogo_ine_historial.csv', parse_dates=['modificado'])
        catdf_historial = pd.concat([old_historial, catdf[['modificado', 'nombre', 'tipo', 'kb', 'link']]]).drop_duplicates(subset=['modificado', 'nombre', 'tipo', 'link'], keep='first')
        catdf_historial.sort_values(['modificado', 'link']).to_csv('catalogo_ine_historial.csv', index=False, float_format="%.2f")
        catdf.sort_values(['modificado', 'link']).to_csv('catalogo_ine.csv', index=False, float_format="%.2f")
    
def get_pagename(url):
    
    response = requests.get('https://www.ine.gob.bo/wp-json/wp/v2/pages?search={}'.format(url))
    pagename = response.json()[0]['title']['rendered']
    return pagename

def get_options(url):
    
    response = requests.get(url)
    html = BeautifulSoup(response.text, 'html.parser')
    options = [{'name': o.get_text(), 'value': o['value']} for o in html.select('option')]
    
    return options

def get_filepath(url, option_value):

    data = {
      'proyecto': option_value,
      'ajax': ''
    }

    script = requests.post('{}ajax.php'.format(url), data=data)
    path = re.findall('\'(base\/.*)\'', script.text)
    if len(path) > 0:
        return url + path[0]

def format_datetime(modified):
    return dt.datetime.strptime(modified, '%a, %d %b %Y %H:%M:%S %Z').replace(tzinfo=pytz.utc).astimezone(pytz.timezone('America/La_Paz'))

def format_tipo(tipo):
    tipomap = {
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': 'spreadsheet',
        'application/pdf': 'pdf',
        'application/vnd.ms-excel': 'excel',
        'application/x-rar-compressed': 'rar',
        'application/zip': 'zip',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': 'word',
        'application/octet-stream': 'stream'
    }
    return tipomap[tipo]

def format_kb(kb):
    return int(kb) / 1024

def get_files(url):
    files = []
    pagename = get_pagename(url)
    options = get_options(url)
    for option in options:
        filepath = get_filepath(url, option['value'])
        if filepath is not None:
            print(filepath)
            headers = requests.head(filepath).headers
            file = dict(
                pagetitle = pagename,
                pagelink = url,
                modificado = format_datetime(headers['Last-Modified']),
                nombre = option['name'],
                tipo = format_tipo(headers['Content-Type']),
                kb = format_kb(headers['Content-Length']),
                link = filepath
            )
            files.append(file)
    return files

def get_extrafiles(urls):
    allfiles = []
    for url in urls:
        files = get_files(url)
        allfiles.extend(files)
    return pd.DataFrame(allfiles)

def merge_dfs(dfs):
    columns = ['pagetitle', 'pagelink', 'modificado', 'nombre', 'tipo', 'kb', 'link']
    return pd.concat([df[columns] for df in dfs]).sort_values(['modificado', 'link'])

extraurls = ['https://www.ine.gob.bo/wp-integrate/vitacora_es/', 'https://www.ine.gob.bo/wp-integrate/vitacora/']
wordpress_domains = ['censo.ine.gob.bo', 'www.ine.gob.bo']

old = pd.read_csv('catalogo_ine.csv', parse_dates=['modificado'])

# Consultar extra files
extrafiles = get_extrafiles(extraurls)

# Coleccionar enlaces a files
sharelinks = []
for domain in wordpress_domains:
    get_links(domain)
sharedf = format_sharelinks(sharelinks)

# Consultar metadatos para cada file
catalogo = []
catalogo_ine(sharedf)
catdf = format_catalogo(catalogo)
catdf = merge_dfs([extrafiles, catdf])

save_catalogo(catdf)
update_data()
