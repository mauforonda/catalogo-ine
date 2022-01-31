#!/usr/bin/env python3

import requests
import pandas as pd
import datetime as dt
import pytz
import locale
import argparse

def base_url():
    parser = argparse.ArgumentParser()
    parser.add_argument("url", type=str)
    args = parser.parse_args()
    return args.url

def update_catalogo(base):
    """
    Actualiza el catálogo
    """

    with open('update_time', 'w+') as f:
        f.write(requests.get('{}/update_time'.format(base)).text)
    catdf = pd.read_csv('{}/catalogo_ine.csv'.format(base), parse_dates=['modificado'])
    old = pd.read_csv('catalogo_ine.csv', parse_dates=['modificado'])
    newlines = len(pd.concat([df.sort_values(['link']) for df in [catdf, old]]).drop_duplicates(keep=False))
    if newlines > 0:
        catdf = catdf[['modificado', 'nombre', 'tipo', 'kb', 'link']]
        old_historial = pd.read_csv('catalogo_ine_historial.csv', parse_dates=['modificado'])
        catdf_historial = pd.concat([old_historial, catdf]).drop_duplicates(subset=['modificado', 'nombre', 'tipo', 'link'], keep='first')
        catdf_historial.sort_values(['modificado', 'link']).to_csv('catalogo_ine_historial.csv', index=False, float_format="%.2f")
        catdf.sort_values(['modificado', 'link']).to_csv('catalogo_ine.csv', index=False, float_format="%.2f")

base = base_url()
update_catalogo(base)
