import pandas as pd
import unidecode
import numpy as np
from bs4 import BeautifulSoup
import requests


SERVER = 'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/'

region_name = {1: 'Tarapacá',
               2: 'Antofagasta',
               3: 'Atacama',
               4: 'Coquimbo',
               5: 'Valparaíso',
               6: 'O’Higgins',
               7: 'Maule',
               8: 'Biobío',
               9: 'Araucanía',
               10: 'Metropolitana',
               11: 'Los Ríos',
               12: 'Los Lagos',
               13: 'Aysén',
               14: 'Magallanes',
               15: 'Arica y Parinacota',
               16: 'Ñuble'}

def get_cuarentenas():
    url = 'producto29/Cuarentenas-Totales.csv'
    df = pd.read_csv(SERVER+url, index_col='ID')

    # Normalize comuna name
    df['Nombre'] = df.apply(lambda x: unidecode.unidecode(x['Nombre']).lower(), 1)
    df['Fecha de Inicio'] = pd.to_datetime(df['Fecha de Inicio'])
    df['Fecha de Término'] = pd.to_datetime(df['Fecha de Término'])

    return df

def get_population():
    population = pd.read_csv('Domain/data/pob_chile.csv')
    population['Comuna'] = population.apply(lambda x: unidecode.unidecode(x['Comuna']).lower(), 1)
    return population

def get_IPC():
    url = 'https://datosmacro.expansion.com/ipc-paises/chile'
    html_page = requests.get(url)
    soup = BeautifulSoup(html_page.text, 'html.parser') # Instanciamos nuestro scrapper

    items = soup.find('table', attrs={'id':'tb1_412'})
    ipc_frames = []
    for item in items.find_all('tr'):
        a_item = item.find('a')
        if a_item is not None:
            url_item = 'https://datosmacro.expansion.com'+a_item['href']
            name = a_item.text[:-3].strip()
            if '<' in name: continue

            html_page_in = requests.get(url_item)
            soup_in = BeautifulSoup(html_page_in.text, 'html.parser')
            table_in = soup_in.find('table', attrs={'id':'tb1_412',
            'class':"table tabledat table-striped table-condensed table-hover"})

            dates_list = []
            names_list = []
            value_list = []
            for item_in in table_in.find_all('tr'):

                try:
                    date = item_in.find('td', attrs={'class': 'fecha'})['data-value']
                    dates_list.append(date)
                    values = []
                    for iitem in item_in.find_all('td', attrs={'class': 'numero'}):
                        v = iitem['data-value']
                        values.append(v)

                    names_list.append(name)
                    value_list.append(values)
                except:
                    continue

            df = pd.DataFrame(np.array(value_list), columns=['interanual', 'acum enero', 'var mensual'])
            df['fecha'] = dates_list
            df['item'] =names_list
            ipc_frames.append(df)

    all_frames = pd.concat(ipc_frames)
    return all_frames

def get_acciones():
    from selenium import webdriver
    from selenium.common.exceptions import NoSuchElementException
    from selenium.webdriver.common.keys import Keys
    browser = webdriver.Firefox()
    url = 'https://es.investing.com/equities/chile'
    browser.get(url)
    option = browser.find_element_by_id('all')
    option.click()
    option = browser.find_element_by_id('cross_rate_markets_stocks_1')
    print(option)
    html_source = browser.page_source
    browser.quit()
    with open('partial.txt', 'w') as handle:
        handle.write(html_source)

    soup = BeautifulSoup(html_source,'html.parser')
    find = soup.find('table', attrs={'class': 'genTbl closedTbl crossRatesTbl elpTbl elp25'})
