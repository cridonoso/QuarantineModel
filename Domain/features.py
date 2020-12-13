import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import unidecode
from datetime import datetime


SERVER = 'https://raw.githubusercontent.com/MinCiencia/Datos-COVID19/master/output/'
NODATA = -99

def casos_totales_por_comuna_cumulativo(fecha='2020-03-30',
                                        comuna='Concepcion',
                                        region='Biobio'):
    url = 'producto1/Covid-19.csv'
    names  = ['Confirmados Comuna Acumulados']
    names  = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Region')
    df['Comuna'] = df.apply(lambda x: unidecode.unidecode(x['Comuna']).lower(), 1)
    df_comuna = df[df['Comuna'] == comuna]

    if fecha in df_comuna.columns.values and df_comuna.shape[0]>0:
        df_comuna.fillna(NODATA, inplace=True)
        values = [df_comuna[fecha].values[0]]
    else:
        values =  [NODATA]*len(names)

    assert len(values)==len(names)
    return values, names

def casos_totales_por_comuna(fecha='2020-03-30',
                             comuna='Concepcion',
                             region='Biobio'):
    url = 'producto2/{}-CasosConfirmados.csv'.format(fecha)
    names = ['Confirmados Comuna']
    names = [unidecode.unidecode(x) for x in names]

    try:
        df = pd.read_csv(SERVER+url, index_col='Region')
        df['Comuna'] = df.apply(lambda x: unidecode.unidecode(x['Comuna']).lower(), 1)
        df.fillna(NODATA, inplace=True)
        df = df[df['Comuna'] == comuna]
        values = list(df['Casos Confirmados'].values)
        if len(values) == 0:
            values = [NODATA]*len(names)
    except Exception as e:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 45 does not match length {}'.format(comuna)

    return values, names


def casos_totales_por_region_cumulativo(fecha='2020-03-30',
                                        comuna='Concepcion',
                                        region='Biobio'):
    url = 'producto3/CasosTotalesCumulativo.csv'
    names = ['Confirmados Region Acumulados']
    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Region')
    if fecha in df.columns:
        df.fillna(NODATA, inplace=True)
        df.reset_index(inplace=True)
        values = list(df[df['Region'] == region][fecha].values)
    else:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 64 does not match length'
    return values, names


# def casos_totales_por_region(fecha='2020-03-03',
#                              comuna='Concepcion',
#                              region='Biobio'):
#     url = 'producto4/{}-CasosConfirmados-totalRegional.csv'.format(fecha)
#     names = ['Casos Nuevos Region', 'Confirmados Region',
#              'Recuperados Region', 'Casos Fallecidos Region']
#
#     try:
#         df_ = pd.read_csv(SERVER+url, index_col='Region')
#         df_.fillna(NODATA, inplace=True)
#         df_.reset_index(inplace=True)
#         df = df_[df_['Region'] == region]
#         if df.shape[0] == 0:
#             df = df_[df_['Region'] == unidecode.unidecode(region)]
#
#         # NOT FINISHED YET
#         # tables do not match;
#         # after certain date they change col names and their values
#
#         return values, [unidecode.unidecode(x) for x in names]
#     except Exception as e:
#         print(e)
#         return [NODATA]*len(names), [unidecode.unidecode(x) for x in names]

def casos_totales_nacional(fecha='2020-03-02',
                           comuna='Concepcion',
                           region='Biobio'):
    url = 'producto5/TotalesNacionales.csv'
    names = ['Nacional nuevos con sintomas', 'Nacional totales',
             'Nacional recuperados',         'Nacional Fallecidos',
             'Nacional activos',             'Nacional nuevos sin sintomas',
             'Nacional nuevos totales',      'Nacional activos por FD',
             'Nacional activos por FIS',     'Nacional recuperados por FIS',
             'Nacional recuperados por FD',  'Nacional confirmados recuperados',
             'Nacional activos confirmados', 'Nacional probables acumulados',
             'Nacional activos probables',   'Nacional nuevos sin notificar',
             'Nacional activos confirmados']
    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Fecha')
    if fecha in df.columns:
        df.reset_index(inplace=True)
        df.fillna(NODATA, inplace=True)
        values = list(df[fecha].values)
    else:
        values = [NODATA]*len(name)

    assert len(values)==len(names), 'Line 114 does not match length'
    return values, names


def PCR_por_region(fecha='2020-03-02',
                   comuna='Concepcion',
                   region='Biobio'):
    url = 'producto7/PCR.csv'
    names = ['# PCR Region']
    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Region')
    if fecha in df.columns:
        df.fillna(NODATA, inplace=True)
        df.reset_index(inplace=True)
        df = df[df['Region'] == region][fecha]
        values = list(df.values)
    else:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 140 does not match length'
    return values, names

def UCI_por_region(fecha='2020-03-02',
                   comuna='Concepcion',
                   region='Biobio'):
    url = 'producto8/UCI.csv'
    names = ['# UCI Region']
    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Region')
    if fecha in df.columns:
        df.fillna(NODATA, inplace=True)
        df.reset_index(inplace=True)
        df = df[df['Region'] == region][fecha]
        values = list(df.values)
    else:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 159 does not match length'
    return values, names

def UCI_etario(fecha='2020-03-02',
               comuna='Concepcion',
               region='Biobio'):
    url = 'producto9/HospitalizadosUCIEtario.csv'
    names = ['UCI Nacional <=39', 'UCI Nacional 40-49', 'UCI Nacional 50-59',
             'UCI Nacional 60-69', 'UCI Nacional >=70']
    names = [unidecode.unidecode(x) for x in names]
    df = pd.read_csv(SERVER+url, index_col='Grupo de edad')

    if fecha in df.columns:
        df.reset_index(inplace=True)
        values = list(df[fecha].values)
    else:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 177 does not match length'
    return values, names

def fallecidos_etario(fecha='2020-03-02',
                      comuna='Concepcion',
                      region='Biobio'):
    url = 'producto10/FallecidosEtario.csv'
    names = ['Fallecidos Nacional <=39', 'Fallecidos Nacional 40-49',
    'Fallecidos Nacional 50-59', 'Fallecidos Nacional 60-69',
    'Fallecidos Nacional 70-79', 'Fallecidos Nacional 80-89',
    'Fallecidos Nacional >=90']
    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Grupo de edad')
    if fecha in df.columns:
        df.reset_index(inplace=True)
        values = list(df[fecha].values)
    else:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 197 does not match length'
    return values, names

def casos_nuevos_por_region_cumulativo(fecha='2020-03-02',
                                       comuna='Concepcion',
                                       region='Biobio'):
    url ='producto13/CasosNuevosCumulativo.csv'
    names = ['Casos Nuevos Reg Cum']
    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Region')
    if fecha in df.columns:
        df.fillna(NODATA, inplace=True)
        df.reset_index(inplace=True)
        df = df[df['Region'] == region][fecha]
        values =  list(df.values)
    else:
        values =  [NODATA]*len(names)

    assert len(values)==len(names), 'Line 216 does not match length'
    return values, names

def fallecidos_por_region_cumulativo(fecha='2020-03-02',
                                     comuna='Concepcion',
                                     region='Biobio'):
    url = 'producto14/FallecidosCumulativo.csv'
    names = ['Fallecidos Region Cum']
    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Region')
    if fecha in df.columns:
        df.fillna(NODATA, inplace=True)
        df.reset_index(inplace=True)
        df = df[df['Region'] == region][fecha]
        values = list(df.values)
    else:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 235 does not match length'
    return values, names


def ventiladores_nacional(fecha='2020-03-02',
                                     comuna='Concepcion',
                                     region='Biobio'):
    url = 'producto20/NumeroVentiladores.csv'
    names = ['Ventiladores Total Nacional', 'Ventinladores Disponibles Nacional',
            'Ventinladores Ocupados Nacional']
    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url)

    try:
        df.fillna(NODATA, inplace=True)
        values = list(df[fecha].values)
    except:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 255 does not match length'
    return values, names

def movilidad_comuna(fecha='2020-03-02',
                                     comuna='Concepcion',
                                     region='Biobio'):
    urls = ['producto33/IndiceDeMovilidad-IM.csv',
          'producto33/IndiceDeMovilidad-IM_externo.csv ',
          'producto33/IndiceDeMovilidad-IM_interno.csv']

    names = ['IM', 'IM interno', 'IM externo', 'Superficie (km2)', 'Poblacion']
    names = [unidecode.unidecode(x) for x in names]

    def step(single_url):
        try:
            df = pd.read_csv(SERVER+single_url, index_col='Region')
            df['Comuna'] = df.apply(lambda x: unidecode.unidecode(x['Comuna']).lower(), 1)
            df.fillna(NODATA, inplace=True)
            df = df[df['Comuna'] == comuna]
            value  = df[fecha].values[0]
            return value
        except Exception as e:
            # print(e)
            value = NODATA
            return value

    values = []
    for link in urls:
        partial = step(link)
        values.append(partial)

    try:
        df = pd.read_csv(SERVER+urls[0], index_col='Region')
        df['Comuna'] = df.apply(lambda x: unidecode.unidecode(x['Comuna']).lower(), 1)
        df.fillna(NODATA, inplace=True)
        df = df[df['Comuna'] == comuna]
        sup_pob = list(df[['Superficie_km2', 'Poblacion']].values[0])
        values+=sup_pob
    except Exception as e:
        # print(e)
        values+=[NODATA]*2

    assert len(values)==len(names), 'Line 297 does not match length {}'.format(comuna)

    return values, names

def fallecidos_por_comuna(fecha='2020-03-30',
                             comuna='Concepcion',
                             region='Biobio'):
    url = 'producto38/CasosFallecidosPorComuna.csv'
    names = ['Fallecidos Comuna']
    names = [unidecode.unidecode(x) for x in names]

    try:
        df = pd.read_csv(SERVER+url, index_col='Region')
        df['Comuna'] = df.apply(lambda x: unidecode.unidecode(x['Comuna']).lower(), 1)
        df.fillna(NODATA, inplace=True)
        df = df[df['Comuna'] == comuna]
        values = df[fecha].values
        if len(values) == 0:
            values = [NODATA]*len(names)

    except Exception as e:
        # print(e)
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 321 does not match length {}'.format(comuna)

    return values, names

def disponibilidad_camas_UCi_region(fecha='2020-03-30',
                             comuna='Concepcion',
                             region='Biobio'):
    url = 'producto52/Camas_UCI.csv'
    names = ['Camas UCI Region habilitadas',
             'Camas UCI Region ocupadas COVID-19',
             'Camas UCI Region ocupadas no COVID-19',
             'Camas base Region (2019)']

    names = [unidecode.unidecode(x) for x in names]

    df = pd.read_csv(SERVER+url, index_col='Region')
    if fecha in df.columns:
        df.fillna(NODATA, inplace=True)
        df.reset_index(inplace=True)
        df = df[df['Region'] == region]
        df = df[fecha]
        values = list(df.values)
    else:
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 346 does not match length'
    return values, names

def positividad_por_comuna(fecha='2020-03-30',
                           comuna='Concepcion',
                           region='Biobio'):
    url = 'producto65/PositividadPorComuna.csv'
    names = ['Positividad Semanal Comuna']
    names = [unidecode.unidecode(x) for x in names]

    try:
        df = pd.read_csv(SERVER+url, index_col='Region')
        df['Comuna'] = df.apply(lambda x: unidecode.unidecode(x['Comuna']).lower(), 1)
        df.fillna(NODATA, inplace=True)
        df = df[df['Comuna'] == comuna]
        df = df.iloc[:, 4:].transpose()

        df = df.loc[(df.index >= fecha)].iloc[0]
        values = df.values
        if len(values) == 0:
            values = [NODATA]*len(names)

    except Exception as e:
        # print(e)
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 372 does not match length {}'.format(comuna)
    return values, names

def valor_dolar(fecha='2020-03-30',
                comuna='Concepcion',
                region='Biobio'):
    path = 'Domain/data/Datos históricos USD_CLP.csv'
    names = ['USD valor']
    names = [unidecode.unidecode(x) for x in names]

    try:
        df = pd.read_csv(path)
        df.fillna(NODATA, inplace=True)
        def fn(fecha):
            date_split = fecha.split('.')
            date_ = pd.DataFrame({'year': [date_split[2]],
                                  'month': [date_split[1]],
                                  'day': [date_split[0]]})
            return pd.to_datetime(date_)

        df['Fecha'] = df.apply(lambda x: fn(x['Fecha']), 1)
        df = df[df['Fecha'] == fecha]['Último']
        df = df.values[0].replace(',', '.')
        df = float(df)
        values = [df]

        if len(values) == 0:
            values = [NODATA]*len(names)

    except Exception as e:
        # print(e)
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 405 does not match length {}'.format(comuna)

    return values, names

def positividad_por_comuna(fecha='2020-03-30',
                           comuna='Concepcion',
                           region='Biobio'):
    url = 'producto65/PositividadPorComuna.csv'
    names = ['Positividad Semanal Comuna']
    names = [unidecode.unidecode(x) for x in names]

    try:
        df = pd.read_csv(SERVER+url, index_col='Region')
        df['Comuna'] = df.apply(lambda x: unidecode.unidecode(x['Comuna']).lower(), 1)
        df.fillna(NODATA, inplace=True)
        df = df[df['Comuna'] == comuna]
        df = df.iloc[:, 4:].transpose()

        df = df.loc[(df.index >= fecha)].iloc[0]
        values = df.values
        if len(values) == 0:
            values = [NODATA]*len(names)

    except Exception as e:
        # print(e)
        values = [NODATA]*len(names)

    assert len(values)==len(names), 'Line 432 does not match length {}'.format(comuna)
    return values, names


def IPC_mensual(fecha='2020-03-30',
                comuna='Concepcion',
                region='Biobio'):

    url = 'Domain/data/ipc.csv'
    df = pd.read_csv(url)
    fecha_mensual = '2020-{}-01'.format(fecha.split('-')[1])

    df = df[df.fecha==fecha_mensual]

    all_names = []
    all_values = []
    for n, v in zip(df['item'], df.values):
        cc = ['IPC '+n+' '+k if 'IPC' not in n else n+' '+k for k in df.columns[:-1]]
        values = list(v[:-2])
        names = list(cc[:-1])
        all_names.append(names)
        all_values.append(values)

    flat_names = [unidecode.unidecode(nn) for n in all_names for nn in n]
    flat_values = [nn for n in all_values for nn in n]

    assert len(flat_names)==len(flat_values), 'Line 458 does not match length {}'.format(comuna)
    return flat_values, flat_names
