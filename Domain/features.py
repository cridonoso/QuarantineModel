import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import unidecode

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

    assert len(values)==len(names), 'Line 135 does not match length'
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

    assert len(values)==len(names), 'Line 154 does not match length'
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

    assert len(values)==len(names), 'Line 172 does not match length'
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

    assert len(values)==len(names), 'Line 192 does not match length'
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

    assert len(values)==len(names), 'Line 192 does not match length'
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

    assert len(values)==len(names), 'Line 192 does not match length'
    return values, names

# HASTA EL 13
