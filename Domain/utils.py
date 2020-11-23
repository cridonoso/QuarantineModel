import pandas as pd
import unidecode


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
