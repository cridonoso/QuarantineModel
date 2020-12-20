import multiprocessing
import pandas as pd
import numpy as np

from Application.Extractor import Extractor
from Domain import features, utils
from joblib import Parallel, delayed


class OnlyCovidExtractor(Extractor):
    """ AlphaExtractor """

    def __init__(self,
                 **kwargs):
        """ Alpha Extractor

        Root class attributes
        ----------
        start_date
        end_date
        quarantines
        comuna_to_region
        population

        """
        super(OnlyCovidExtractor, self).__init__(**kwargs)
        self.features_list = [features.casos_totales_por_comuna,
                              features.casos_totales_por_comuna_cumulativo,
                              features.casos_totales_por_region_cumulativo,
                              features.casos_totales_nacional,
                              features.PCR_por_region,
                              features.UCI_por_region,
                              features.UCI_etario,
                              features.fallecidos_etario,
                              features.casos_nuevos_por_region_cumulativo,
                              features.fallecidos_por_region_cumulativo,
                              features.ventiladores_nacional,
                              features.movilidad_comuna,
                              features.fallecidos_por_comuna,
                              features.disponibilidad_camas_UCi_region,
                              features.positividad_por_comuna]

    def get_features(self, date, comuna, region):
        num_cores = multiprocessing.cpu_count()
        N = self.population[self.population['Comuna'] == comuna]['Poblacion'].values

        if N.shape[0] == 0:
            return [], []

        response = Parallel(n_jobs=num_cores)(delayed(fn)(date, comuna, region) \
                            for fn in self.features_list)

        values = np.concatenate([x[0] for x in response])
        feat_names = np.concatenate([x[1] for x in response])
        return values, feat_names


    def get_label(self, date, comuna):
        comuna_df = self.quarantines[self.quarantines['Nombre'] == comuna]
        cond = (date >= comuna_df['Fecha de Inicio']) & (date <= comuna_df['Fecha de Término'])
        y = int(cond.any())
        return y


class AlphaExtractor(Extractor):
    """ AlphaExtractor """

    def __init__(self,
                 **kwargs):
        """ Alpha Extractor

        Root class attributes
        ----------
        start_date
        end_date
        quarantines
        comuna_to_region
        population

        """
        super(AlphaExtractor, self).__init__(**kwargs)
        self.features_list = [features.casos_totales_por_comuna,
                              features.casos_totales_por_comuna_cumulativo,
                              features.casos_totales_por_region_cumulativo,
                              features.casos_totales_nacional,
                              features.PCR_por_region,
                              features.UCI_por_region,
                              features.UCI_etario,
                              features.fallecidos_etario,
                              features.casos_nuevos_por_region_cumulativo,
                              features.fallecidos_por_region_cumulativo,
                              features.ventiladores_nacional,
                              features.movilidad_comuna,
                              features.fallecidos_por_comuna,
                              features.disponibilidad_camas_UCi_region,
                              features.positividad_por_comuna,
                              features.valor_dolar,
                              features.IPC_mensual,
                              features.SPIPSA,
                              features.SPCLXIGPA]

    def get_features(self, date, comuna, region):
        num_cores = multiprocessing.cpu_count()
        N = self.population[self.population['Comuna'] == comuna]['Poblacion'].values

        if N.shape[0] == 0:
            return [], []

        response = Parallel(n_jobs=num_cores)(delayed(fn)(date, comuna, region) \
                            for fn in self.features_list)

        values = np.concatenate([x[0] for x in response])
        feat_names = np.concatenate([x[1] for x in response])
        return values, feat_names


    def get_label(self, date, comuna):
        comuna_df = self.quarantines[self.quarantines['Nombre'] == comuna]
        cond = (date >= comuna_df['Fecha de Inicio']) & (date <= comuna_df['Fecha de Término'])
        y = int(cond.any())
        return y


class BetaExtractor(Extractor):
    """ AlphaExtractor """

    def __init__(self,
                 **kwargs):
        """ Beta Extractor

        Root class attributes
        ----------
        start_date
        end_date
        quarantines
        comuna_to_region
        population

        """
        super(BetaExtractor, self).__init__(**kwargs)
        self.features_list = [features.casos_totales_por_comuna,
                              features.casos_totales_por_comuna_cumulativo,
                              # features.casos_totales_por_region_cumulativo,
                              # features.casos_totales_nacional,
                              # features.PCR_por_region,
                              # features.UCI_por_region,
                              # features.UCI_etario,
                              # features.fallecidos_etario,
                              # features.casos_nuevos_por_region_cumulativo,
                              # features.fallecidos_por_region_cumulativo,
                              # features.ventiladores_nacional,
                              # features.movilidad_comuna,
                              # features.fallecidos_por_comuna,
                              # features.disponibilidad_camas_UCi_region,
                              # features.positividad_por_comuna,
                              # features.valor_dolar,
                              # features.IPC_mensual,
                              # features.SPIPSA,
                              # features.SPCLXIGPA,
                              ]

    def get_features(self, date, comuna, region):
        num_cores = multiprocessing.cpu_count()
        N = self.population[self.population['Comuna'] == comuna]['Poblacion'].values

        if N.shape[0] == 0:
            return [], []

        response = Parallel(n_jobs=num_cores)(delayed(fn)(date, comuna, region) \
                            for fn in self.features_list)

        values = np.concatenate([x[0] for x in response])
        feat_names = np.concatenate([x[1] for x in response])
        return values, feat_names


    def get_label(self, date, comuna):
        comuna_df = self.quarantines[self.quarantines['Nombre'] == comuna]

        comuna_df['Fecha de Inicio'] = comuna_df.apply(lambda x: x['Fecha de Inicio'].date(), 1)
        comuna_df['Fecha de Término'] = comuna_df.apply(lambda x: x['Fecha de Término'].date(), 1)

        is_first = (comuna_df['Fecha de Inicio'] == date).values[0]
        is_end = (comuna_df['Fecha de Término'] == date).values[0]

        if is_first: return 1
        if is_end: return 0
        return None
