import multiprocessing
import numpy as np
import pandas as pd
import pickle

from joblib import Parallel, delayed
from datetime import datetime, timedelta
from Domain import utils
from tqdm import tqdm


class Extractor:
    """Feature Extractor."""

    def __init__(self,
                 start_date='2020-04-09',
                 end_date=datetime.today()):
        """ Initializer of Feature Extractor

        Parameters
        ----------
        features : list
            list of functions to be executed
        comuna_to_region: Dictonary
            mapping between comunas' name and region numbers

        """
        super(Extractor, self).__init__()
        self.start_date  = start_date
        self.end_date    = end_date

        self.quarantines = utils.get_cuarentenas()
        self.comuna_to_region = pd.read_csv('Domain/data/comuna_region.csv')
        self.population  = utils.get_population()

        self.features    = [] # list of features
        self.labels      = [] # list of labels
        self.comunas     = [] # list of processed comunas
        self.feat_names  = [] # list of processed comunas

    def get_label(self):
        raise NotImplementedError("get_labels method was not implemented")

    def get_features(self):
        raise NotImplementedError("get_features method was not implemented")

    def run_comuna(self, comuna):
        range = pd.date_range(start=self.start_date, end=self.end_date)
        region = self.comuna_to_region[self.comuna_to_region['Comuna'] == comuna]['Region']
        region = utils.region_name[region.values[0]]

        features_comuna = []
        labels_comuna   = []

        for date in tqdm(range, desc='Processing {}'.format(comuna)):
            values, feat_names = self.get_features(str(date.date()),
                                       comuna,
                                       region)
            if len(values) == 0: continue
            label = self.get_label(date, comuna)
            features_comuna.append(values)
            labels_comuna.append(label)

        return features_comuna, labels_comuna, feat_names

    def extract_features(self, n_jobs=1, comuna=None, return_response=False):

        if comuna is not None:
            values, labels, names = self.run_comuna(comuna)
            self.features = values
            self.labels = labels
            self.feat_names = names
        else:
            response = Parallel(n_jobs=n_jobs)(delayed(self.run_comuna)(c) \
                       for c in self.quarantines['Nombre'])

            if return_response: return response

            self.features = np.vstack([x[0] for x in response if len(x[0])>0])
            self.labels = np.concatenate([x[1] for x in response if len(x[0])>0])
            for x in response:
                if len(x[0])>0:
                    self.feat_names = x[-1]
                    break

    def to_pickle(self, path):
        response_dic = {
            'features':self.features,
            'labels':self.labels,
            'feat_names':self.feat_names
        }

        with open(path, 'wb') as handle:
            pickle.dump(response_dic, handle)

    def to_csv(self, path):
        print(len(self.features))
        print(len(self.labels))
        df = pd.DataFrame(self.features, columns=self.feat_names)
        df['Label'] = self.labels
        df.to_csv(path, index=False)
