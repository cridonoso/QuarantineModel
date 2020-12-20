import sys,os
sys.path.append(os.path.realpath('.'))
from Domain.entities import (AlphaExtractor,
                             OnlyCovidExtractor,
                             BetaExtractor)
import multiprocessing
from Domain import utils
from time import time

t0 = time()
ipc_frame = utils.get_IPC()
ipc_frame.to_csv('Domain/data/ipc.csv', index=False)

alpha = BetaExtractor(start_date='2020-03-10', end_date='2020-12-19')
num_cores = multiprocessing.cpu_count()

alpha.extract_features(n_jobs=num_cores)
alpha.to_csv('Domain/data/20201220_features_beta.csv')
# alpha.to_pickle('Domain/data/dataset_new.pkl')
t1 = time()

print('Feature extraction took {:.2f} sec on {} cores'.format((t1-t0), num_cores))
