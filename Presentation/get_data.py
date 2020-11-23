import sys,os
sys.path.append(os.path.realpath('.'))
from Domain.entities import AlphaExtractor
import multiprocessing

alpha = AlphaExtractor(start_date='2020-03-02')
num_cores = multiprocessing.cpu_count()
alpha.extract_features(n_jobs=num_cores)
alpha.to_pickle('Domain/data/dataset.pkl')
