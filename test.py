import unittest
from Domain.entities import AlphaExtractor
import multiprocessing
from Domain import utils


class TestingAppClass(unittest.TestCase):

    def test_assertion(self):
        # ipc_frame = utils.get_IPC()
        # ipc_frame.to_csv('Domain/data/ipc.csv', index=False)
        # utils.get_acciones()
        alpha = AlphaExtractor(start_date='2020-08-20',
                               end_date='2020-09-11')
        num_cores = multiprocessing.cpu_count()
        alpha.extract_features(n_jobs=num_cores, comuna='concepcion')
        alpha.extract_features(n_jobs=num_cores)

    # def test_upper(self):
    #     alpha = AlphaExtractor(start_date='2020-10-02')
    #     num_cores = multiprocessing.cpu_count()
    #     alpha.extract_features(n_jobs=num_cores)
    #     alpha.to_pickle('Domain/data/dataset_full.pkl')

if __name__ == '__main__':
    unittest.main()
