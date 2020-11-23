import unittest
from Domain.entities import AlphaExtractor
import multiprocessing

class TestingAppClass(unittest.TestCase):

    def test_assertion(self):
        alpha = AlphaExtractor(start_date='2020-10-02')
        num_cores = multiprocessing.cpu_count()
        alpha.extract_features(n_jobs=num_cores, comuna='la calera')

    # def test_upper(self):
    #     alpha = AlphaExtractor(start_date='2020-10-02')
    #     num_cores = multiprocessing.cpu_count()
    #     alpha.extract_features(n_jobs=num_cores)
    #     alpha.to_pickle('Domain/data/dataset_full.pkl')

if __name__ == '__main__':
    unittest.main()
