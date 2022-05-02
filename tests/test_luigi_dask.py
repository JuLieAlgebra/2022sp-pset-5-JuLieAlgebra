from unittest import TestCase

from csci_utils_starters.csci_utils_luigi_dask_target import ParquetTarget, CSVTarget

# from csci_utils_starters.csci_utils_luigi_dask_target import *
from pset_5.tasks import BySomething, CleanedReviews, YelpReviews


class RequireDask(TestCase):
    def test_parquet_target(self):
        task = ParquetTarget(path, glob="*.csv", flag="_SUCCESS", storage_options=None)
        print(task)
