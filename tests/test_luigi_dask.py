from unittest import TestCase

from csci_utils_starters.csci_utils_luigi_dask_target import ParquetTarget, CSVTarget

# from csci_utils_starters.csci_utils_luigi_dask_target import *
from pset_5.tasks import ByDecade, ByStars, ByDay, CleanedReviews, YelpReviews


class RequireDask(TestCase):
    def test_parquet_target(self):
        path = "data/"
        task = ParquetTarget(path, glob="*.csv", flag="_SUCCESS", storage_options=None)
        print(task)
