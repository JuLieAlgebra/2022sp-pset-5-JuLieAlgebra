from unittest import TestCase
from csci_utils_starters.csci_utils_luigi_task import Requirement, Requires

# from csci_utils_starters.csci_utils_luigi_dask_target import *
from pset_5.tasks import ByDecade, ByStars, ByDay, CleanedReviews, YelpReviews


class RequireTests(TestCase):
    def test_requirement(self):
        req = Requirement(task_class=ByStars)
        assert req.__class__.__name__ == "Requirement"

    def test_requires(self):
        requires = Requires()
        assert requires.__class__.__name__ == "Requires"

    def test_yelp_requires(self):
        cl = CleanedReviews()
        assert type(cl.requires()["other"]) == YelpReviews

    def test_decade_requires(self):
        cl = ByDecade()
        assert type(cl.requires()["other"]) == CleanedReviews

    def test_year_requires(self):
        cl = ByDecade()
        assert type(cl.requires()["other"]) == CleanedReviews

    def test_day_requires(self):
        cl = ByDay()
        assert type(cl.requires()["other"]) == CleanedReviews
