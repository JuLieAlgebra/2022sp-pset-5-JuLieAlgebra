from unittest import TestCase

from csci_utils.luigi import tasks
from csci_utils.luigi.dask import target

from pset_5.tasks import ByDecade, ByStars, ByDay, CleanedReviews, YelpReviews


### Need to test functionality and remove files to make sure that it gets downloaded
class RequireTests(TestCase):
    def test_requirement(self):
        req = tasks.Requirement(task_class=ByStars)
        assert req.__class__.__name__ == "Requirement"

    def test_requires(self):
        requires = tasks.Requires()
        assert requires.__class__.__name__ == "Requires"

    def test_yelp_requires(self):
        """Requirement and Requires testing for pset_5 luigi task"""
        cl = CleanedReviews()
        assert type(cl.requires()["other"]) == YelpReviews

    def test_decade_requires(self):
        """Requirement and Requires testing for pset_5 luigi task"""
        cl = ByDecade()
        assert type(cl.requires()["other"]) == CleanedReviews

    def test_year_requires(self):
        """Requirement and Requires testing for pset_5 luigi task"""
        cl = ByDecade()
        assert type(cl.requires()["other"]) == CleanedReviews

    def test_day_requires(self):
        """Requirement and Requires testing for pset_5 luigi task"""
        cl = ByDay()
        assert type(cl.requires()["other"]) == CleanedReviews
