from unittest import TestCase
import os

from csci_utils.luigi import tasks
from csci_utils.luigi.dask import target

from pset_5.tasks import ByDecade, ByStars, ByDay, CleanedReviews, YelpReviews


## NOTE: since subset=True for the testing, it will not interfere with running `pipenv run python -m pset_5 --full FULL`, as
##       the salt for that will be different with subset=False
class TaskTests(TestCase):
    def test_requires(self):
        requires = tasks.Requires()
        assert requires.__class__.__name__ == "Requires"

    def test_cleanedreviews(self):
        """Testing functionality for the single partition of data testing for pset_5 luigi task"""
        cl = CleanedReviews(subset=True)
        # checking that the task's requirements are as expected
        assert type(cl.requires()["other"]) == YelpReviews
        outfile = cl.output().path
        if os.path.exists(os.path.join(outfile, "_SUCCESS")):
            os.remove(os.path.join(outfile, "_SUCCESS"))
        cl.run()
        assert os.path.exists(os.path.join(outfile, "_SUCCESS"))

    def test_decade(self):
        """Testing functionality for the single partition of data testing for pset_5 luigi task"""
        cl = ByDecade(subset=True)
        outfile = cl.output().path
        if os.path.exists(os.path.join(outfile, "_SUCCESS")):
            os.remove(os.path.join(outfile, "_SUCCESS"))
        cl.run()
        assert os.path.exists(os.path.join(outfile, "_SUCCESS"))

        # checking that the task's requirements are as expected
        assert type(cl.requires()["other"]) == CleanedReviews

    def test_stars(self):
        """Testing functionality for the single partition of data testing for pset_5 luigi task"""
        cl = ByStars(subset=True)
        outfile = cl.output().path
        if os.path.exists(os.path.join(outfile, "_SUCCESS")):
            os.remove(os.path.join(outfile, "_SUCCESS"))
        cl.run()
        assert os.path.exists(os.path.join(outfile, "_SUCCESS"))
        # checking that the task's requirements are as expected
        assert type(cl.requires()["other"]) == CleanedReviews

    def test_day(self):
        """Testing functionality for the single partition of data testing for pset_5 luigi task"""
        cl = ByDay(subset=True)
        outfile = cl.output().path
        if os.path.exists(os.path.join(outfile, "_SUCCESS")):
            os.remove(os.path.join(outfile, "_SUCCESS"))
        cl.run()
        assert os.path.exists(os.path.join(outfile, "_SUCCESS"))

        # checking that the task's requirements are as expected
        assert type(cl.requires()["other"]) == CleanedReviews
