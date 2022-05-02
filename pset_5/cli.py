import luigi

from pset_5 import tasks


def main():
    luigi.build([tasks.CleanedReviews()], local_scheduler=True)  # , n_workers=1)
