import luigi.build


def main():
    luigi.build([CleanedReviews()], local_scheduler=True, n_workers=1)
