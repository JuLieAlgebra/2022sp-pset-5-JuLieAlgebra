from luigi import Task, BoolParameter


class CleanedReviews(Task):
    subset = BoolParameter(default=True)

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure

    def run(self):

        numeric_cols = ["funny", "cool", "useful", "stars"]
        # ddf = self.input().read_dask(...)

        if self.subset:
            ddf = ddf.get_partition(0)

        # out = ...
        self.output().write_dask(out, compression="gzip")


class BySomething(Task):

    # Be sure to read from CleanedReviews locally

    def run(self):
        raise NotImplementedError()

    def get_results(self):
        return self.output().read_dask().compute()
