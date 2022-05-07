import os

from luigi import Task, ExternalTask, BoolParameter, Parameter
import luigi
import dask.dataframe

from csci_utils.luigi.dask.target import ParquetTarget, CSVTarget
from csci_utils.luigi.tasks import Requirement, Requires, TargetOutput
from csci_utils.hash.hash_str import get_user_id

from pset_5.salt import SaltedOutput


################################################################################


class YelpReviews(ExternalTask):
    __version__ = "0.1.0"
    # note that this is going to use the csci_salt secret & canvas for get_csci_pepper
    HASH_ID = get_user_id(b"2022sp")
    S3_ROOT = f"s3://cscie29-data/{HASH_ID}/pset_5/yelp_data/"

    output = SaltedOutput(
        target_class=CSVTarget,
        file_pattern=S3_ROOT,
        ext="",
        glob="*.csv",
        storage_options=dict(requester_pays=True),
    )


class CleanedReviews(Task):
    __version__ = "0.1.5"
    subset = BoolParameter(default=True)

    requires = Requires()
    other = Requirement(YelpReviews)

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure
    path = os.path.join("data", "{task.__class__.__name__}-{salt}")
    output = SaltedOutput(
        file_pattern=path, target_class=ParquetTarget, ext="", glob="*.parquet"
    )

    def run(self):
        numeric_cols = ["funny", "cool", "useful", "stars"]
        ddf = self.input()["other"]  # that's the CSV target for some reason?
        ddf = ddf.read_dask(
            dtype={
                "cool": "float64",
                "funny": "float64",
                "useful": "float64",
                "stars": "float64",
            }
        )
        if self.subset:
            ddf = ddf.get_partition(0)

        # filling nan's with zeros as directed
        for col in numeric_cols:
            ddf[col] = ddf[col].fillna(0)

        # should cover null user_id values and no others, since all the nans were filled in the numeric columns
        ddf = ddf.dropna(subset=numeric_cols)
        ddf = ddf[ddf["review_id"].str.len() == 22]
        ddf = ddf.set_index("review_id")
        ddf = ddf.astype(
            dtype={
                "cool": "int64",
                "funny": "int64",
                "useful": "int64",
                "stars": "int64",
            }
        )
        ddf.dropna(subset=["text"])
        # ddf[ddf["text"].isnull()] = ""
        ddf = ddf[ddf["user_id"].str.len() != 0]
        ddf["date"] = dask.dataframe.to_datetime(ddf["date"])

        out = ddf
        self.output().write_dask(out, compression="gzip")


### Note: Am getting 0.6/10 on the quiz answers, really not sure why, but I think, after
###       lots of testing on test files, that I am messing up somewhere in the CleanedReviews
###       task and not in the BySomething tasks
class ByDecade(Task):
    __version__ = "0.1.2"
    subset = BoolParameter(default=True)

    # Be sure to read from CleanedReviews locally
    path = os.path.join("data", "{task.__class__.__name__}-{salt}")
    output = SaltedOutput(
        file_pattern=path, target_class=ParquetTarget, ext="", glob="*.parquet"
    )

    requires = Requires()
    other = Requirement(CleanedReviews, subset=subset)

    def run(self):
        """Return the average (rounded to int) length of review by year"""
        ddf = self.input()["other"].read_dask()
        year_ddf = ddf.groupby(ddf.date.dt.year)
        f = lambda ddf: ddf.text.str.len().mean()
        year_ddf = year_ddf.apply(f).to_frame()
        year_ddf.columns = ["avg_len"]
        out = year_ddf
        self.output().write_dask(out, compression="gzip")

    def get_results(self):
        """I think this is for answering the questions?"""
        return self.output().read_dask().compute()


class ByStars(Task):
    __version__ = "0.1.1"
    subset = BoolParameter(default=True)

    requires = Requires()
    other = Requirement(CleanedReviews, subset=subset)

    path = os.path.join("data", "{task.__class__.__name__}-{salt}")
    output = SaltedOutput(
        file_pattern=path, target_class=ParquetTarget, ext="", glob="*.parquet"
    )

    def run(self):
        """Find the average (rounded to int) length of review by # of stars"""
        ddf = self.input()["other"].read_dask()
        star_ddf = ddf.groupby(ddf.stars)

        f = lambda ddf: ddf.text.str.len().mean()
        avg_len = star_ddf.apply(f).to_frame()
        avg_len.columns = ["avg_len"]
        out = avg_len
        self.output().write_dask(out, compression="gzip")

    def get_results(self):
        return self.output().read_dask().compute()


class ByDay(Task):
    __version__ = "0.1.1"
    subset = BoolParameter(default=True)

    requires = Requires()
    other = Requirement(CleanedReviews, subset=subset)

    path = os.path.join("data", "{task.__class__.__name__}-{salt}")
    output = SaltedOutput(
        file_pattern=path, target_class=ParquetTarget, ext="", glob="*.parquet"
    )

    def run(self):
        """Find the average (rounded to int) length of review by day of week (mon=0, sun=6)"""
        ddf = self.input()["other"].read_dask()
        day_ddf = ddf.groupby(ddf.date.dt.weekday)
        f = lambda ddf: ddf.text.str.len().mean()
        day_ddf = day_ddf.apply(f).to_frame()
        day_ddf.columns = ["avg_len"]
        out = day_ddf
        self.output().write_dask(out, compression="gzip")

    def get_results(self):
        return self.output().read_dask().compute()
