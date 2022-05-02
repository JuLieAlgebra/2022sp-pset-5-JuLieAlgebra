from luigi import Task, ExternalTask, BoolParameter, Parameter
import luigi
from luigi.contrib.s3 import S3Target
import s3fs

from csci_utils.hash.hash_str import get_user_hash

from csci_utils_starters.csci_utils_luigi_task import Requirement, Requires
from csci_utils_starters.csci_utils_luigi_dask_target import ParquetTarget, CSVTarget

from pset_5.salt import SaltedOutput

# from csci_utils.luigi.dask_target import ParquetTarget, CSVTarget
# from csci_utils.luigi.task import Requirement, Requires


class YelpReviews(ExternalTask):
    __version__ = "0.1.0"
    # note that this is going to use the csci_salt secret
    HASH_ID = get_user_hash("2022sp").hex()[:8]
    S3_ROOT = f"s3://cscie29-data/{HASH_ID}/pset_5/yelp_data/"

    def output(self):
        # Need to use the s3fs for the csv target...?
        # Slightly modified example from s3fs documentation
        fs = s3fs.S3FileSystem(requester_pays=True)
        print(self.HASH_ID)
        bucket_path = f"cscie29-data/{self.HASH_ID}/pset_5/yelp_data/"
        targets = fs.ls(bucket_path)  # -> ['my-file.txt']
        with fs.open(bucket_path + "yelp_subset_0.csv", "rb") as f:
            print(f.read())  # -> b'Hello, world'

        return CSVTarget(S3_ROOT, storage_options=dict(requester_pays=True))
        # file_pattern = "yelp_subset_{i}.csv"
        # return {
        #     S3Target(
        #         S3_ROOT + file_pattern.format(i=i), format=luigi.format.Nop
        #     ): file_pattern.format(i=i)
        #     for i in range(0, 20)
        # }


class DownloadReviews(Task):
    __version__ = "0.1.0"

    requires = Requires()
    other = Requirement(YelpReviews)

    # need to fix this default
    path = Parameter(default="data/")

    output = SaltedOutput(
        target_class=CSVTarget,
        format=luigi.format.Nop,
        target_kwargs=dict(requester_pays=True),
    )

    # or many it's this way..? Only if returning the dictionary form YelpReviews though
    def run(self):
        """Downloads the model by writing a copy to the output file"""
        # I think I need to modify this
        inputs = self.input()
        # is collection a list of S3 targets?
        collection = inputs.keys()
        # how to do this?
        self.output().write_dask(collection, compute=True, storage_options=None)


class CleanedReviews(Task):
    __version__ = "0.1.0"
    subset = BoolParameter(default=True)

    requires = Requires()
    other = Requirement(DownloadReviews)

    output = SaltedOutput(
        target_class=ParquetTarget, storage_options=dict(requester_pays=True)
    )
    # def output(self):
    #     return SaltedOutput(...)

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure

    def run(self):

        numeric_cols = ["funny", "cool", "useful", "stars"]
        ddf = self.input().read_dask(...)

        if self.subset:
            ddf = ddf.get_partition(0)

        # out = ...
        self.output().write_dask(out, compression="gzip")


class BySomething(Task):
    __version__ = "0.1.0"

    # Be sure to read from CleanedReviews locally
    def output(self):
        return  # something

    def run(self):
        raise NotImplementedError()

    def get_results(self):
        return self.output().read_dask().compute()
