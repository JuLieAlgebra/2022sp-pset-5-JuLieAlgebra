import luigi
import sys
from pprint import pprint
import argparse

from pset_5 import tasks
from pset_5 import submission


def create_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full",
        help="Run Luigi debug on full set of partitions or not",
        action="store_true",
    )
    return parser


def subset_flag(parser, args) -> bool:
    """Parse flag for running with full dataset or not"""
    return not parser.parse_args(args).full


def main():
    """
    Note!! To use full dataset:
    Usage: pipenv run python -m pset_5 --full
    """
    arg = subset_flag(create_parse(), sys.argv[1:])
    tasks.CleanedReviews.subset = luigi.BoolParameter(default=arg)
    luigi.build(
        [
            tasks.ByStars(),
            tasks.ByDay(),
            tasks.ByDecade(),
        ],
        local_scheduler=True,
    )
    sub = submission.SubmitP5()
    with sub.submit() as submission_objects:
        print("###### \n Quiz Questions \n ######")
        sub.print_questions(submission_objects[1])
        print("####### \n Quiz Answers \n #######\n")
        pprint(submission_objects[2])
