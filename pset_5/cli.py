import luigi
import sys
from pprint import pprint
import argparse

from pset_5 import tasks
from pset_5 import submission


def create_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full", help="Run Luigi tasks on full set of partitions or not"
    )
    return parser


def subset_flag(parser, args) -> bool:
    """Parse flag for running with full dataset or not"""
    if parser.parse_args(args).full:
        return True
    return False


def main():
    arg = subset_flag(create_parse(), sys.argv[1:])
    luigi.build(
        [
            tasks.ByStars(subset=arg),
            tasks.ByDay(subset=arg),
            tasks.ByDecade(subset=arg),
        ],
        local_scheduler=True,
    )
    sub = submission.SubmitP5()
    with sub.submit() as submission_objects:
        print("###### \n Quiz Questions \n ######")
        sub.print_questions(submission_objects[1])
        print("####### \n Quiz Answers \n #######\n")
        pprint(submission_objects[2])
