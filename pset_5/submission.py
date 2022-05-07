from typing import Dict, List
import os
import io
import json
from datetime import datetime, timedelta, timezone

from git import Repo
from canvasapi.quiz import QuizSubmission, QuizSubmissionQuestion
from canvasapi.upload import Uploader
from canvasapi.util import combine_kwargs
import luigi

from csci_utils.canvas import canvas, answers
from pset_5.tasks import ByDecade, ByStars, ByDay, CleanedReviews, YelpReviews


class AnsP5(answers.Answers):
    """Override from CSCI_Utils to be able to answer new questions"""

    def __init__(self, questions, submission_info):
        self.REGISTRY = {
            "clean": self.answer_repo,
            "commit": self.answer_commit_id,
            "hours": self.answer_time_spent,
            "user_id": self.answer_hashed_user_id,
            "parquet": self.answer_parquet,
            "tag": self.answer_bool_tagged,
            "v_major": self.answer_major_version,
            "v_minor": self.answer_minor_version,
            "v_patch": self.answer_patch_version,
            "stars": self.answer_stars,
            "year": self.answer_decade,
            "dow": self.answer_day,
        }

        self.submission_info = submission_info
        self.repo = Repo(".", search_parent_directories=True)
        self.questions = questions
        self.time_spent = "15+"
        self.answers = self.get_answers()

    def answer_stars(self, question) -> dict:
        """Answers the question by stars

        Question 2 - <p id="stars">Find the average (rounded to int) length of review by # of stars</p>
        {'answer': {'stars_1': None,
            'stars_2': None,
            'stars_3': None,
            'stars_4': None,
            'stars_5': None},
        'answers': None,
        'id': 3039109,
        'question_type': 'fill_in_multiple_blanks_question'}
        """
        task = ByStars()
        ddf = task.get_results()
        ans = {
            star: round(avg_len)
            for star, avg_len in zip(sorted(question.answer.keys()), ddf.avg_len.values)
        }
        return ans

    def answer_decade(self, question) -> dict:
        """Answers the question by year

        Question 1 - <p id="year">Return the average (rounded to int) length of review by year</p>
        {'answer': {'year_2005': None,
            'year_2006': None,
            'year_2007': None,
            'year_2008': None,
            'year_2009': None,
            'year_2010': None,
            'year_2011': None,
            'year_2012': None,
            'year_2013': None,
            'year_2014': None,
            'year_2015': None,
            'year_2016': None,
            'year_2017': None},
        'answers': None,
        'id': 3039108,
        'question_type': 'fill_in_multiple_blanks_question'}
        """
        task = ByDecade()
        ddf = task.get_results()
        ans = {
            year: round(avg_len)
            for year, avg_len in zip(sorted(question.answer.keys()), ddf.avg_len.values)
        }
        return ans

    def answer_day(self, question) -> dict:
        """Answers the question by dow

        Question 3 - <p id="dow">Find the average (rounded to int) length of review by day of week (mon=0, sun=6)</p>
        {'answer': {'dow_0': None,
            'dow_1': None,
            'dow_2': None,
            'dow_3': None,
            'dow_4': None,
            'dow_5': None,
            'dow_6': None},
        'answers': None,
        'id': 3039110,
        'question_type': 'fill_in_multiple_blanks_question'}
        """
        task = ByDay()
        ddf = task.get_results()
        # prsorted(question.answer.keys()), ddf.avg_len.values)
        # prddf)
        ans = {
            dow: round(avg_len)
            for dow, avg_len in zip(sorted(question.answer.keys()), ddf.avg_len.values)
        }
        return ans


class SubmitP5(canvas.Submission):
    """Override of the submission class's get_answers function to answer new questions"""

    def get_answers(self, questions, submission_info) -> list:
        ans = AnsP5(questions, submission_info)
        return ans.answers

    def check_late_submission(self) -> None:
        """CSCI_Utils function stopped working, not entirely sure why, but could be the switch I made to os.environ instead
        of Environs env?"""
        late_days = int(
            os.environ.get("LATE_SUBMISSION_DAYS")
        )  # Prevents builds after the submission deadline

        if datetime.now(timezone.utc) > (
            self.assignment.due_at_date + timedelta(days=late_days, minutes=30)
        ):
            # If you accidentally trigger a build after the deadline, this
            # code will rerun - and mark your submissions late!  Therefore it
            # is best to error out unless you intend to submit late
            raise RuntimeError(
                "Assignment past due, will not submit. Set LATE_SUBMISSION_DAYS if you want to submit late"
            )
