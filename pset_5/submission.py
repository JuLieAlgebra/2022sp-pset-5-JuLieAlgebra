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
            "by_stars": self.answer_stars,
            "by_decade": self.answer_decade,
            "by_day": self.answer_day,
        }

        self.submission_info = submission_info
        self.repo = Repo(".", search_parent_directories=True)
        self.questions = questions
        self.time_spent = "15+"
        self.answers = self.get_answers()

    def answer_stars(self, question) -> dict:
        """Answers the question by stars"""
        task = ByStars()
        ddf = task.get_results()
        ans = {
            star: avg_len for star, avg_len in zip(ddf.index.values, ddf.avg_len.values)
        }
        return ans

    def answer_decade(self, question) -> dict:
        """Answers the question by decade"""
        task = ByDecade()
        ddf = task.get_results()
        ans = {
            star: avg_len for star, avg_len in zip(ddf.index.values, ddf.avg_len.values)
        }
        return ans

    def answer_day(self, question) -> dict:
        """Answers the question by star"""
        task = ByDay()
        ddf = task.get_results()
        ans = {
            str(star): avg_len
            for star, avg_len in zip(ddf.index.values, ddf.avg_len.values)
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

        print(self.assignment.due_at_date + timedelta(days=late_days, minutes=30))
        print(self.assignment.due_at_date)
        if datetime.now(timezone.utc) > (
            self.assignment.due_at_date + timedelta(days=late_days, minutes=30)
        ):
            # If you accidentally trigger a build after the deadline, this
            # code will rerun - and mark your submissions late!  Therefore it
            # is best to error out unless you intend to submit late
            raise RuntimeError(
                "Assignment past due, will not submit. Set LATE_SUBMISSION_DAYS if you want to submit late"
            )
