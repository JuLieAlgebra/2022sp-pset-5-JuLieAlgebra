import sys
import os
import argparse
from datetime import datetime, timedelta
from unittest import TestCase, mock, main
from io import StringIO
from contextlib import contextmanager
import contextlib
from datetime import datetime, timedelta, timezone

from canvasapi import Canvas, course
from git import Repo

####################
from unittest import TestCase, mock
import os
from pset_5.submission import AnsP5, SubmitP5
from pset_5 import cli


class SubmissionTests(TestCase):
    def fake_list_quiz_questions(self):
        class FakeCleanQuestion:
            def __init__(self):
                self.answers = [
                    {"id": 4031, "text": "True"},
                    {"id": 5153, "text": "False"},
                ]
                self.answer = None
                self.id = 1
                self.question_type = "true_false_question"
                self.question_text = '<p id="clean">Is your git repo clean?</p>'

        class FakeYearQuestion:
            def __init__(self):
                self.answers = None
                self.answer = {
                    "test strings doing something": None,
                    "blah blah blah": None,
                    "I have a cat on my legs right now": None,
                }
                self.id = 2
                self.question_type = "fill_in_multiple_blanks_question"
                self.question_text = '<p id="year">Embed the following documents (replacing "_" with " "), and return the sum of the resulting vector in the string form \'<span>{:.4f}\'</span></p>'

        class FakeDOWQuestion:
            def __init__(self):
                self.answers = None
                self.answer = {
                    "test strings doing something": None,
                    "blah blah blah": None,
                    "I have a cat on my legs right now": None,
                }
                self.id = 2
                self.question_type = "fill_in_multiple_blanks_question"
                self.question_text = '<p id="dow">Embed the following documents (replacing "_" with " "), and return the sum of the resulting vector in the string form \'<span>{:.4f}\'</span></p>'

        class FakeStarsQuestion:
            def __init__(self):
                self.answers = None
                self.answer = {
                    "test strings doing something": None,
                    "blah blah blah": None,
                    "I have a cat on my legs right now": None,
                }
                self.id = 2
                self.question_type = "fill_in_multiple_blanks_question"
                self.question_text = '<p id="stars">Embed the following documents (replacing "_" with " "), and return the sum of the resulting vector in the string form \'<span>{:.4f}\'</span></p>'

        return [
            FakeCleanQuestion(),
            FakeStarsQuestion(),
            FakeYearQuestion(),
            FakeDOWQuestion(),
        ]

    def fake_canvas(url, token):
        """Fakes a couple of things for testing get_csci_pepper"""

        class FakeAssignment:
            def __init__(self):
                self.due_at_date = datetime.now(timezone.utc)
                self.id = 100

        class FakeCourse:
            def __init__(self):
                self.uuid = b"1000"

            def get_assignment(self, id):
                return FakeAssignment()

            def get_quiz(self, id):
                return 200

        class FakeCanvas:
            def __init__(self):
                self.fake_id = 555

            def get_course(self, id):
                return FakeCourse()

        return FakeCanvas()

    def fake_assignment(self, fake_canvas):
        class FakeAssignment:
            def submit(self, fake_arg1, comment=""):
                return 10

        return FakeAssignment()

    def fake_quiz(self, fake_canvas):
        question_list = self.fake_list_quiz_questions()

        class FakeSubmission:
            def answer_submission_questions(self, quiz_questions=""):
                return ["fake", "answers"]

            def get_submission_questions(self):
                return question_list

            def complete(self):
                return True

        class FakeQuiz:
            def create_submission(self):
                return FakeSubmission()

        return FakeQuiz()

    def fake_sub_info(self, fake_canvas, fake_arg1, fake_arg2):
        return {"is_dirty": False}

    @mock.patch(
        "csci_utils.canvas.canvas.Submission.get_assignment",
        lambda fake_canvas: SubmissionTests().fake_assignment(fake_canvas),
    )
    @mock.patch(
        "csci_utils.canvas.canvas.Submission.get_quiz",
        lambda fake_canvas: SubmissionTests().fake_quiz(fake_canvas),
    )
    @mock.patch(
        "csci_utils.canvas.canvas.Submission.get_submission_info",
        lambda fake_canvas, fake_arg1, fake_arg2: SubmissionTests().fake_sub_info(
            fake_canvas, fake_arg1, fake_arg2
        ),
    )
    @mock.patch.dict(
        os.environ,
        {
            "CANVAS_URL": "fake_url",
            "CANVAS_TOKEN": "faketoken",
            "CANVAS_COURSE_ID": "1000",
            "CANVAS_QUIZ_ID": "500",
            "CANVAS_ASSIGNMENT_ID": "99",
            "LATE_SUBMISSION_DAYS": "0",
            "ALLOW_DIRTY": "True",
        },
    )
    @mock.patch("canvasapi.Canvas", fake_canvas)
    def test_submission(self):
        sub = SubmitP5()
        with sub.submit() as s:
            print(sub.course)  # get_answers(sub.questions, sub.submission_info)

    @mock.patch("argparse.ArgumentParser.parse_args", mock.Mock())
    def test_cli_full(self):
        """Tests that the arg parser returns the right values for subset=False"""
        parser = cli.create_parse()
        # Ugh, got a little tripped up over the inverted logic
        # Should we use the subset? If --full, no.
        assert not cli.subset_flag(parser, "--full")

    # TODO test when not making other changes to testing module
    # @mock.patch("argparse.ArgumentParser.parse_args", mock.Mock())
    # def test_cli_subset(self):
    #     """Tests that the arg parser returns the right values for subset=False"""
    #     parser = cli.create_parse()
    #     # Ugh, got a little tripped up over the inverted logic
    #     # Should we use the subset? If --full, no.
    #     assert not cli.subset_flag(parser, "")
