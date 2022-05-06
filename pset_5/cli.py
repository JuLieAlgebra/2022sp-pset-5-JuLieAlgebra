import luigi

from pset_5 import tasks
from pset_5 import submission


def main():
    luigi.build(
        [tasks.ByStars(), tasks.ByDay(), tasks.ByDecade()],
        local_scheduler=True,
    )
    sub = submission.SubmitP5()
    with sub.submit() as submission_objects:
        print("###### \n Quiz Questions \n ######")
        sub.print_questions(submission_objects[1])
        print("####### \n Quiz Answers \n #######\n", submission_objects[2])
