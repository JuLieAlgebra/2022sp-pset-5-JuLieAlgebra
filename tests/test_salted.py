from tempfile import TemporaryDirectory
from unittest import TestCase
from luigi import Task

class SaltedTests(TestCase):
    def test_salted_tasks(self):
        return # remove later
        with TemporaryDirectory() as tmp:
            class SomeTask(Task):
                output = SaltedOutput(file_pattern=os.path.join([tmp, '...']))
                ...

            # Decide how to test a salted workflow