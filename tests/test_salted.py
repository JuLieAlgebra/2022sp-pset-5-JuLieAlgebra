from tempfile import TemporaryDirectory
from unittest import TestCase
import os
import luigi

from pset_5 import salt


class SaltedTests(TestCase):
    def test_salted_tasks(self):
        with TemporaryDirectory() as tmp:

            class SomeTask(luigi.Task):
                __version__ = "1.0"
                pattern = "{task.__class__.__name__}-{salt}"
                output = salt.SaltedOutput(file_pattern=os.path.join(tmp, pattern))

                def run(self):
                    with self.output().open("w") as out:
                        out.write("Test")

            task = SomeTask()
            task.output()
            print(task.output())
            # assert os.path.exists(os.path.join(tmp, pattern))

    def test_salted(self):
        """Tests basic functionality and that changing parameters
        results in new salt"""
        with TemporaryDirectory() as tmp:

            class Requirement(luigi.Task):
                __version__ = "1.0"

                def output(self):
                    return luigi.LocalTarget("requirement.txt")

            class myTask(luigi.Task):
                __version__ = "1.0"
                param = luigi.Parameter()

                def output(self):
                    return luigi.LocalTarget("test.txt")

                def requires(self):
                    return Requirement()

            # task = myTask("arg")
            # salt_word = salt.get_salted_version(task)
            # task.__version__ = "1.1"
            # salt_update = salt_word.get_salted_version(task)

            # assert salt_word != salt_update
