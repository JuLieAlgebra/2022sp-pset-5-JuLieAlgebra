from tempfile import TemporaryDirectory
from unittest import TestCase
import os
import luigi

from pset_5 import salt


class SaltedTests(TestCase):
    """Tests that the SaltedOutput extra credit works as intended"""

    def test_salted_tasks(self):
        """Tests that SaltedOutput writes a file with the pattern and class expected"""
        with TemporaryDirectory() as tmp:

            class SomeTask(luigi.Task):
                __version__ = "1.0"
                pattern = "{task.__class__.__name__}-{salt}"
                output = salt.SaltedOutput(file_pattern=os.path.join(tmp, pattern))

                def run(self):
                    with self.output().open("w") as out:
                        out.write("Test")

            task = SomeTask()
            outfile = task.output()
            assert isinstance(outfile, luigi.local_target.LocalTarget)
            assert outfile.path[:-15] == os.path.join(tmp, "SomeTask")

    def test_salted(self):
        """Tests basic functionality and that changing versions
        results in new salt"""
        with TemporaryDirectory() as tmp:

            class Requirement(luigi.Task):
                __version__ = "1.0"

                pattern = "{task.__class__.__name__}-{salt}"
                output = salt.SaltedOutput(file_pattern=os.path.join(tmp, pattern))

                def run(self):
                    with self.output().open("w") as out:
                        out.write("Test")

            class myTask(luigi.Task):
                __version__ = "1.0"
                param = luigi.Parameter()

                pattern = "{task.__class__.__name__}-{salt}"
                output = salt.SaltedOutput(file_pattern=os.path.join(tmp, pattern))

                def run(self):
                    with self.output().open("w") as out:
                        out.write("Test")

                def requires(self):
                    return Requirement()

            task = myTask("arg")
            outfile = task.output()
            task.__version__ = "1.1"
            update_outfile = task.output()

            assert outfile != update_outfile
