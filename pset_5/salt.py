import hashlib
from functools import partial

from luigi.task import flatten
from luigi import LocalTarget, Task, Target

# from csci_utils.luigi.task import TargetOutput
from csci_utils_starters.csci_utils_luigi_task import TargetOutput


class SaltedOutput(TargetOutput):
    def __init__(
        self,
        file_pattern="{task.__class__.__name__}-{salt}",
        ext=".csv",
        target_class=LocalTarget,
        **target_kwargs
    ):
        super().__init__(
            file_pattern=file_pattern,
            ext=ext,
            target_class=target_class,
            **target_kwargs,
        )

    def __get__(self, task: Task, cls):
        if task is None:
            return self
        return partial(self.__call__, task)

    def __call__(self, task: Task) -> Target:
        # modified file pattern to be more the lecture from March 3rd
        return self.target_class(
            self.file_pattern.format(task=task, salt=self.get_salted_version(task))
            + self.ext,
            **self.target_kwargs,
        )

    def get_salted_version(self, task: Task) -> str:
        """
        Rough version of Prof. Gorlin's implementation. Skips over the parameters
        of the Tasks.
        """
        salt = ""
        # sorting the requirements as suggested to increase salt stability
        for req in sorted(flatten(task.requires())):
            salt += self.get_salted_version(req)

        salt += task.__class__.__name__ + task.__version__
        return hashlib.sha256(salt.encode()).hexdigest()[:10]


# # LocalTarget(file_pattern.format(
# #         salt=get_salted_version(task)[:6], self=task, **kwargs
# #     ), format=format)
