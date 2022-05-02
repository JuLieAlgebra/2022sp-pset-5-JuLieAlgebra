from luigi.task import flatten
from csci_utils.luigi.task import TargetOutput


class SaltedOutput(TargetOutput):
    def __init__(
        self,
        file_pattern="{task.__class__.__name__}-{salt}{self.ext}",
        ext=".csv",
        target_class=LocalTarget,
        **target_kwargs
    ):
        super().__init__(
            file_pattern=file_pattern.format(salt=self.get_salted_version()),
            ext=ext,
            target_class=target_class,
            target_kwargs=target_kwargs,
        )
        # self.file_pattern = file_pattern
        # self.ext = ext
        # self.target_class = target_class
        # self.target_kwargs = target_kwargs

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
        return sha256(salt.encode()).hexdigest()[:10]


# SaltedOutput()

# # LocalTarget(file_pattern.format(
# #         salt=get_salted_version(task)[:6], self=task, **kwargs
# #     ), format=format)


# From Prof Gorlin's Salted demo
def get_salted_version(task):
    """Create a salted id/version for this task and lineage
    :returns: a unique, deterministic hexdigest for this task
    :rtype: str
    """

    msg = ""

    # Salt with lineage
    for req in flatten(task.requires()):
        # Note that order is important and impacts the hash - if task
        # requirements are a dict, then consider doing this is sorted order

        # recursive call until there are no more dependencies
        msg += get_salted_version(req)

    # Uniquely specify this task
    msg += ",".join(
        [
            # Basic capture of input type
            task.__class__.__name__,
            # Change __version__ at class level when everything needs rerunning!
            task.__version__,
        ]
        + [
            # Depending on strictness - skipping params is acceptable if
            # output already is partitioned by their params; including every
            # param may make hash *too* sensitive
            "{}={}".format(param_name, repr(task.param_kwargs[param_name]))
            for param_name, param in sorted(task.get_params())
            if param.significant
        ]
    )
    return sha256(msg.encode()).hexdigest()
