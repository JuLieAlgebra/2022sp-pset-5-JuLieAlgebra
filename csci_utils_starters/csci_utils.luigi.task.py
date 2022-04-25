# csci_utils.luigi.task
from functools import partial

from luigi.local_target import LocalTarget
from luigi.target import Target
from luigi.task import Task


class Requirement:
    def __init__(self, task_class, **params):
        raise NotImplementedError()

    def __get__(self, task: Task, cls) -> Task:
        raise NotImplementedError()
        return task.clone(self.task_class, **self.params)


class Requires:
    """Composition to replace :meth:`luigi.task.Task.requires`

    Example::

        class MyTask(Task):
            # Replace task.requires()
            requires = Requires()
            other = Requirement(OtherTask)

            def run(self):
                # Convenient access here...
                with self.other.output().open('r') as f:
                    ...

        >>> MyTask().requires()
        {'other': OtherTask()}

    """

    def __get__(self, task, cls):
        raise NotImplementedError()
        # Bind self/task in a closure
        return partial(self.__call__, task)

    def __call__(self, task) -> dict:
        """Returns the requirements of a task

        Assumes the task class has :class:`.Requirement` descriptors, which
        can clone the appropriate dependences from the task instance.

        :returns: requirements compatible with `task.requires()`
        :rtype: dict
        """
        # Search task.__class__ for Requirement instances
        # return
        raise NotImplementedError()


class TargetOutput:
    def __init__(
        self,
        file_pattern="{task.__class__.__name__}",
        ext=".csv",
        target_class=LocalTarget,
        **target_kwargs
    ):
        raise NotImplementedError()

    def __get__(self, task: Task, cls):
        raise NotImplementedError()
        return partial(self.__call__, task)

    def __call__(self, task: Task) -> Target:
        # Determine the path etc here
        # return self.target_class(...)
        raise NotImplementedError()
