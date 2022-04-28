# Pset 5

[![Test assignment and submit to Canvas](https://github.com/csci-e-29/2022sp-pset-5-JuLieAlgebra/actions/workflows/canvas.yaml/badge.svg?branch=master)](https://github.com/csci-e-29/2022sp-pset-5-JuLieAlgebra/actions/workflows/canvas.yaml)

[![Maintainability](https://api.codeclimate.com/v1/badges/990ab3ff7dd17e3a202a/maintainability)](https://codeclimate.com/repos/626872670fd5e817ed017244/maintainability)

[![Test Coverage](https://api.codeclimate.com/v1/badges/990ab3ff7dd17e3a202a/test_coverage)](https://codeclimate.com/repos/626872670fd5e817ed017244/test_coverage)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Problem Statement](#problem-statement)
  - [General Advice](#general-advice)
- [Before you begin](#before-you-begin)
  - [Dask](#dask)
    - [Requester pays, and a potential bug!](#requester-pays-and-a-potential-bug)
  - [Luigi warnings](#luigi-warnings)
  - [Testing Tasks](#testing-tasks)
  - [Pytest path](#pytest-path)
- [Problems](#problems)
  - [Composition](#composition)
    - [Requires and Requirement](#requires-and-requirement)
    - [TargetOutput](#targetoutput)
  - [Dask Targets](#dask-targets)
  - [Dask Analysis](#dask-analysis)
    - [Yelp Reviews](#yelp-reviews)
    - [Clean the data](#clean-the-data)
    - [Analysis](#analysis)
      - [CLI](#cli)
- [Extra Credit (Optional Extensions)](#extra-credit-optional-extensions)
  - [Salted](#salted)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

## Problem Statement

In this pset you will do the following high level steps using _Luigi_ and _Dask_:

   1. Reference and read data from AWS S3 with a Luigi _ExternalTask_.

   2. Clean the data, reading in from CSV files and writing out to local Parquet
   files.

   3. Analyze the data, reading in from Parquet files and writing out to Parquet
   files.

Luigi pipelines are invoked from the end, only executing if its target input
file(s) exist.  If the input file does not exist, Luigi invokes the preceding
`Task` that produces the input file. The pipeline looks like this:

**Task3** -> **Task2** -> **Task1**

Task3 relies on the output of Task2, which relies on the output of Task1.  This
pset implements these tasks in order from Task1, Task2, Task3.

Your implementation likely includes the following:

   1. [Python Descriptors](https://docs.python.org/3.7/howto/descriptor.html)
   2. [Callable Objects](https://docs.python.org/3.7/howto/descriptor.html)
   3. [Luigi _Task_ and _ExternalTask_](https://luigi.readthedocs.io/en/stable/api/luigi.task.html): You will extend these classes.
   4. [Dask Dataframes](https://docs.dask.org/en/latest/dataframe.html)

### General Advice

* You will make significant additions to your csci_utils project. It may be
easier to debug code intended for csci_utils in your pset5 project, and then
move it to csci_utils once your tests and debugger show that it works.

* The implementation of each function is typically doable in 10 lines of code or
less, often less.

* AWS credentials must be available in your environment, and *not* committed to
your git repository.

* Test your Luigi tasks thoroughly as you implement them.  Task1 must work
before Task2 works.

* During development, the pipeline entry point might change as you finish
implementing Tasks.  You will run Task1 as you implement Task1.  Then you will
run Task2, which calls Task1, as you implement Task2.

* Do not use boto3 directly in your main code. It may optionally appear in your
test code if you take that approach.

## Before you begin

Begin with your cookiecutter template as usual, and manually merge and link
to this repo as origin.

### Dask

You will likely need to `pipenv install dask[dataframe] fastparquet s3fs`

Ensure you are using `fastparquet` rather than `pyarrow` when working with dask.
This will work automatically so long as it is installed.

In the past, there have been issues installing fastparquet without including
`pytest-runner` in pipenv dev dependencies (and making sure you `pipenv install
--dev`)

#### Requester pays, and a potential bug!

Dask targets must be configured to work with the s3 'requester pays' mode:

```python
target = SomeDaskTarget(storage_options=dict(requester_pays=True))
```

NB: the following bug appears to have been resolved in more recent versions of
s3fs. However, check the course forums for other discussions related to S3/s3fs
versions and issues.  The text below is left for interest or future debugging
needs.

However, testing this pset with `s3fs==0.3.5`, it appears there is a bug that
will prevent dask from passing this config through!

In my testing, listing s3 objects (and calling `target.exists()`) works out of
the box, but actually reading the data failed with a `PermissionDenied` error.

Here is one possible monkey patch:

```python
from s3fs.core import S3File, _fetch_range as _backend_fetch

# A bug! _fetch_range does not pass through request kwargs
def _fetch_range(self, start, end):
    # Original _fetch_range does not pass req_kw through!
    return _backend_fetch(self.fs.s3, self.bucket, self.key, self.version_id, start, end, req_kw=self.fs.req_kw)
S3File._fetch_range = _fetch_range
```

Check your version of s3fs and determine whether this patch (or a modification)
is necessary.

You may want to copy files locally (or even push them up to your own s3 bucket)
to test out your code before messing around with monkey patching s3fs.  If you
can't get this working, it is fine to copy down the csvs and push to your own
(private) s3 bucket for this problem set.

### Luigi warnings

Calling `luigi.build()` inside a test can raise some verbose and nasty warnings,
which are not useful.  While not the best practice, you can quickly silence
these by adding the following to the pytest config file:

```
filterwarnings =
    ignore::DeprecationWarning
    ignore::UserWarning
```

### Testing Tasks

Testing Luigi tasks end to end can be hard, but not impossible. You can write
tests using fake tasks:

```python
from tempfile import TemporaryDirectory
from luigi import Task, build

class SaltedTests(TestCase):
    def test_salted_tasks(self):
        with TemporaryDirectory() as tmp:
            class SomeTask(Task):
                output = SaltedOutput(file_pattern=os.path.join([tmp, '...']))
                ...

            # Decide how to test a salted workflow
```

Note that you can use `build([SomeTask()], local_scheduler=True)` inside a test
to fully run a luigi workflow, but you may want to suppress some warnings if you
do so.

### Pytest path

If you write any tests at the top level `tests/` directory, placing an empty
pytest config file named `conftest.py` at the top of the repository helps to
ensure pytest will set the path correctly at all times. This should not be
necessary if your tests are all inside your package.

## Problems

While you should be able to complete these tasks in order, if you get stuck, it
is possible to finish the dask computation without composing the task or even
doing a proper Luigi target. Ensure to capture your work in appropriate branches
to allow yourself to get unstuck if necessary.

### Composition

Review the descriptor patterns to replace `def output(self):` and  `def
requires(self):` with composition.

Create a new package/module `csci_utils.luigi.task` to capture this work.

#### Requires and Requirement

Finish the implementation of `Requires` and `Requirement` as discussed in
lecture, moving the starter code in
[./csci_utils_starters/csci_utils.luigi.task.py](./csci_utils_starters/csci_utils.luigi.task.py)
to the appropraite location in your `csci_utils` repo (do not leave it in this
repo!).

Tips:

* You can access all the properties of an object using `obj.__dict__` or
`dir(obj)`.  The latter will include inherited properties, while the former will
not.

* You can access the class of an object using `obj.__class__` or `type(obj)`.
  * eg, `MyTask().__class__` is `MyTask`

* The properties on an instance are not necessarily the same as the properties
on a class, eg `MyTask.other` is a `Requirement` instance, but `MyTask().other`
but should be an instance of `OtherTask`.

* You can get an arbitrary property of an object using `getattr(obj, 'asdf')`,
eg `{k: getattr(obj, k) for k in dir(obj)}`

* You can check the type using `isinstance(obj, Requirement)`

* You can use dict comprehensions eg `{k: v for k, v in otherdict.items() if
condition}`


#### TargetOutput

Implement a descriptor that can be used to generate a luigi target using
composition, inside the same `task` module as above.

Note the string patterns in `file_pattern`, which are intended to be used like
such (assuming you had an instance where `task.param == 'hello'`):

```python
>>> '{task.param}-{var}.txt'.format(task=task, var='world')
'hello-world.txt'
```

See [str.format](https://docs.python.org/3.4/library/stdtypes.html#str.format)
for reference.

### Dask Targets

Dask outputs are typically folders; as such, they are not suitable for
directly using the luigi `FileSystemTarget` variants.  Dask uses its own file
system abstractions which are not compatible with Luigi's.

Correctly implementing the appropriate logic is a bit difficult, so you can
start with the included code in [./csci_utils_starters/csci_utils.luigi.dask.target.py](./csci_utils_starters/csci_utils.luigi.dask.target.py) and move it to your utils library too.

A few notes on the design here:

* There is an abstract `BaseDaskTarget` which tries to handle all the general
logic.

* The main public interface of `BaseDaskTarget` includes `.exists()`,
`.read_dask()`, and `.write_dask()`

* Note how concrete Dask targets must extend some of the private methods to
adapt the base class to CSV or Parquet.  Don't overthink these - they only
require a few lines of code!

* Also note that some of the abstract methods are `classmethod`'s.  This is a
somewhat arbitrary design choice; it means all instance logic of how to
read/write is done in the parent class, and the concrete class read/write
behaves literally like a function - all config should be passed through as
variables to the concrete implementations.

* Note that these targets force you to specify directory datasets with an ending
`/`; Dask (annoyingly) is inconsistent on this, so you may find yourself
manipulating paths inside ParquetTarget and CSVTarget differently.  The user of
these targets should not need to worry about these details!

You should fully implement and test read/write for both CSVTarget and
ParquetTarget, even if you don't need to read/write from both for the problem
set (you need both read and write to adequately test each one!).

### Dask Analysis

Implement your luigi tasks in [pset_5.tasks](./pset_5/tasks.py)

#### Yelp Reviews

The data for this problem set is here:
```bash
$ aws s3 ls s3://cscie29-data/<HASH_ID>/pset_5/yelp_data/
2019-03-29 16:35:53    6909298 yelp_subset_0.csv
...
2019-03-29 16:35:54    7010664 yelp_subset_19.csv
```

where `<HASH_ID>` is the first 8 digits of our semester code hashed with the
CSCI salt and pepper.  Semester codes are the year followed by either `sp` of
`fa`, eg `2019fa`.

***Do not copy the data! Everything will be done with dask via S3.***  You do
NOT need a Makefile for this pset.

Write an ExternalTask named `YelpReviews` which uses the appropriate dask target
from above.  Read the code carefully - the defaults for the dask target may
not work!

#### Clean the data

We will turn the data into a parquet data set and cache it locally.  Start with
the `CleanedReviews` implementation in `pset_5.tasks`.

Note that we turn on `subset` by default to limit bandwidth during CI/CD and any
default testing you do.  Be sure to deploy your final answers quiz using the
full data set.

Notes on cleaning:

* All computation should use Dask.  Do not compute to a pandas DF (you may use
map_partitions etc)

* Ensure that the `date` column is parsed as a pandas datetime using
[parse_dates](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_csv.html)

* The columns `["funny", "cool", "useful", "stars"]` are all inherently
integers. However, given there are missing values, you must first read them as
floats, fill nan's as 0, then convert to int.  You can provide a dict of
`{col: dtype}` when providing the dtype arg in places like `read_parquet` and
`astype`

* There are about 60 rows (out of 200,000) that are corrupted which you can
drop. They are misaligned, eg the text shows up in the row id.  You can find
them by dropping rows where `user_id` is null or the length of `review_id`
is not 22.

* You should set the index to `review_id` and ensure the output reads back with
meaningful divisions

#### Analysis
Let's look at how the length of a review depends on some factors.  By length,
let's just measure the number of characters (no need for text parsing).  Refer
to the Canvas quiz to determine the exact analysis required; create a separate
Luigi task to answer each problem (eg, `ByDecade`, `ByStars`, etc).

##### CLI

Running `python -m pset_5` on your deployment should kick off the luigi build
and display the results of each of these analyses.

You should add a flag, eg `python -m pset_5 --full`, to run and display the
results on the entire set.  Use these for your deployed quiz responses.

Each analysis should be a separate Luigi task, which computes its analysis and
writes the result to parquet.  To display to the terminal or answer a quiz, the
output should be read back from the written parquet file.  See the pattern in
`BySomething`.

You can restructure using parent classes if you wish to capture some of the
patterns.

A few tips:

* You may need to use the `write_index` param in `to_parquet` after a `groupby`
given the default dask behavior. See
[to_parquet](http://docs.dask.org/en/latest/dataframe-api.html#dask.dataframe.DataFrame.to_parquet)

* You cannot write a `Series` to parquet. If you have something like
`df['col'].groupby(...).mean()` then you need to promote it back to a dataframe,
like `series.to_frame()`, or `df[['col']]` to keep it as a 1-column frame to
begin with.

* Round all your answers and store them as integers.  Report the integers in
Canvas.

* Consider using pandas/dask [text
tools](https://pandas.pydata.org/pandas-docs/stable/user_guide/text.html) like
`df.str`

* Only load the columns you need from `CleanedReviews`!  No need to load all
columns every time.

## Extra Credit (Optional Extensions)

### Salted

You can include a Salted Graph workflow outlined and demo'ed here:
[https://github.com/gorlins/salted](https://github.com/gorlins/salted)

You may use the code as reference, but it is not intended to be directly
imported - you should reimplement what you need in your csci_utils.

You can either create a subclass of `TargetOutput` or allow for this
functionality in the main class, eg:

```python
class SaltedOutput(TargetOutput):
    def __init__(self, file_pattern='{task.__class__.__name__}-{salt}', ...):
        ...
```

If the format string asks for the keyword `salt`, you should calculate the
task's salted id as a hexdigest and include it as a kwarg in the string format.

Refer to `get_salted_id` in the demo repo to get the globally unique salted data
id.

Considerations - how should/do the following impact your salted version, given
the implementation?  Not all may be relevant to this problem set.

* How would you handle renaming a parameter, or adding a new parameter with a
default value that was previously hard coded?

* For large graphs with many redundant dependencies (eg a task is required by
multiple other tasks, which may then be joined downstream), do you recreate
the salt or cache it for performance?

* For tasks with multiple requirements, how do you handle a reordering or
renaming (for a dict requires)?

* How do you handle serialization of parameters for hashing?  E.g. for `1` vs
`1.0`?  What about more complicated parameter types, like `TaskParameter`?

* How do you handle 'partitioning' parameters (those that eg dictate which
folder a task is written to or which data subset to load, rather than
dictating how the task processes the data)?  Do you want consistent versions
across partitions?
