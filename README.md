# Asyncpipe
*Asynchronous processing pipeline using asyncio*

Developed and tested with **Python 3.9.6**

## Description

Inspired by `Luigi` and `Airflow` framework. `Asyncpipe` uses `asyncio` to
execute concurrently several processing tasks. It handles failures of
dependent tasks and skips execution of the down stream tasks. Rerunning the
pipeline is supported and only failed/unfinished tasks are executed, provided
their dependencies are done.

### Cycle Detection
Before the pipeline runs it performs a cycle detection in case the user
defined cyclic dependencies.

### Soft Failures
If task's parameter `fail_softly` is set True and the task raises exception
than the state will be `FAILED SOFTLY` allowing downstream tasks to execute.
It is useful when the pipeline contains many branches which eventually merge
into one aggregation final task. If there are some branches which failed
softly then the aggregation task can still execute and provide at least
partial results. `propagate_soft_failures` task's parameter must be
set `False` in order to execute the aggregation task, else it will also
finish `FAILED SOFTLY`. When a task fails softly it creates soft failure file
flag instead of regular output file flag.
Example of instantiating task with soft failures enabled:
```
task = TheTask(dependency=[other_task], fail_softly=True)
```
If upstream task(s) failed softly and the task's parameter
`propagate_soft_failures` is set True, then this task will not execute and
will also create soft failure output flag hence propagating the soft failure
downstream. If `propagate_soft_failures` is set false, it will try to execute
itself not propagating upstream soft failures downstream.
Example of propagating soft failures:
```
class SomePipeline(Pipeline):
    def define(self):
        a = TaskA(fail_softly=True)
        b = TaskB(propagate_soft_failures=True)
        c = TaskC(propagate_soft_failures=False)
        return [a, b, c]
```
In this example if `TaskA` fails (during its execution an exception is risen),
it will have state `FAILED SOFTLY` and will create soft failure output flag.
The `TaskB` will not execute, will create soft failure output flag and
will have state `FAILED SOFTLY` as well. On the other hand, `TaskC` will try
to execute. If it succeeds it will be marked `DONE` else it will be `FAILED`.

When the pipeline is to be rerun the soft failures flags are deleted during
initialization phase so that the rerun of `FAILED SOFTLY` tasks can happen.

## Build
```
poetry install
```

## Unit tests
```
poetry run pytest tests/
```

## Examples
In the folder `examples` are scripts for getting the understanding how to
define tasks and pipelines.

## Technical Debt
 * Multiple outputs from a task are not supported, currently the output is
only one file flag.
 * More civilized abortion of the pipeline is not implemented.
 * ...
