# Asyncpipe
 *Asynchronous processing pipeline using asyncio*

Developed with and tested for Python 3.9.6

## Description

Inspired by `Luigi` and `Airflow` framework. `Asyncpipe` uses `asyncio` to execute 
concurrently several processing tasks. It handles failures of dependent tasks and skips
execution of the down stream tasks. Rerunning the pipeline is supported and only 
failed/unfinished tasks are executed, provided their dependencies are done.

## Build environment
Either run inside newly created and activated virtual environment:
```
pip install -r reqs.txt
```
or on linux:
```
./build_venv.sh
```

## Unit tests
To run basic unit tests run (on linux):
```
./run_test.sh
```
or just in activated virtual environment:
```
pytest test.py
```

## Examples
In the folder `examples` are scripts for getting the understanding how to define tasks
and pipelines.

## Current Caveats
 * No checks for cyclic graphs are made - user's responsibility to define the pipeline 
as an acyclic graph.
 * Multiple outputs from a task are not supported, currently the output is only one file flag.
 * More civilized abortion of the pipeline is not implemented, yet.
