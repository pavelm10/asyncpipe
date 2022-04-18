import asyncio
from abc import ABC, abstractmethod
import pathlib
from typing import List

from asyncpipe.utils import init_logger
from asyncpipe.task import TaskState, Task


class Pipeline(ABC):
    """Defines the actual pipeline structure, what tasks shall run in which
    order and what are the dependencies"""

    def __init__(self, root_output_dir: pathlib.Path, workers: int = 1):
        """
        :param root_output_dir: (pathlib.Path) the root directory where all
            the tasks' subdirectories are located
        :param workers: (optional, int) number of concurrent tasks to run,
            defaults to 1
        """
        self.root_output_dir = root_output_dir
        self.workers = workers
        self.log = init_logger(self.__class__.__name__)
        self.task_results = {}
        self.tasks = set()

    @abstractmethod
    def define(self) -> List:
        """Main method for defining the pipeline structure,
        must be implemented by a subclass"""
        return []

    def _handle_results(self) -> None:
        """Prints tasks' results if there is any failed task will raise
        eventually TasksFailedError"""
        failed = False
        for t_name, t_state in self.task_results.items():
            msg = '%s: %s', t_name, t_state
            if t_state == TaskState.FAILED:
                failed = True
                func = self.log.error
            elif t_state == TaskState.DONE:
                func = self.log.info
            else:
                func = self.log.warning
            func(msg)

        if failed:
            raise TasksFailedError(
                'There was at least one failed task in the pipeline'
            )

    def _init_tasks(self) -> List:
        """initializes all the tasks: sets semaphore, root output dir,
        checks which tasks are done"""
        tasks_complete = []
        semaphore = asyncio.Semaphore(self.workers)
        self.log.info('Semaphore initialized with %d workers', self.workers)
        for t in self.tasks:
            t.semaphore = semaphore
            t.root_out_dir = self.root_output_dir
            t.remove_soft_failure_flag()
            tasks_complete.append(t.done())
        return tasks_complete

    async def _execute(self):
        """
        Main async execution method which wraps pipeline definition,
        tasks' initialization and actual task triggering
        """
        tasks = self.define()
        self._traverse_the_graph(tasks)
        self.log.info('"There are %d tasks in the pipeline', len(self.tasks))

        tasks_complete = self._init_tasks()
        if all(tasks_complete):
            self.log.info('Nothing to run, all tasks are complete')
            return

        self.log.info('Launching tasks...')
        atasks = [
            asyncio.create_task(t.execute(), name=t.name) for t in self.tasks
        ]

        for t in atasks:
            name, state = await t
            self.task_results[name] = state

        self._handle_results()

    def run(self):
        """interface method to wrap calling of the execute method"""
        asyncio.run(self._execute())

    def _traverse_the_graph(self, tasks: List[Task]) -> None:
        """
        From the most downstream tasks traverse the dependencies tree to
        obtain all tasks.

        :param tasks: (List[Task]) list of the most downstream tasks
        """
        for t in tasks:
            self.tasks.add(t)
            if len(t.dependency) > 0:
                self._traverse_the_graph(t.dependency)


class PipelineError(Exception):
    """Generic Exception to be overriden"""


class TasksFailedError(PipelineError):
    """raise when at least one task failed in the pipeline"""
