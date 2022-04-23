import asyncio
from abc import ABC, abstractmethod
import pathlib
from typing import List, Optional

from asyncpipe.utils import init_logger
from asyncpipe.task import TaskState, Task

_LOGGER = init_logger(__name__)


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
                func = _LOGGER.error
            elif t_state == TaskState.DONE:
                func = _LOGGER.info
            else:
                func = _LOGGER.warning
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
        _LOGGER.info('Semaphore initialized with %d workers', self.workers)
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
        _LOGGER.info('There are %d tasks in the pipeline', len(self.tasks))

        tasks_complete = self._init_tasks()
        if all(tasks_complete):
            _LOGGER.info('Nothing to run, all tasks are complete')
            return

        _LOGGER.info('Launching tasks...')
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

    def _traverse_the_graph(
        self, tasks: List[Task], rec_stack: Optional[List] = None
    ) -> None:
        """
        From the most downstream tasks traverse the dependencies tree to
        obtain all tasks. Detect the cycles in the graph along the way.

        :param tasks: (List[Task]) list of the most downstream tasks
        :param rec_stack: current recursion loop memory of visited nodes, if
            any task from tasks is already in the rec_stack the pipeline graph
            contains a cycle - exception will be raised.
        """
        if rec_stack is None:
            rec_stack = []
        for t in tasks:
            if t.name in rec_stack:
                raise CyclicPipelineError('Cycle detected in the pipeline')

            self.tasks.add(t)
            rec_stack.append(t.name)
            if len(t.dependency) > 0:
                self._traverse_the_graph(t.dependency, rec_stack)
            rec_stack.remove(t.name)


class PipelineError(Exception):
    """Generic Exception to be overriden"""


class TasksFailedError(PipelineError):
    """raise when at least one task failed in the pipeline"""


class CyclicPipelineError(PipelineError):
    """Raise when a cycle is detected in the pipeline"""
