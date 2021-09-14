import asyncio
from abc import ABC, abstractmethod
import pathlib
from typing import List, Set
from utils import init_logger
from task import TaskState, Task


class Pipeline(ABC):
    """Defines the actual pipeline structure, what tasks shall run in which order and what are the dependencies"""

    def __init__(self, root_output_dir: pathlib.Path, workers: int = 1):
        """
        :param root_output_dir: (pathlib.Path) the root directory where all the tasks' subdirectories are located
        :param workers: (optional, int) number of concurrent tasks to run, defaults to 1
        """
        self.root_output_dir = root_output_dir
        self.workers = workers
        self.log = init_logger(self.__class__.__name__)
        self.task_results = {}
        self.tasks = set()

    @abstractmethod
    def define(self) -> List:
        """main method for defining the pipeline structure, must be implemented by a subclass"""
        return []

    def _handle_results(self) -> None:
        """prints tasks' results if there is any failed task will raise eventually TasksFailedError"""
        failed = False
        for t_name, t_state in self.task_results.items():
            msg = f"{t_name}: {t_state}"
            if t_state == TaskState.FAILED:
                failed = True
                func = self.log.error
            elif t_state == TaskState.DONE:
                func = self.log.info
            else:
                func = self.log.warning
            func(msg)

        if failed:
            raise TasksFailedError("There was at least one failed task in the pipeline")

    def _init_tasks(self, semaphore: asyncio.Semaphore, tasks: Set) -> List:
        """initializes all the tasks: sets semaphore, root output dir, checks which tasks are done"""
        tasks_complete = []
        for t in tasks:
            t.semaphore = semaphore
            t.root_out_dir = self.root_output_dir
            t.remove_soft_failure_flag()
            tasks_complete.append(t.done())
        return tasks_complete

    async def _execute(self):
        """
        main async execution method which wraps pipeline definition, tasks' initialization and actual task triggering
        """
        semaphore = asyncio.Semaphore(self.workers)
        self.log.info(f"Semaphore initialized with {self.workers} workers")

        tasks = self.define()
        self._traverse_the_graph(tasks)
        self.log.info(f"There are {len(self.tasks)} tasks in the pipeline")

        tasks_complete = self._init_tasks(semaphore, self.tasks)
        if all(tasks_complete):
            self.log.info("Nothing to run, all tasks are complete")
            return

        self.log.info("Launching tasks...")
        atasks = [asyncio.create_task(t.execute(), name=t.name) for t in self.tasks]

        for t in atasks:
            name, state = await t
            self.task_results[name] = state

        self._handle_results()

    def run(self):
        """interface method to wrap calling of the execute method"""
        asyncio.run(self._execute())

    def _traverse_the_graph(self, tasks: List[Task]) -> None:
        """
        from the most downstream tasks traverse the dependencies tree to obtain all tasks
        :param tasks: (List[Task]) list of the most downstream tasks
        """
        for t in tasks:
            self.tasks.add(t)
            if len(t.dependency) > 0:
                self._traverse_the_graph(t.dependency)


class PipelineError(Exception):
    pass


class TasksFailedError(PipelineError):
    """raise when at least one task failed in the pipeline"""
    pass
