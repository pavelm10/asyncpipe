import asyncio
from abc import ABC, abstractmethod
import pathlib
from typing import List
from utils import init_logger
from task import TaskState


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

    @abstractmethod
    def define(self) -> List:
        """main method for defining the pipeline structure, must be implemented by a subclass"""
        return []

    def _handle_results(self, results: List):
        """prints tasks' results if there is any failed task will raise eventually TasksFailedError"""
        failed = False
        for (t_name, t_state) in results:
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

    def _init_tasks(self, semaphore: asyncio.Semaphore, tasks: List) -> List:
        """initializes all the tasks: sets semaphore, root output dir, checks which tasks are done"""
        tasks_complete = []
        for t in tasks:
            t.semaphore = semaphore
            t.root_out_dir = self.root_output_dir
            tasks_complete.append(t.done())
        return tasks_complete

    async def _execute(self):
        """
        main async execution method which wraps pipeline definition, tasks' initialization and actual task triggering
        """
        results = []
        semaphore = asyncio.Semaphore(self.workers)
        self.log.info(f"Semaphore initialized with {self.workers} workers")

        tasks = self.define()
        self.log.info(f"There are {len(tasks)} tasks in the pipeline")

        tasks_complete = self._init_tasks(semaphore, tasks)
        if all(tasks_complete):
            self.log.info("Nothing to run, all tasks are complete")
            return

        self.log.info("Launching tasks...")
        atasks = [asyncio.create_task(t.execute(), name=t.name) for t in tasks]

        for t in atasks:
            res = await t
            results.append(res)

        self._handle_results(results)

    def run(self):
        """interface method to wrap calling of the execute method"""
        asyncio.run(self._execute())


class PipelineError(Exception):
    pass


class TasksFailedError(PipelineError):
    """raise when at least one task failed in the pipeline"""
    pass
