import asyncio
from abc import ABC, abstractmethod
import json
import pathlib
from typing import Dict, List, Tuple

import aiofiles

from utils import init_logger


class TaskState:
    """definition of possible task's states"""
    PENDING = 'PENDING'
    FAILED = 'FAILED'
    DONE = 'DONE'
    FAILED_DEPENDENCY = "FAILED DEPENDENCY"


class Task(ABC):
    """Atomic unit of concurrent processing. Provides common functionality for derived subclasses"""

    def __init__(self, dependency: List = None):
        """
        :param dependency: (optional, List) list of tasks which must be finished before this task can execute
        """
        self.event = asyncio.Event()
        self.dependency = [] if dependency is None else dependency
        if not isinstance(self.dependency, list):
            self.dependency = [dependency]

        self._semaphore = None
        self._root_out_dir = None
        self._state = TaskState.PENDING
        self.log = init_logger(self.name)

    @property
    def name(self) -> str:
        """returns tasks name"""
        return f"{self.__class__.__name__}"

    @property
    def output_dir(self) -> pathlib.Path:
        """output directory for the file flag to be stored in, can be overridden by a subclass"""
        return self.root_out_dir / self.name

    @property
    def output(self) -> pathlib.Path:
        """output json file path of the done flag indicating the task is done, can be overridden by a subclass"""
        return self.output_dir / 'done.json'

    @property
    def semaphore(self) -> asyncio.Semaphore:
        """synchronization semaphore that allows only certain number of concurrent tasks to run"""
        return self._semaphore

    @semaphore.setter
    def semaphore(self, the_semaphore: asyncio.Semaphore) -> None:
        """semaphore setter"""
        self._semaphore = the_semaphore

    @property
    def root_out_dir(self) -> pathlib.Path:
        """the root directory where all the tasks' subdirectories are located"""
        return self._root_out_dir

    @root_out_dir.setter
    def root_out_dir(self, the_out_dir: pathlib.Path) -> None:
        """root output directory setter"""
        self._root_out_dir = the_out_dir

    def done(self) -> bool:
        """
        if the task's done file flag exists returns True, else False, can be overridden by a subclass
        :return: (bool)
        """
        return self.output.exists()

    def output_data(self) -> Dict:
        """
        output dictionary that will be stored in the task's done flag, can be overridden/extended by a subclass
        :return: (Dict)
        """
        return {"task_name": self.__class__.__name__}

    async def create_output(self) -> None:
        """
        async method for creating task's done flag, can be overridden by a subclass
        :return:
        """
        self.output_dir.mkdir(parents=True, exist_ok=True)
        data_str = json.dumps(self.output_data(), sort_keys=True)
        async with aiofiles.open(self.output, 'w') as h:
            await h.write(data_str)

        self.log.info("output_created")

    @abstractmethod
    async def run_task(self) -> None:
        """the actual processing happens here, async method that must be implemented by a subclass, """
        raise NotImplementedError("Shall be implemented by a subclass")

    async def _execute(self) -> None:
        """handles semaphore, wraps the run_task method, handles exceptions and creates task's output file flag"""
        await self.semaphore.acquire()
        try:
            self.log.info("semaphore acquired")
            await self.run_task()
            self._state = TaskState.DONE
            await self.create_output()
            self.log.info("executed")
        except BaseException as ex:
            self.log.exception(ex)
            self._state = TaskState.FAILED
        finally:
            self.semaphore.release()
            self.log.info("semaphore released")
            self.log.info("event set")

    def _dependencies_done(self):
        """checks if all dependencies are done, if yes then returns True, else False"""
        states = [d.done() for d in self.dependency]
        return all(states)

    async def _wait_for_dependencies(self):
        """async method to wait for all dependencies that are not done to finish"""
        for d in self.dependency:
            if not d.done():
                await d.event.wait()

    async def _handle_dependencies(self) -> bool:
        """main handler of dependencies, if the task can be executed returns True else False"""
        ret = True
        if self._dependencies_done():
            self.log.info("dependencies done")
        else:
            self.log.info("dependencies not done, waiting...")
            await self._wait_for_dependencies()
            self.log.info("dependencies finally are done, starting execution...")

            if not self._dependencies_done():
                self.log.error("Not all dependencies are done - skipping")
                self._state = TaskState.FAILED_DEPENDENCY
                ret = False
        return ret

    async def execute(self) -> Tuple:
        """
        main async method for resolving if the task shall run, handles dependencies and executes the task
        :return: (Tuple) task's name, task's state
        """
        if self.done():
            self.log.info("already done - skipping")
            self._state = TaskState.DONE
        else:
            self.log.info("not complete")
            run = True
            if len(self.dependency) > 0:
                run = await self._handle_dependencies()

            if run:
                await self._execute()
        self.event.set()
        return self.name, self._state