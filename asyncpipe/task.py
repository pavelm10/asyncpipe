import asyncio
from abc import ABC, abstractmethod
import json
import pathlib
from typing import Dict, List, Tuple

import aiofiles

from asyncpipe.utils import init_logger

_LOGGER = init_logger(__name__)


class TaskState:
    """definition of possible task's states"""

    PENDING = 'PENDING'
    FAILED = 'FAILED'
    DONE = 'DONE'
    FAILED_DEPENDENCY = "FAILED DEPENDENCY"
    FAILED_SOFTLY = "FAILED SOFTLY"


class Task(ABC):
    """Atomic unit of concurrent processing.
    Provides common functionality for derived subclasses"""

    def __init__(
        self,
        dependency: List = None,
        fail_softly: bool = False,
        propagate_soft_failures: bool = True,
    ):
        """
        :param dependency: (optional, List) list of tasks which must be
            finished before this task can execute
        :param fail_softly: (optional, bool) if set True and the task raises
            exception than the state will be FAILED SOFTLY allowing
            downstream tasks to execute. It is useful when the pipeline
            contains many branches which eventually merge into one aggregation
            final task. If there are some branches which failed softly then
            the aggregation task can still execute and provide at least
            partial results. ``propagate_soft_failures`` must be set False in
            order to execute the aggregation task, else it will also finish
            FAILED SOFTLY. When a task fails softly it creates soft failure
            file flag instead of regular output file flag.
        :param propagate_soft_failures: (optional, bool) if set True and
            upstream task(s) failed softly, then this task will not execute
            and will also create soft failure output flag hence propagating
            the soft failure downstream. If set false, it will try to execute
            itself not propagating upstream soft failures downstream.
        """
        self.event = asyncio.Event()
        self.dependency = [] if dependency is None else dependency
        self.fail_softly = fail_softly
        self.propagate_soft_failures = propagate_soft_failures
        if not isinstance(self.dependency, list):
            self.dependency = [dependency]

        self._semaphore = None
        self._root_out_dir = None
        self._state = TaskState.PENDING

    @property
    def name(self) -> str:
        """returns tasks name"""
        return f"{self.__class__.__name__}"

    @property
    def output_dir(self) -> pathlib.Path:
        """output directory for the file flag to be stored in, can be
        overridden by a subclass"""
        return self.root_out_dir / self.name

    @property
    def output(self) -> pathlib.Path:
        """output json file path of the done flag indicating the task is done,
        can be overridden by a subclass"""
        return self.output_dir / 'done.json'

    @property
    def soft_failure_output(self) -> pathlib.Path:
        """
        output json file path of the failed softly flag indicating the task
        failed softly, can be overridden by a subclass
        """
        return self.output_dir / 'failed_softly.json'

    @property
    def semaphore(self) -> asyncio.Semaphore:
        """synchronization semaphore that allows only certain number of
        concurrent tasks to run"""
        return self._semaphore

    @semaphore.setter
    def semaphore(self, the_semaphore: asyncio.Semaphore) -> None:
        """semaphore setter"""
        self._semaphore = the_semaphore

    @property
    def root_out_dir(self) -> pathlib.Path:
        """the root directory where all the tasks' subdirectories
        are located"""
        return self._root_out_dir

    @root_out_dir.setter
    def root_out_dir(self, the_out_dir: pathlib.Path) -> None:
        """root output directory setter"""
        self._root_out_dir = the_out_dir

    def _format_log_message(self, message) -> str:
        return '%s - %s', self.name, message

    def log_info(self, message: str):
        _LOGGER.info(self._format_log_message(message))

    def log_error(self, message: str):
        _LOGGER.error(self._format_log_message(message))

    def log_exception(self, message: str):
        _LOGGER.exception(self._format_log_message(message))

    def log_warning(self, message: str):
        _LOGGER.warning(self._format_log_message(message))

    def done(self) -> bool:
        """
        if the task's done file flag exists returns True, else False,
        can be overridden by a subclass
        :return: (bool)
        """
        return self.output.exists()

    def failed_softly(self) -> bool:
        """
        if the task's failed softly file flag exists returns True, else False,
        can be overridden by a subclass
        :return: (bool)
        """
        return self.soft_failure_output.exists()

    def remove_soft_failure_flag(self) -> None:
        """if soft failure flag exists, removes it"""
        if self.failed_softly():
            self.soft_failure_output.unlink()

    def output_data(self) -> Dict[str, str]:
        """
        output dictionary that will be stored in the task's done flag,
        can be overridden/extended by a subclass
        :return: (Dict)
        """
        return {'task_name': self.__class__.__name__}

    async def create_soft_failure_output(self) -> None:
        """
        async method for creating task's soft failure flag,
        can be overridden by a subclass
        :return:
        """
        await self._write_output(self.soft_failure_output)
        self.log_info('soft failure output created')

    async def create_output(self) -> None:
        """
        async method for creating task's done flag,
        can be overridden by a subclass
        :return:
        """
        await self._write_output(self.output)
        self.log_info('output created')

    async def _write_output(self, out_path: pathlib.Path) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        data = self.output_data()
        data['state'] = self._state
        data_str = json.dumps(data, sort_keys=True)
        async with aiofiles.open(out_path, 'w') as file:
            await file.write(data_str)

    @abstractmethod
    async def run_task(self) -> None:
        """the actual processing happens here, async method that must be
        implemented by a subclass,"""
        raise NotImplementedError("Shall be implemented by a subclass")

    async def _execute(self) -> None:
        """handles semaphore, wraps the run_task method, handles exceptions
        and creates task's output file flag"""
        await self.semaphore.acquire()
        try:
            self.log_info('semaphore acquired')
            await self.run_task()
            self._state = TaskState.DONE
            await self.create_output()
            self.log_info('executed')
        except BaseException as exc:
            self.log_exception(exc)
            self._state = (
                TaskState.FAILED_SOFTLY
                if self.fail_softly
                else TaskState.FAILED
            )
            if self._state == TaskState.FAILED_SOFTLY:
                await self.create_soft_failure_output()
        finally:
            self.semaphore.release()
            self.log_info('semaphore released')

    def _dependencies_done(self) -> bool:
        """checks if all dependencies are done, if yes then returns True,
        else False"""
        states = [d.done() for d in self.dependency]
        return all(states)

    def _dependencies_failed_softly(self) -> bool:
        """checks if any dependency failed softly, if yes then returns True,
        else False"""
        states = [d.failed_softly() for d in self.dependency]
        return any(states)

    async def _wait_for_dependencies(self):
        """async method to wait for all dependencies that are not done
        to finish"""
        for dep in self.dependency:
            if not dep.done() and not dep.failed_softly():
                await dep.event.wait()

    async def _handle_dependencies(self) -> bool:
        """main handler of dependencies, if the task can be executed returns
        True else False"""
        ret = True
        if self._dependencies_done():
            self.log_info('dependencies done')
        else:
            self.log_info('dependencies not done, waiting...')
            await self._wait_for_dependencies()
            if self._dependencies_failed_softly():
                self.log_warning('At least one dependency failed softly')
                ret = not self.propagate_soft_failures
                if not ret:
                    self.log_warning(
                        'Propagating soft failures - soft failing as well'
                    )
                    self._state = TaskState.FAILED_SOFTLY
                    await self.create_soft_failure_output()
            elif not self._dependencies_done():
                self.log_error('Not all dependencies are done - skipping')
                self._state = TaskState.FAILED_DEPENDENCY
                ret = False
            else:
                self.log_info(
                    'dependencies finally are done, starting execution...'
                )

        return ret

    async def execute(self) -> Tuple[str, str]:
        """
        main async method for resolving if the task shall run, handles
        dependencies and executes the task
        :return: (Tuple) task's name, task's state
        """
        if self.done():
            self.log_info('already done - skipping')
            self._state = TaskState.DONE
        else:
            self.log_info('not complete')
            run = True
            if len(self.dependency) > 0:
                run = await self._handle_dependencies()

            if run:
                await self._execute()
        self.event.set()
        self.log_info('event set')
        return self.name, self._state
