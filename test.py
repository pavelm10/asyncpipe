import asyncio
import random
from typing import List, Dict
import pytest

from pipeline import Pipeline, TasksFailedError
from task import Task


class DummyException(Exception):
    pass


class TaskBase(Task):
    def __init__(self, idx: int, *args, **kwargs):
        self.idx = idx
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}_{self.idx}"

    def output_data(self) -> Dict:
        od = super().output_data()
        od['idx'] = self.idx
        return od

    async def run_task(self) -> None:
        await asyncio.sleep(random.randint(1, 5))


class TaskTroubleMaker(TaskBase):
    def __init__(self, evt: asyncio.Event, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.evt = evt

    async def run_task(self) -> None:
        await super().run_task()
        if self.evt.is_set():
            raise DummyException("I was told to fail, so I did")


class TaskB(TaskBase):
    pass


class TaskC(TaskBase):
    pass


class TaskD(TaskBase):
    pass


class DummyPipeline(Pipeline):

    def __init__(self, set_evt: bool = False, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_evt = set_evt

    def define(self) -> List:
        all_tasks = []
        branching_stage_tasks = []
        evt = asyncio.Event()
        if self.set_evt:
            evt.set()
        btask = TaskB(idx=2021)
        for idx in range(0, 10):
            ctask = TaskC(idx=idx, dependency=[btask])
            problem_task = TaskTroubleMaker(evt=evt, idx=idx, dependency=[ctask])
            branching_stage_tasks.extend([problem_task, ctask])

        dtask = TaskD(idx=42, dependency=branching_stage_tasks)
        all_tasks.append(btask)
        all_tasks.extend(branching_stage_tasks)
        all_tasks.append(dtask)
        return all_tasks


@pytest.mark.parametrize("workers", (10, 20))
def test_pipeline_success(workers, tmp_path):
    pipe = DummyPipeline(root_output_dir=tmp_path, workers=workers)
    pipe.run()
    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 22


def test_pipeline_rerun(tmp_path):
    # first run pipeline with a Task failing
    pipe = DummyPipeline(set_evt=True, root_output_dir=tmp_path, workers=10)
    with pytest.raises(TasksFailedError):
        pipe.run()
        files = list(tmp_path.rglob("*.json"))
        assert len(files) == 1

    # then rerun the pipeline, but no task will fail. Only not done tasks will be processed
    pipe = DummyPipeline(root_output_dir=tmp_path, workers=10)
    pipe.run()
    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 22
