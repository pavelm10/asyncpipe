import asyncio
import random
from typing import List, Dict
import pytest

from asyncpipe.pipeline import Pipeline, TasksFailedError
from asyncpipe.task import Task


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
        await asyncio.sleep(random.randint(1, 3))


class TaskTroubleMaker(TaskBase):
    def __init__(self, evt: asyncio.Event, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.evt = evt

    async def run_task(self) -> None:
        await super().run_task()
        if self.evt.is_set():
            raise DummyException("I was told to fail, so I did")


class TaskA(TaskBase):
    pass


class TaskB(TaskBase):
    pass


class TaskC(TaskBase):
    pass


class TaskD(TaskBase):
    pass


class DummyPipeline(Pipeline):

    def __init__(self, set_evt: bool = False, fail_softly: bool = False, pass_soft_fail: bool = True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.set_evt = set_evt
        self.fail_softly = fail_softly
        self.propagate_soft_failures = pass_soft_fail

    def define(self) -> List:
        branching_stage_tasks = []
        evt = asyncio.Event()
        if self.set_evt:
            evt.set()
        btask = TaskB(idx=2021)
        for idx in range(0, 10):
            ctask = TaskC(idx=idx, dependency=[btask])
            problem_task = TaskTroubleMaker(evt=evt, idx=idx, dependency=[ctask], fail_softly=self.fail_softly)
            branching_stage_tasks.append(problem_task)

        dtask = TaskD(idx=42, dependency=branching_stage_tasks, propagate_soft_failures=self.propagate_soft_failures)
        return [dtask]


class BranchingPipeline(Pipeline):

    def define(self) -> List:
        atask = TaskA(idx=0)
        btask = TaskB(idx=1, dependency=[atask])
        ctask = TaskC(idx=2, dependency=[btask])
        problem_task = TaskTroubleMaker(idx=3, dependency=[btask], evt=asyncio.Event())
        dtask = TaskD(idx=4, dependency=[ctask, problem_task])
        return [dtask]


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
    assert len(files) == 11

    # then rerun the pipeline, but no task will fail. Only not done tasks will be processed
    pipe = DummyPipeline(root_output_dir=tmp_path, workers=10)
    pipe.run()
    files = list(tmp_path.rglob("*.json"))
    assert len(files) == 22


@pytest.mark.parametrize("pass_soft_fail", (True, False))
def test_pipeline_rerun_with_soft_failures_and_propagating(tmp_path, pass_soft_fail):
    # first run pipeline with a Task softly failing
    pipe = DummyPipeline(set_evt=True, root_output_dir=tmp_path, workers=10,
                         fail_softly=True, pass_soft_fail=pass_soft_fail)
    pipe.run()
    done_files = list(tmp_path.rglob("done.json"))
    soft_fail_files = list(tmp_path.rglob("failed_softly.json"))
    assert len(done_files) == 11 if pass_soft_fail else 12
    assert len(soft_fail_files) == 11 if pass_soft_fail else 10
    if pass_soft_fail:
        assert pipe.task_results['TaskD_42'] == "FAILED SOFTLY"

    # then rerun the pipeline, but no task will fail. Only not done and softly failed tasks will be processed
    pipe = DummyPipeline(root_output_dir=tmp_path, workers=10)
    pipe.run()
    files = list(tmp_path.rglob("done.json"))
    assert len(files) == 22


def test_branching_pipeline(tmp_path):
    pipe = BranchingPipeline(root_output_dir=tmp_path)
    pipe.run()
    files = list(tmp_path.rglob("done.json"))
    assert len(files) == 5
