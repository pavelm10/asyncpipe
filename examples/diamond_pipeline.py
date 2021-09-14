import asyncio
import pathlib
from typing import List, Dict
import random

from pipeline import Pipeline
from task import Task


class TaskBase(Task):
    def __init__(self, idx: int, *args, **kwargs):
        self.idx = idx
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        return f"{self.__class__.__name__}_{self.idx}"

    def output_data(self) -> Dict:
        od = super().output_data()
        od['task_id'] = self.idx
        return od

    async def run_task(self) -> None:
        self.log.info(f"running with ID: {self.idx}")
        await asyncio.sleep(random.randint(1, 5))


class TaskA(TaskBase):
    pass


class TaskB(TaskBase):
    pass


class TaskC(TaskBase):

    async def run_task(self) -> None:
        self.log.info(f"running with ID: {self.idx}")
        files = sorted(self.root_out_dir.rglob("*.json"))
        for f in files:
            self.log.info(f)


class TaskD(TaskBase):

    async def run_task(self) -> None:
        self.log.info(f"running with ID: {self.idx}")
        self.log.info("Just starting, doing nothing interesting")
        await asyncio.sleep(random.randint(1, 5))


class DiamondPipeline(Pipeline):

    def define(self) -> List:
        d_task = TaskD(idx=111)
        branch_tasks = []
        for idx in range(10):
            a_task = TaskA(idx=idx, dependency=[d_task])
            b_task = TaskB(idx=idx, dependency=[a_task])
            branch_tasks.append(b_task)
        c_task = TaskC(idx=666, dependency=branch_tasks)
        return [c_task]


if __name__ == "__main__":
    import argparse
    import shutil

    argp = argparse.ArgumentParser()
    argp.add_argument('-o', '--out-dir', help='path to the output directory', required=False)
    argp.add_argument('-w', '--workers', help='number of concurrent workers', type=int, default=5)

    pargs = argp.parse_args()
    if pargs.out_dir is None:
        out = pathlib.Path("./test_out")
    else:
        out = pathlib.Path(pargs.out_dir)
    if out.exists():
        shutil.rmtree(out.as_posix())
    out.mkdir()

    pipe = DiamondPipeline(root_output_dir=out, workers=pargs.workers)
    pipe.run()
