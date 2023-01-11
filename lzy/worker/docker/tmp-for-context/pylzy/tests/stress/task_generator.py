from random import randint
from uuid import uuid4

from task import LzyTask, durable_task


class TaskGenerator:
    def __init__(self, min_task_duration_minutes, max_task_duration_minutes):
        self.min_task_duration = min_task_duration_minutes
        self.max_task_duration = max_task_duration_minutes

    def create_task(self):
        total_minutes = randint(self.min_task_duration, self.max_task_duration)
        return LzyTask(
            durable_task,
            f"task_on_{total_minutes}_min_{str(uuid4())[:4]}",
            args=(),
            kwargs={"total_minutes": total_minutes, "step_minutes": 1},
        )
