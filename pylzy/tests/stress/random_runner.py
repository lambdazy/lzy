from multiprocessing import Pool, Process
from random import randint
from time import sleep
from typing import Set

from task_generator import LzyTask, TaskGenerator


class RandomRunner(Process):
    def __init__(
        self,
        task_supplier: TaskGenerator,
        max_task_in_parallel,
        max_rand_sleep_period_sec,
        num_task_to_execute,
    ):
        super().__init__(target=self.cycle, name="main_random_runner")
        self.task_supplier = task_supplier
        self.max_task_in_parallel = max_task_in_parallel
        self.max_rand_sleep_period_sec = max_rand_sleep_period_sec
        self.num_task_to_execute = num_task_to_execute
        self.tasks_counter = 0
        self.tasks_queue: Set[LzyTask] = set()

    def collect_finished_tasks(self):
        tasks_to_remove = set()
        for task in self.tasks_queue:
            if task.ready:
                tasks_to_remove.add(task)
        for task in tasks_to_remove:
            self.tasks_queue.remove(task)

    def cycle(self):
        pool = Pool(processes=self.max_task_in_parallel)

        while self.tasks_counter < self.num_task_to_execute:
            self.collect_finished_tasks()

            if len(self.tasks_queue) < self.max_task_in_parallel:
                task = self.task_supplier.create_task()
                future = pool.apply_async(task)
                self.tasks_queue.add(future)
                self.tasks_counter += 1

            sleep(randint(0, self.max_rand_sleep_period_sec))

        pool.close()
        pool.join()
