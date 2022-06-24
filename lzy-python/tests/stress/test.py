from random_runner import RandomRunner
from task_generator import TaskGenerator


def test_only_one_task_in_parallel():
    task_generator = TaskGenerator(
        min_task_duration_minutes=2, max_task_duration_minutes=3
    )

    runner = RandomRunner(
        task_generator,
        max_task_in_parallel=1,
        max_rand_sleep_period_sec=100,
        num_task_to_execute=3,
    )

    runner.start()
    runner.join()


def test_two_task_in_parallel():
    task_generator = TaskGenerator(
        min_task_duration_minutes=1, max_task_duration_minutes=3
    )

    runner = RandomRunner(
        task_generator,
        max_task_in_parallel=2,
        max_rand_sleep_period_sec=10,
        num_task_to_execute=6,
    )

    runner.start()
    runner.join()


if __name__ == "__main__":
    test_only_one_task_in_parallel()
    test_two_task_in_parallel()
