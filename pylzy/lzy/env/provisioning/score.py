from __future__ import annotations

from typing import Callable

from lzy.types import VmResources

ScoreFunctionType = Callable[[VmResources, VmResources], float]

DEFAULT_WEIGHTS = {
    'gpu': 100.0,
    'cpu': 20.0,
    'ram': 1.0
}


def maximum_score_function(requested: VmResources, spec: VmResources) -> float:
    """Score function that scores big machines the max.
    Use it when you need the best spec available.
    """

    d_gpu = spec.gpu_count - requested.gpu_count
    d_cpu = spec.cpu_count - requested.cpu_count
    d_ram = spec.ram_size_gb - requested.ram_size_gb

    return d_gpu * DEFAULT_WEIGHTS['gpu'] + d_cpu * DEFAULT_WEIGHTS['cpu'] + d_ram * DEFAULT_WEIGHTS['ram']


def minimum_score_function(requested: VmResources, spec: VmResources) -> float:
    """Score function that scores small machines the max.
    Use it when you need smallest spec available that matches you requirements.

    """

    max_score = maximum_score_function(requested=requested, spec=spec)
    return 1.0 / max_score if max_score else float("inf")
