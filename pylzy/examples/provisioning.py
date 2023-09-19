from __future__ import annotations

from lzy.api.v1 import op, Lzy, Provisioning, provisioning, AnyProvisioning
from lzy.types import VmResources


@op
def example1() -> int:
    return 1


@provisioning(cpu_count=8)
@op
def example2() -> int:
    return 2


@provisioning(gpu_count=AnyProvisioning)
@op
def example3() -> int:
    return 3


if __name__ == '__main__':
    lzy = Lzy()
    with lzy.workflow("example_no_provisioning_arguments"):
        # op with no provisioning arguments at all
        # will auto-choose minimal available pool with default "lp.minimum_score_function"
        result1 = example1()

    with lzy.workflow("example_standard_score_function").with_provisioning(
        Provisioning(score_function=lp.maximum_score_function, cpu_count=1)
    ):
        # you can change score function, but we do not made shortcut argument for it,
        # so you need to pass provisioning object
        result2 = example1()

    def my_score_function(requested: VmResources, spec: VmResources) -> float:
        return 1.0 * requested.gpu_count_final * spec.cpu_count_final

    with lzy.workflow("example_custom_score_function").with_provisioning(
        lp.Provisioning(score_function=my_score_function)
    ):
        # or make your own score function
        result3 = example1()

    with lzy.workflow("provisioning_example_merge_paramenters").with_provisioning(
        cpu_count=4,
        gpu_count=1
    ):
        # this op inherits workflow cpu_count=4, gpu_count=1, ram_size_gb=Any
        result4 = example1()

        # this op inherits workflow gpu_count=1, ram_size_gb=Any, but cpu_count
        # will be overrided by op cpu_count=8 argument
        result5 = example2()

        # and this op ingerit cpu_count=4, ram_size_gb=Any from workflow,
        # but we removed requirement gpu_count=1 from this op by
        # passing gpu_count=Any to this operation arguments
        result6 = example3()

    print(result1, result2, result3, result4, result5, result6)
