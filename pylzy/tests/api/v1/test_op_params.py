from unittest import TestCase

from api.v1.mocks import RuntimeMock, StorageRegistryMock
from lzy.api.v1 import Lzy, op
from lzy.api.v1.provisioning import GpuType, Provisioning, CpuType
from lzy.api.v1.call import LzyCall


@op
def func() -> None:
    pass


class LzyOpParamsTests(TestCase):
    def setUp(self):
        self.lzy = Lzy(runtime=RuntimeMock(), storage_registry=StorageRegistryMock())

    def test_description(self):
        description = "my favourite func"

        @op(description=description)
        def func_description() -> None:
            pass

        with self.lzy.workflow("test") as wf:
            func_description()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual(description, call.description)

    def test_invalid_workflow_provisioning(self):
        with self.assertRaisesRegex(ValueError, "cpu_type is not set"):
            with self.lzy.workflow("test", provisioning=Provisioning()):
                func()
        with self.assertRaisesRegex(ValueError, "cpu_count is not set"):
            with self.lzy.workflow("test", provisioning=Provisioning(cpu_type=CpuType.BROADWELL.name)):
                func()
        with self.assertRaisesRegex(ValueError, "ram_size_gb is not set"):
            with self.lzy.workflow("test", provisioning=Provisioning(cpu_type=CpuType.BROADWELL.name, cpu_count=4)):
                func()
        with self.assertRaisesRegex(ValueError, "gpu_type is not set"):
            with self.lzy.workflow("test", provisioning=Provisioning(cpu_type=CpuType.BROADWELL.name, cpu_count=4,
                                                                     ram_size_gb=8)):
                func()
        with self.assertRaisesRegex(ValueError, "gpu_count is not set"):
            with self.lzy.workflow("test", provisioning=Provisioning(cpu_type=CpuType.BROADWELL.name, cpu_count=4,
                                                                     ram_size_gb=8,
                                                                     gpu_type=str(GpuType.NO_GPU.value))):
                func()
        with self.assertRaisesRegex(ValueError, "gpu_type is set to <none> while gpu_count"):
            with self.lzy.workflow("test", provisioning=Provisioning(cpu_type=CpuType.BROADWELL.name, cpu_count=4,
                                                                     ram_size_gb=8, gpu_type=str(GpuType.NO_GPU.value),
                                                                     gpu_count=4)):
                func()

    def test_default_provisioning(self):
        with self.lzy.workflow("test") as wf:
            func()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual(Provisioning.default().cpu_type, call.provisioning.cpu_type)
        self.assertEqual(Provisioning.default().cpu_count, call.provisioning.cpu_count)
        self.assertEqual(Provisioning.default().ram_size_gb, call.provisioning.ram_size_gb)
        self.assertEqual(Provisioning.default().gpu_count, call.provisioning.gpu_count)
        self.assertEqual(Provisioning.default().gpu_type, call.provisioning.gpu_type)

    def test_workflow_provisioning(self):
        with self.lzy.workflow("test", gpu_count=4, gpu_type=GpuType.A100.name) as wf:
            func()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual(Provisioning.default().cpu_type, call.provisioning.cpu_type)
        self.assertEqual(Provisioning.default().cpu_count, call.provisioning.cpu_count)
        self.assertEqual(Provisioning.default().ram_size_gb, call.provisioning.ram_size_gb)
        self.assertEqual(4, call.provisioning.gpu_count)
        self.assertEqual(GpuType.A100.name, call.provisioning.gpu_type)

    def test_op_provisioning(self):
        @op(gpu_count=8, provisioning=Provisioning(gpu_count=4, cpu_count=16))
        def func_with_provisioning() -> None:
            pass

        with self.lzy.workflow("test", gpu_type=GpuType.A100.name, cpu_count=32) as wf:
            func_with_provisioning()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual(Provisioning.default().cpu_type, call.provisioning.cpu_type)
        self.assertEqual(16, call.provisioning.cpu_count)
        self.assertEqual(Provisioning.default().ram_size_gb, call.provisioning.ram_size_gb)
        self.assertEqual(8, call.provisioning.gpu_count)
        self.assertEqual(GpuType.A100.name, call.provisioning.gpu_type)
