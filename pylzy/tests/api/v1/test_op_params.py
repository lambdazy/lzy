import pytest
from unittest import TestCase

from tests.api.v1.mocks import RuntimeMock, StorageRegistryMock, EnvProviderMock
from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec
from lzy.api.v1 import Lzy, op, Env, DockerPullPolicy
from lzy.api.v1.provisioning import (
    CpuType,
    GpuType,
    Provisioning,
    maximum_score_function,
    minimum_score_function,
)
from lzy.api.v1.call import LzyCall
from lzy.api.v1.workflow import LzyWorkflow
from lzy.exceptions import BadProvisioning
from platform import python_version


@op
def func() -> None:
    pass


@pytest.mark.usefixtures('vm_pool_specs', 'vm_pool_spec_large', 'vm_pool_spec_small')
class LzyOpParamsTests(TestCase):
    vm_pool_specs = None
    vm_pool_spec_large = None
    vm_pool_spec_small = None

    def setUp(self):
        assert self.vm_pool_specs
        assert self.vm_pool_spec_large
        assert self.vm_pool_spec_small

        self.lzy = Lzy(
            runtime=RuntimeMock(
                vm_pool_specs=self.vm_pool_specs,
            ),
            storage_registry=StorageRegistryMock(),
            py_env_provider=EnvProviderMock({"pylzy": "0.0.0"}, ["local_module_path"])
        )

    def assert_pool_spec(self, wf: LzyWorkflow, etalon: VmPoolSpec):
        call: LzyCall = wf.owner.runtime.calls[0]
        vm_pool_spec = call._vm_pool_spec  # it is assigned at mock runtime

        self.assertEqual(vm_pool_spec, etalon)

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
        provisioning = Provisioning(
            gpu_type=GpuType.NO_GPU.value,
            gpu_count=4
        )
        with self.assertRaisesRegex(BadProvisioning, r"gpu_type is set to NO_GPU while gpu_count"):
            with self.lzy.workflow("test", provisioning=provisioning):
                func()

    def test_unavailable_provisioning(self):
        for field, value in (
            ('cpu_count', 1000),
            ('gpu_count', 1001),
            ('ram_size_gb', 1002),
            ('cpu_type', 'foo'),
            ('gpu_type', 'bar'),
        ):
            kwargs = {field: value}

            with self.assertRaisesRegex(BadProvisioning, r"not a single one available spec from"):
                with self.lzy.workflow("test", **kwargs):
                    func()

    def test_default_provisioning(self):
        with self.lzy.workflow("test") as wf:
            func()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        provisioning: Provisioning = call.provisioning

        self.assertEqual(None, provisioning.cpu_type)
        self.assertEqual(None, provisioning.gpu_type)
        self.assertEqual(None, provisioning.cpu_count)
        self.assertEqual(None, provisioning.ram_size_gb)
        self.assertEqual(None, provisioning.gpu_count)

        self.assertEqual('', provisioning.cpu_type_final)
        self.assertEqual('', provisioning.gpu_type_final)
        self.assertEqual(0, provisioning.cpu_count_final)
        self.assertEqual(0, provisioning.ram_size_gb_final)
        self.assertEqual(0, provisioning.gpu_count_final)

        self.assert_pool_spec(wf, self.vm_pool_spec_small)

    def test_workflow_provisioning(self):
        with self.lzy.workflow("test", gpu_count=1, gpu_type=GpuType.V100.name) as wf:
            func()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        provisioning = call.provisioning

        self.assertIsNone(None, provisioning.cpu_type)
        self.assertIsNone(None, provisioning.cpu_count)
        self.assertIsNone(None, provisioning.ram_size_gb)
        self.assertEqual(1, provisioning.gpu_count)
        self.assertEqual(GpuType.V100.name, provisioning.gpu_type)

        self.assert_pool_spec(wf, self.vm_pool_spec_large)

    def test_op_provisioning(self):
        @op(gpu_count=1, provisioning=Provisioning(gpu_count=4, cpu_count=8))
        def func_with_provisioning() -> None:
            pass

        with self.lzy.workflow("test", gpu_type=GpuType.V100.name, cpu_count=32, gpu_count=1) as wf:
            func_with_provisioning()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        provisioning = call.provisioning

        self.assertEqual(None, provisioning.cpu_type)
        self.assertEqual(8, provisioning.cpu_count)
        self.assertEqual(None, provisioning.ram_size_gb)
        self.assertEqual(1, provisioning.gpu_count)
        self.assertEqual(GpuType.V100.name, provisioning.gpu_type)

        self.assert_pool_spec(wf, self.vm_pool_spec_large)

    def test_op_provisioning_invalid(self):
        @op(gpu_count=8)
        def func_with_provisioning() -> None:
            pass

        with self.assertRaisesRegex(BadProvisioning, "gpu_type is set to NO_GPU while gpu_count"):
            with self.lzy.workflow("test", gpu_type=str(GpuType.NO_GPU.value)):
                func_with_provisioning()

    def test_invalid_workflow_env(self):
        with self.assertRaisesRegex(ValueError, "Python version & libraries cannot be overriden if conda yaml is set"):
            with self.lzy.workflow("test", python_version="3.9.15", env=Env(conda_yaml_path="my_file")):
                func()

        with self.assertRaisesRegex(ValueError, "Python version & libraries cannot be overriden if conda yaml is set"):
            with self.lzy.workflow("test", libraries={"pylzy": "1.1.1"}, env=Env(conda_yaml_path="my_file")):
                func()

        with self.assertRaisesRegex(ValueError, "Python version & libraries cannot be overriden if conda yaml is set"):
            with self.lzy.workflow("test", env=Env(conda_yaml_path="my_file", libraries={"pylzy": "1.1.1"})):
                func()

        with self.assertRaisesRegex(ValueError, "Python version & libraries cannot be overriden if conda yaml is set"):
            with self.lzy.workflow("test", env=Env(conda_yaml_path="my_file", python_version="3.9.15")):
                func()

        with self.assertRaisesRegex(ValueError, "docker_image is set but docker_pull_policy is not"):
            # noinspection PyTypeChecker
            with self.lzy.workflow("test", docker_image="lzy", docker_pull_policy=None):
                func()

    def test_provisioning_score_func(self):
        with self.lzy.workflow("test") as wf:
            func()

        self.assert_pool_spec(wf, self.vm_pool_spec_small)

        with self.lzy.workflow(
            "test",
            provisioning=Provisioning(score_function=minimum_score_function)
        ) as wf:
            func()

        self.assert_pool_spec(wf, self.vm_pool_spec_small)

        with self.lzy.workflow(
            "test",
            provisioning=Provisioning(score_function=maximum_score_function)
        ) as wf:
            func()

        self.assert_pool_spec(wf, self.vm_pool_spec_large)

    def test_default_env(self):
        with self.lzy.workflow("test") as wf:
            func()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual(python_version(), call.env.python_version)
        self.assertTrue("pylzy" in call.env.libraries)
        self.assertIsNone(call.env.conda_yaml_path)
        self.assertIsNone(call.env.docker_image)
        self.assertTrue(len(call.env.local_modules_path) >= 1)

    def test_workflow_env(self):
        @op
        def func_without_env() -> None:
            pass

        with self.lzy.workflow("test",
                               python_version="3.8.6",
                               libraries={"pylzy": "1.1.1"},
                               local_modules_path=['/a/b/c']) as wf:
            func_without_env()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual("3.8.6", call.env.python_version)
        self.assertTrue("pylzy" in call.env.libraries)
        self.assertTrue("/a/b/c" in call.env.local_modules_path)
        self.assertIsNone(call.env.conda_yaml_path)
        self.assertIsNone(call.env.docker_image)

    def test_op_env(self):
        @op(python_version="3.9.15", libraries={"cloudpickle": "1.1.1"})
        def func_with_env() -> None:
            pass

        with self.lzy.workflow("test", python_version="3.8.6", env=Env(libraries={"pylzy": "1.1.1"})) as wf:
            func_with_env()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual("3.9.15", call.env.python_version)
        self.assertTrue("pylzy" in call.env.libraries)
        self.assertTrue("cloudpickle" in call.env.libraries)
        self.assertIsNone(call.env.conda_yaml_path)
        self.assertIsNone(call.env.docker_image)

    def test_op_env_invalid(self):
        @op(python_version="3.9.15")
        def func_with_py() -> None:
            pass

        @op(libraries={"cloudpickle": "1.1.1"})
        def func_with_libs() -> None:
            pass

        with self.assertRaisesRegex(ValueError, "Python version & libraries cannot be overriden if conda yaml is set"):
            with self.lzy.workflow("test", conda_yaml_path="yaml"):
                func_with_py()

        with self.assertRaisesRegex(ValueError, "Python version & libraries cannot be overriden if conda yaml is set"):
            with self.lzy.workflow("test", conda_yaml_path="yaml"):
                func_with_libs()

    def test_op_env_yaml(self):
        @op(conda_yaml_path="path_op")
        def func_with_env() -> None:
            pass

        with self.lzy.workflow("test", conda_yaml_path="path_wf") as wf:
            func_with_env()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertIsNone(call.env.python_version)
        self.assertEqual({}, call.env.libraries)
        self.assertEqual("path_op", call.env.conda_yaml_path)
        self.assertIsNone(call.env.docker_image)
        self.assertTrue(len(call.env.local_modules_path) > 0)

    def test_wf_env_yaml(self):
        with self.lzy.workflow("test", conda_yaml_path="path_wf") as wf:
            func()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertIsNone(call.env.python_version)
        self.assertEqual({}, call.env.libraries)
        self.assertEqual("path_wf", call.env.conda_yaml_path)
        self.assertIsNone(call.env.docker_image)
        self.assertTrue(len(call.env.local_modules_path) > 0)

    def test_docker_wf(self):
        with self.lzy.workflow("test", docker_image='image1') as wf:
            func()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertIsNotNone(call.env.python_version)
        self.assertTrue(len(call.env.libraries) > 0)
        self.assertEqual("image1", call.env.docker_image)
        self.assertIsNone(call.env.conda_yaml_path)
        self.assertTrue(len(call.env.local_modules_path) > 0)
        self.assertEqual(DockerPullPolicy.IF_NOT_EXISTS, call.env.docker_pull_policy)

    def test_docker_op(self):
        @op(docker_image="image2", docker_pull_policy=DockerPullPolicy.ALWAYS)
        def func_with_docker() -> None:
            pass

        with self.lzy.workflow("test", docker_image='image1') as wf:
            func_with_docker()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertIsNotNone(call.env.python_version)
        self.assertTrue(len(call.env.libraries) > 0)
        self.assertEqual("image2", call.env.docker_image)
        self.assertIsNone(call.env.conda_yaml_path)
        self.assertTrue(len(call.env.local_modules_path) > 0)
        self.assertEqual(DockerPullPolicy.ALWAYS, call.env.docker_pull_policy)

    def test_docker_with_conda(self):
        @op(docker_image="image2", conda_yaml_path="path1")
        def func_with_docker() -> None:
            pass

        with self.lzy.workflow("test", docker_image='image1') as wf:
            func_with_docker()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertIsNone(call.env.python_version)
        self.assertEqual({}, call.env.libraries)
        self.assertEqual("image2", call.env.docker_image)
        self.assertEqual("path1", call.env.conda_yaml_path)
        self.assertTrue(len(call.env.local_modules_path) > 0)

    def test_env_variables(self):

        @op(env_variables={"a": "a1", "b": "b"})
        def foo() -> None:
            pass

        with self.lzy.workflow("test", env_variables={"a": "a2", "c": "c"}) as wf:
            foo()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertEqual(call.env.env_variables, {"a": "a1", "b": "b", "c": "c"})

    def test_local_modules(self):
        @op
        def func_with_env() -> None:
            pass

        with self.lzy.workflow("test", local_modules_path=["lol_kek"]) as wf:
            func_with_env()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertTrue("lol_kek" in call.env.local_modules_path)

    def test_local_modules_op(self):
        @op(local_modules_path=["lol_kek"])
        def func_with_env() -> None:
            pass

        with self.lzy.workflow("test") as wf:
            func_with_env()

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertTrue("lol_kek" in call.env.local_modules_path)

    def test_op_with_default_cache_params(self):
        default_version = "0.0"

        @op
        def cached_op(a: int) -> str:
            return f"Value: {a}"

        with self.lzy.workflow("test") as wf:
            cached_op(42)

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertFalse(call.cache)
        self.assertEqual(default_version, call.version)

    def test_cached_op(self):
        version = "1.0"

        @op(cache=True, version=version)
        def cached_op(a: int) -> str:
            return f"Value: {a}"

        with self.lzy.workflow("test") as wf:
            cached_op(42)

        # noinspection PyUnresolvedReferences
        call: LzyCall = wf.owner.runtime.calls[0]
        self.assertTrue(call.cache)
        self.assertEqual(version, call.version)
