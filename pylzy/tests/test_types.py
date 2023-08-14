from dataclasses import fields

from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec
from lzy.types import VM_SPEC_PROTO_RELATION, VmResources, VmSpec


def test_proto_relation():
    """
    This test is about us maintaining VM_SPEC_PROTO_RELATION table up to date
    """

    real_vm_spec_fields = {field.name for field in fields(VmSpec)}
    table_vm_spec_fields = set(VM_SPEC_PROTO_RELATION.keys())

    real_proto_fields = {field.name for field in VmPoolSpec.DESCRIPTOR.fields}
    table_proto_fields = set(VM_SPEC_PROTO_RELATION.values())
    proto_fields_to_ignore = {'poolSpecName', 'zones'}

    assert table_vm_spec_fields == real_vm_spec_fields
    assert table_proto_fields == real_proto_fields - proto_fields_to_ignore

    # no duplicates
    assert len(table_proto_fields) == len(table_vm_spec_fields)


def test_data_up_to_date(vm_pool_specs):
    for spec in vm_pool_specs:
        assert spec.DESCRIPTOR.fields == VmPoolSpec.DESCRIPTOR.fields


def test_from_proto(vm_pool_specs):
    for spec in vm_pool_specs:
        # mostly this test checks that .from_proto method is not failing on test data
        vm_spec = VmSpec.from_proto(spec)
        assert isinstance(vm_spec, VmSpec)
        assert isinstance(vm_spec, VmResources)
