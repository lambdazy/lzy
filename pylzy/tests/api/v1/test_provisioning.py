import pytest

from dataclasses import fields

from ai.lzy.v1.workflow.workflow_pb2 import VmPoolSpec
from lzy.api.v1.provisioning import Provisioning


def test_proto_relation():
    """
    This test is about us maintaining Provisioning.proto_relation table up to date
    """

    proto_relation = Provisioning.proto_relation

    provisioning_fields = {field.name for field in fields(Provisioning)}
    provisioning_fields_to_ignore = {'score_function'}

    proto_fields = {field.name for field in VmPoolSpec.DESCRIPTOR.fields}
    proto_fields_to_ignore = {'poolSpecName', 'zones'}

    assert set(proto_relation.keys()) == provisioning_fields - provisioning_fields_to_ignore
    assert set(proto_relation.values()) == proto_fields - proto_fields_to_ignore


def test_override_interface():
    provisioning = Provisioning()

    with pytest.raises(TypeError):
        provisioning.override(Provisioning(), cpu_count=1)

    with pytest.raises(TypeError):
        provisioning.override()


def test_data_up_to_date(vm_pool_specs):
    for spec in vm_pool_specs:
        assert spec.DESCRIPTOR.fields == VmPoolSpec.DESCRIPTOR.fields
