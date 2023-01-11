# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from ai.lzy.v1.workflow import workflow_private_service_pb2 as ai_dot_lzy_dot_v1_dot_workflow_dot_workflow__private__service__pb2


class LzyWorkflowPrivateServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.StopWorkflow = channel.unary_unary(
                '/ai.lzy.v1.workflow.LzyWorkflowPrivateService/StopWorkflow',
                request_serializer=ai_dot_lzy_dot_v1_dot_workflow_dot_workflow__private__service__pb2.StopWorkflowRequest.SerializeToString,
                response_deserializer=ai_dot_lzy_dot_v1_dot_workflow_dot_workflow__private__service__pb2.StopWorkflowResponse.FromString,
                )


class LzyWorkflowPrivateServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def StopWorkflow(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_LzyWorkflowPrivateServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'StopWorkflow': grpc.unary_unary_rpc_method_handler(
                    servicer.StopWorkflow,
                    request_deserializer=ai_dot_lzy_dot_v1_dot_workflow_dot_workflow__private__service__pb2.StopWorkflowRequest.FromString,
                    response_serializer=ai_dot_lzy_dot_v1_dot_workflow_dot_workflow__private__service__pb2.StopWorkflowResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'ai.lzy.v1.workflow.LzyWorkflowPrivateService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class LzyWorkflowPrivateService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def StopWorkflow(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ai.lzy.v1.workflow.LzyWorkflowPrivateService/StopWorkflow',
            ai_dot_lzy_dot_v1_dot_workflow_dot_workflow__private__service__pb2.StopWorkflowRequest.SerializeToString,
            ai_dot_lzy_dot_v1_dot_workflow_dot_workflow__private__service__pb2.StopWorkflowResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
