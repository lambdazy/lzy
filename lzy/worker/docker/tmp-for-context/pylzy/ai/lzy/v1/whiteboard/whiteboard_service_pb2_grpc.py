# Generated by the gRPC Python protocol compiler plugin. DO NOT EDIT!
"""Client and server classes corresponding to protobuf-defined services."""
import grpc

from ai.lzy.v1.whiteboard import whiteboard_service_pb2 as ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2


class LzyWhiteboardServiceStub(object):
    """Missing associated documentation comment in .proto file."""

    def __init__(self, channel):
        """Constructor.

        Args:
            channel: A grpc.Channel.
        """
        self.Get = channel.unary_unary(
                '/ai.lzy.v1.whiteboard.LzyWhiteboardService/Get',
                request_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.GetRequest.SerializeToString,
                response_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.GetResponse.FromString,
                )
        self.List = channel.unary_unary(
                '/ai.lzy.v1.whiteboard.LzyWhiteboardService/List',
                request_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.ListRequest.SerializeToString,
                response_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.ListResponse.FromString,
                )
        self.CreateWhiteboard = channel.unary_unary(
                '/ai.lzy.v1.whiteboard.LzyWhiteboardService/CreateWhiteboard',
                request_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.CreateWhiteboardRequest.SerializeToString,
                response_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.CreateWhiteboardResponse.FromString,
                )
        self.LinkField = channel.unary_unary(
                '/ai.lzy.v1.whiteboard.LzyWhiteboardService/LinkField',
                request_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.LinkFieldRequest.SerializeToString,
                response_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.LinkFieldResponse.FromString,
                )
        self.FinalizeWhiteboard = channel.unary_unary(
                '/ai.lzy.v1.whiteboard.LzyWhiteboardService/FinalizeWhiteboard',
                request_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.FinalizeWhiteboardRequest.SerializeToString,
                response_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.FinalizeWhiteboardResponse.FromString,
                )


class LzyWhiteboardServiceServicer(object):
    """Missing associated documentation comment in .proto file."""

    def Get(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def List(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def CreateWhiteboard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def LinkField(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')

    def FinalizeWhiteboard(self, request, context):
        """Missing associated documentation comment in .proto file."""
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details('Method not implemented!')
        raise NotImplementedError('Method not implemented!')


def add_LzyWhiteboardServiceServicer_to_server(servicer, server):
    rpc_method_handlers = {
            'Get': grpc.unary_unary_rpc_method_handler(
                    servicer.Get,
                    request_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.GetRequest.FromString,
                    response_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.GetResponse.SerializeToString,
            ),
            'List': grpc.unary_unary_rpc_method_handler(
                    servicer.List,
                    request_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.ListRequest.FromString,
                    response_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.ListResponse.SerializeToString,
            ),
            'CreateWhiteboard': grpc.unary_unary_rpc_method_handler(
                    servicer.CreateWhiteboard,
                    request_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.CreateWhiteboardRequest.FromString,
                    response_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.CreateWhiteboardResponse.SerializeToString,
            ),
            'LinkField': grpc.unary_unary_rpc_method_handler(
                    servicer.LinkField,
                    request_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.LinkFieldRequest.FromString,
                    response_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.LinkFieldResponse.SerializeToString,
            ),
            'FinalizeWhiteboard': grpc.unary_unary_rpc_method_handler(
                    servicer.FinalizeWhiteboard,
                    request_deserializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.FinalizeWhiteboardRequest.FromString,
                    response_serializer=ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.FinalizeWhiteboardResponse.SerializeToString,
            ),
    }
    generic_handler = grpc.method_handlers_generic_handler(
            'ai.lzy.v1.whiteboard.LzyWhiteboardService', rpc_method_handlers)
    server.add_generic_rpc_handlers((generic_handler,))


 # This class is part of an EXPERIMENTAL API.
class LzyWhiteboardService(object):
    """Missing associated documentation comment in .proto file."""

    @staticmethod
    def Get(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ai.lzy.v1.whiteboard.LzyWhiteboardService/Get',
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.GetRequest.SerializeToString,
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.GetResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def List(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ai.lzy.v1.whiteboard.LzyWhiteboardService/List',
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.ListRequest.SerializeToString,
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.ListResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def CreateWhiteboard(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ai.lzy.v1.whiteboard.LzyWhiteboardService/CreateWhiteboard',
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.CreateWhiteboardRequest.SerializeToString,
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.CreateWhiteboardResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def LinkField(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ai.lzy.v1.whiteboard.LzyWhiteboardService/LinkField',
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.LinkFieldRequest.SerializeToString,
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.LinkFieldResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)

    @staticmethod
    def FinalizeWhiteboard(request,
            target,
            options=(),
            channel_credentials=None,
            call_credentials=None,
            insecure=False,
            compression=None,
            wait_for_ready=None,
            timeout=None,
            metadata=None):
        return grpc.experimental.unary_unary(request, target, '/ai.lzy.v1.whiteboard.LzyWhiteboardService/FinalizeWhiteboard',
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.FinalizeWhiteboardRequest.SerializeToString,
            ai_dot_lzy_dot_v1_dot_whiteboard_dot_whiteboard__service__pb2.FinalizeWhiteboardResponse.FromString,
            options, channel_credentials,
            insecure, call_credentials, compression, wait_for_ready, timeout, metadata)
