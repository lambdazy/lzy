package ai.lzy.util.grpc;

import com.google.protobuf.MessageOrBuilder;
import io.grpc.*;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

public class GrpcLogsInterceptor {
    private static final Logger SERVER_LOG = LogManager.getLogger("GrpcServer");
    private static final Logger CLIENT_LOG = LogManager.getLogger("GrpcClient");

    public static ServerInterceptor server() {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                         ServerCallHandler<ReqT, RespT> next)
            {
                var callId = UUID.randomUUID().toString();
                var methodName = call.getMethodDescriptor().getFullMethodName();

                var grpcServerCall = new ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT>(call) {
                    @Override
                    public void sendMessage(RespT message) {
                        if (SERVER_LOG.isTraceEnabled()) {
                            SERVER_LOG.debug("{}::<{}>: response: ({})", methodName, callId, printMessageSafe(message));
                        } else {
                            SERVER_LOG.debug("{}::<{}>: response: <...>", methodName, callId);
                        }
                        super.sendMessage(message);
                    }
                };

                var listener = next.startCall(grpcServerCall, headers);

                return new GrpcForwardingServerCallListener<>(call.getMethodDescriptor(), listener) {
                    @Override
                    public void onMessage(ReqT message) {
                        if (SERVER_LOG.isTraceEnabled()) {
                            SERVER_LOG.debug("{}::<{}>, request: ({})", methodName, callId, printMessageSafe(message));
                        } else {
                            SERVER_LOG.debug("{}::<{}>, request: <...>", methodName, callId);
                        }
                        super.onMessage(message);
                    }
                };
            }
        };
    }

    public static ClientInterceptor client(String name) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next)
            {
                var methodName = method.getFullMethodName();
                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    @Override
                    public void sendMessage(ReqT message) {
                        if (CLIENT_LOG.isTraceEnabled()) {
                            CLIENT_LOG.debug("{} call {}, request ({})", name, methodName, printMessageSafe(message));
                        } else {
                            CLIENT_LOG.debug("{} call {}, request <...>", name, methodName);
                        }
                        super.sendMessage(message);
                    }
                };
            }
        };
    }


    private static String printMessageSafe(Object message) {
        return message instanceof MessageOrBuilder msg
            ? ProtoPrinter.safePrinter().shortDebugString(msg)
            : message.getClass().getName();
    }

    private static class GrpcForwardingServerCallListener<M, R> extends SimpleForwardingServerCallListener<M> {
        final String methodName;

        protected GrpcForwardingServerCallListener(MethodDescriptor<M, R> method, ServerCall.Listener<M> listener) {
            super(listener);
            methodName = method.getFullMethodName();
        }
    }
}
