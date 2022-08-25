package ai.lzy.metrics;

import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public final class MetricsGrpcInterceptor {
    private static final Logger LOG = LogManager.getLogger(MetricsGrpcInterceptor.class);

    private static final RequestMetrics requestMetrics = new RequestMetrics("grpc");

    public static ServerInterceptor server(String app) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                         ServerCallHandler<ReqT, RespT> next)
            {
                final var fullMethodName = call.getMethodDescriptor().getFullMethodName();
                final var methodName = getMethodName(fullMethodName);
                final var rm = requestMetrics.begin(app + "_server", methodName);

                return new ForwardingServerCallListener<>() {
                    private ReqT request;
                    private RespT response;

                    private final ServerCall.Listener<ReqT> delegate = next.startCall(
                        new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                            @Override
                            public void sendMessage(RespT message) {
                                response = message;
                                super.sendMessage(message);
                            }

                            @Override
                            public void close(Status status, Metadata trailers) {
                                endRequest(rm, status, request, response);
                                super.close(status, trailers);
                            }
                        }, headers);

                    @Override
                    protected ServerCall.Listener<ReqT> delegate() {
                        return delegate;
                    }

                    @Override
                    public void onMessage(ReqT message) {
                        request = message;
                        super.onMessage(message);
                    }

                    @Override
                    public void onHalfClose() {
                        // rationale: https://github.com/grpc/grpc-java/issues/1552#issuecomment-196955189
                        try {
                            super.onHalfClose();
                        } catch (Throwable e) {
                            LOG.error("Unexpected exception while executing method <{}_server> {}: {}",
                                app, fullMethodName, e.getMessage(), e);
                            endRequest(rm, Status.fromThrowable(e), request, response);
                            throw e;
                        }
                    }
                };
            }
        };
    }

    public static ClientInterceptor client(String app) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next)
            {
                var rm = requestMetrics.begin(app + "_client", getMethodName(method.getFullMethodName()));

                return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                    private ReqT request;
                    private RespT response;

                    @Override
                    public void start(ClientCall.Listener<RespT> rl, Metadata headers) {
                        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(rl) {
                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                endRequest(rm, status, request, response);
                                super.onClose(status, trailers);
                            }

                            @Override
                            public void onMessage(RespT message) {
                                response = message;
                                super.onMessage(message);
                            }
                        }, headers);
                    }

                    @Override
                    public void sendMessage(ReqT message) {
                        request = message;
                        super.sendMessage(message);
                    }
                };
            }
        };
    }

    private static String getMethodName(String fullMethodName) {
        if (fullMethodName.startsWith("ai.lzy.")) {
            return fullMethodName.substring("ai.lzy.".length());
        }
        return fullMethodName;
    }

    private static void endRequest(RequestMetrics.Request rm, Status status, Object request, Object response) {
        var requestSize = messageSize(request);
        var responseSize = messageSize(response);
        rm.end(status.getCode().toString(), requestSize, responseSize);
    }

    private static int messageSize(Object message) {
        // return message instanceof AbstractMessage ? ((AbstractMessage) message).getSerializedSize() : 0;
        return 0;
    }
}
