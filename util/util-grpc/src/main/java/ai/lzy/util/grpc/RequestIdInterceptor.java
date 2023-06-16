package ai.lzy.util.grpc;

import ai.lzy.logs.LogContextKey;
import io.grpc.*;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.UUID;

public class RequestIdInterceptor {
    private static final Logger LOG = LogManager.getLogger(RequestIdInterceptor.class);

    public static ServerInterceptor forward() {
        return create(false);
    }

    public static ServerInterceptor generate() {
        return create(true);
    }

    private static ServerInterceptor create(boolean generateRequestId) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                         Metadata headers,
                                                                         ServerCallHandler<ReqT, RespT> next)
            {
                if (generateRequestId && GrpcHeaders.getRequestId() != null) {
                    LOG.error("Got request {} with rid {}, ignore",
                        call.getMethodDescriptor().getFullMethodName(), GrpcHeaders.getRequestId());
                }

                var rid = "rid-" + UUID.randomUUID();
                LOG.debug("Server request id: {}", rid);

                var ridContext = GrpcHeaders.createContext(Map.of(GrpcHeaders.X_REQUEST_ID, rid));
                var logContext = Map.of(LogContextKey.REQUEST_ID, rid);

                var listener = next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
                    @Override
                    public void sendHeaders(Metadata headers) {
                        headers.put(GrpcHeaders.X_REQUEST_ID, rid);
                        super.sendHeaders(headers);
                    }
                }, headers);

                return new ServerCall.Listener<>() {
                    @Override
                    public void onMessage(final ReqT message) {
                        withRid(ridContext, logContext, () -> listener.onMessage(message));
                    }

                    @Override
                    public void onHalfClose() {
                        withRid(ridContext, logContext, listener::onHalfClose);
                    }

                    @Override
                    public void onCancel() {
                        withRid(ridContext, logContext, listener::onCancel);
                    }

                    @Override
                    public void onComplete() {
                        withRid(ridContext, logContext, listener::onComplete);
                    }

                    @Override
                    public void onReady() {
                        withRid(ridContext, logContext, listener::onReady);
                    }
                };
            }
        };
    }

    public static FromResponse fromResponse() {
        return new FromResponse();
    }

    private static void withRid(Context grpcContext, Map<String, String> logContext, Runnable r) {
        grpcContext.run(() -> {
            try (var ignore = CloseableThreadContext.putAll(logContext)) {
                r.run();
            }
        });
    }

    public static final class FromResponse implements ClientInterceptor {
        @Nullable
        private volatile String rid = null;

        private FromResponse() {}

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                   CallOptions callOptions, Channel next)
        {
            return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> listener, Metadata headers) {
                    super.start(
                        new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                            @Override
                            public void onHeaders(Metadata headers) {
                                super.onHeaders(headers);
                                rid = headers.get(GrpcHeaders.X_REQUEST_ID);
                            }
                        },
                        headers);
                }
            };
        }

        @Nullable
        public String rid() {
            return rid;
        }
    }
}
