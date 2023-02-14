package ai.lzy.util.grpc;

import io.grpc.*;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import lombok.Lombok;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import javax.annotation.Nullable;

public class RequestIdInterceptor {
    private static final Logger LOG = LogManager.getLogger(RequestIdInterceptor.class);

    public static ServerInterceptor server() {
        return server(false);
    }

    public static ServerInterceptor server(boolean generateRequestId) {
        return new ServerInterceptor() {
            @Override
            public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                         ServerCallHandler<ReqT, RespT> next)
            {
                var rid = GrpcHeaders.getRequestId();
                if (rid == null && generateRequestId) {
                    rid = "gen-" + UUID.randomUUID();
                }
                LOG.debug("Server request id: {}", rid);

                if (rid == null) {
                    return next.startCall(call, headers);
                }

                var ridContext = GrpcHeaders.createContext(Map.of(GrpcHeaders.X_REQUEST_ID, rid));
                var logContext = Map.of("rid", rid);

                var original = withRid(ridContext, logContext, () -> next.startCall(call, headers));

                return new SimpleForwardingServerCallListener<>(original) {
                    @Override
                    public void onMessage(final ReqT message) {
                        withRid(ridContext, logContext, () -> super.onMessage(message));
                    }

                    @Override
                    public void onHalfClose() {
                        withRid(ridContext, logContext, super::onHalfClose);
                    }

                    @Override
                    public void onCancel() {
                        withRid(ridContext, logContext, super::onCancel);
                    }

                    @Override
                    public void onComplete() {
                        withRid(ridContext, logContext, super::onComplete);
                    }

                    @Override
                    public void onReady() {
                        withRid(ridContext, logContext, super::onReady);
                    }
                };
            }
        };
    }

    public static ClientInterceptor client() {
        return client(false);
    }

    public static ClientInterceptor client(boolean generateRequestId) {
        return new ClientInterceptor() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
                                                                       CallOptions callOptions, Channel next)
            {
                var rId = GrpcHeaders.getRequestId();
                if (rId == null && generateRequestId) {
                    rId = "gen-" + UUID.randomUUID();
                }
                LOG.debug("Client request id: {}", rId);

                if (rId == null) {
                    return next.newCall(method, callOptions);
                }

                var rid = rId;
                var ridContext = GrpcHeaders.createContext(Map.of(GrpcHeaders.X_REQUEST_ID, rid));
                var logContext = Map.of("rid", rid);

                var original = withRid(ridContext, logContext, () -> next.newCall(method, callOptions));

                return new ForwardingClientCall.SimpleForwardingClientCall<>(original) {
                    @Override
                    public void start(Listener<RespT> responseListener, Metadata headers) {
                        headers.put(GrpcHeaders.X_REQUEST_ID, rid);
                        withRid(ridContext, logContext, () -> super.start(
                            new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
                                @Override
                                public void onHeaders(Metadata headers) {
                                    withRid(ridContext, logContext, () -> super.onHeaders(headers));
                                }

                                @Override
                                public void onMessage(RespT message) {
                                    withRid(ridContext, logContext, () -> super.onMessage(message));
                                }

                                @Override
                                public void onClose(Status status, Metadata trailers) {
                                    withRid(ridContext, logContext, () -> super.onClose(status, trailers));
                                }

                                @Override
                                public void onReady() {
                                    withRid(ridContext, logContext, super::onReady);
                                }
                            },
                            headers));
                    }

                    @Override
                    public void sendMessage(ReqT message) {
                        withRid(ridContext, logContext, () -> super.sendMessage(message));
                    }

                    @Override
                    public void request(int numMessages) {
                        withRid(ridContext, logContext, () -> super.request(numMessages));
                    }

                    @Override
                    public void cancel(@Nullable String message, @Nullable Throwable cause) {
                        withRid(ridContext, logContext, () -> super.cancel(message, cause));
                    }

                    @Override
                    public void halfClose() {
                        withRid(ridContext, logContext, super::halfClose);
                    }

                    @Override
                    public void setMessageCompression(boolean enabled) {
                        withRid(ridContext, logContext, () -> super.setMessageCompression(enabled));
                    }

                    @Override
                    public boolean isReady() {
                        return withRid(ridContext, logContext, super::isReady);
                    }

                    @Override
                    public Attributes getAttributes() {
                        return withRid(ridContext, logContext, super::getAttributes);
                    }
                };
            }
        };
    }


    private static void withRid(Context ridContext, Map<String, String> logContext, Runnable r) {
        withRid(ridContext, logContext, () -> {
            r.run();
            return null;
        });
    }

    private static <T> T withRid(Context ridContext, Map<String, String> logContext, Callable<T> c) {
        var previous = ridContext.attach();
        try (var ignore = CloseableThreadContext.putAll(logContext)) {
            try {
                return c.call();
            } catch (Exception e) {
                throw Lombok.sneakyThrow(e);
            }
        } finally {
            ridContext.detach(previous);
        }
    }

}
