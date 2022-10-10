package ai.lzy.util.grpc;

import ai.lzy.v1.validation.LV;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;

import java.util.UUID;
import javax.annotation.Nullable;

public class GrpcLogsInterceptor implements ServerInterceptor {
    private static final Logger LOG = LogManager.getLogger("GrpcLogs");
    private static final ProtoPrinter.Printer SAFE_PRINTER
        = ProtoPrinter.printer().usingSensitiveExtension(LV.sensitive);

    @Override
    public <M, R> ServerCall.Listener<M> interceptCall(ServerCall<M, R> call,
                                                       Metadata headers,
                                                       ServerCallHandler<M, R> next)
    {
        final String callId = UUID.randomUUID().toString();

        var grpcServerCall = new GrpcServerCall<>(call, callId, headers.get(GrpcHeaders.X_REQUEST_ID));
        var listener = next.startCall(grpcServerCall, headers);

        return new GrpcForwardingServerCallListener<>(call.getMethodDescriptor(), listener) {
            @Override
            public void onMessage(M message) {
                if (LOG.isTraceEnabled()) {
                    LOG.info("{}::<{}>, request: ({})", methodName, callId, printMessageSafe(message));
                } else {
                    LOG.info("{}::<{}>, request: <...>", methodName, callId);
                }
                super.onMessage(message);
            }
        };
    }

    private static class GrpcServerCall<M, R> extends ServerCall<M, R> {
        final ServerCall<M, R> serverCall;
        final String callId;

        protected GrpcServerCall(ServerCall<M, R> serverCall, String callId, @Nullable String reqId) {
            this.serverCall = serverCall;
            this.callId = callId;
            ThreadContext.put("reqid", reqId != null ? reqId : callId);
        }

        @Override
        public void request(int numMessages) {
            serverCall.request(numMessages);
        }

        @Override
        public void sendHeaders(Metadata headers) {
            serverCall.sendHeaders(headers);
        }

        @Override
        public void sendMessage(R message) {
            var methodName = serverCall.getMethodDescriptor().getFullMethodName();
            if (LOG.isTraceEnabled()) {
                LOG.info("{}::<{}>: response: ({})", methodName, callId, printMessageSafe(message));
            } else {
                LOG.info("{}::<{}>: response: <...>", methodName, callId);
            }
            serverCall.sendMessage(message);
        }

        @Override
        public void close(Status status, Metadata trailers) {
            serverCall.close(status, trailers);
            ThreadContext.remove("reqid");
        }

        @Override
        public boolean isCancelled() {
            return serverCall.isCancelled();
        }

        @Override
        public MethodDescriptor<M, R> getMethodDescriptor() {
            return serverCall.getMethodDescriptor();
        }
    }

    private static String printMessageSafe(Object message) {
        return message instanceof MessageOrBuilder msg
            ? SAFE_PRINTER.shortDebugString(msg)
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
