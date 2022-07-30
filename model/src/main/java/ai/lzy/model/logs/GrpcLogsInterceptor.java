package ai.lzy.model.logs;

import ai.lzy.model.JsonUtils;
import com.google.protobuf.MessageOrBuilder;
import io.grpc.ForwardingServerCallListener.SimpleForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class GrpcLogsInterceptor implements ServerInterceptor {
    private static final Logger logger = LogManager.getLogger(GrpcLogsInterceptor.class);

    @Override
    public <M, R> ServerCall.Listener<M> interceptCall(
        ServerCall<M, R> call, Metadata headers, ServerCallHandler<M, R> next) {

        final String callId = UUID.randomUUID().toString();

        GrpcServerCall<M, R> grpcServerCall = new GrpcServerCall<>(call, callId);

        ServerCall.Listener<M> listener = next.startCall(grpcServerCall, headers);

        return new GrpcForwardingServerCallListener<>(call.getMethodDescriptor(), listener) {
            @Override
            public void onMessage(M message) {
                logger.info("{}::<{}>, request: {}", methodName, callId,
                    message instanceof MessageOrBuilder msg ? JsonUtils.printSingleLine(msg) : message.toString());
                super.onMessage(message);
            }
        };
    }

    private static class GrpcServerCall<M, R> extends ServerCall<M, R> {

        final ServerCall<M, R> serverCall;
        final String callId;

        protected GrpcServerCall(ServerCall<M, R> serverCall, String callId) {
            this.serverCall = serverCall;
            this.callId = callId;
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
            logger.info(
                "{}::<{}>: response: {}",
                serverCall.getMethodDescriptor().getFullMethodName(), callId, message
            );
            serverCall.sendMessage(message);
        }

        @Override
        public void close(Status status, Metadata trailers) {
            serverCall.close(status, trailers);
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

    private static class GrpcForwardingServerCallListener<M, R> extends SimpleForwardingServerCallListener<M> {

        final String methodName;

        protected GrpcForwardingServerCallListener(MethodDescriptor<M, R> method, ServerCall.Listener<M> listener) {
            super(listener);
            methodName = method.getFullMethodName();
        }
    }
}
