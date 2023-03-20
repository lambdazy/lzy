package ai.lzy.util.grpc;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

public class GrpcExceptionHandlingInterceptor implements ServerInterceptor {

    private static final Logger LOG = LogManager.getLogger("GrpcExceptionHandling");

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
                                                                 Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next)
    {
        ServerCall.Listener<ReqT> listener = next.startCall(call, headers);
        return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(listener) {
            @Override
            public void onHalfClose() {
                try {
                    super.onHalfClose();
                } catch (Exception e) {
                    LOG.error("Got unhandled exception", e);
                    StatusRuntimeException exception = GrpcUtils.mapToGrpcException(e);
                    call.close(exception.getStatus(), Objects.requireNonNullElseGet(exception.getTrailers(),
                        Metadata::new));
                }
            }
        };
    }

    public static GrpcExceptionHandlingInterceptor server() {
        return new GrpcExceptionHandlingInterceptor();
    }
}
