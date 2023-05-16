package ai.lzy.service.util;

import ai.lzy.common.ClientErrorCode;
import ai.lzy.service.LzyServiceMetrics;
import ai.lzy.service.config.ClientVersions;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import io.grpc.*;
import jakarta.inject.Singleton;

import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class ClientVersionInterceptor implements ServerInterceptor {
    private static final Set<String> forbiddenMethods = Set.of(
        LzyWorkflowServiceGrpc.getStartWorkflowMethod().getFullMethodName()
    );

    public static final AtomicBoolean ALLOW_WITHOUT_HEADER = new AtomicBoolean(false);

    private final LzyServiceMetrics metricReporter;

    ClientVersionInterceptor(LzyServiceMetrics metricReporter) {
        this.metricReporter = metricReporter;
    }

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next)
    {
        var forbidIfUnsupported = forbiddenMethods.contains(call.getMethodDescriptor().getFullMethodName());
        var version = headers.get(GrpcHeaders.CLIENT_VERSION);

        var meta = new Metadata();
        meta.put(GrpcHeaders.LZY_ERROR_CODE, String.valueOf(ClientErrorCode.CLIENT_VERSION_NOT_SUPPORTED.code));

        if (version == null) {
            if (ALLOW_WITHOUT_HEADER.get()) {
                return next.startCall(call, headers);
            } else {
                var status = Status.FAILED_PRECONDITION.withDescription(
                    "Please specify X-Client-Version header with version of your client");

                call.close(status, meta);
                return new ServerCall.Listener<>() {
                };
            }
        }

        final boolean supported;
        try {
            supported = ClientVersions.isSupported(version);
        } catch (StatusException e) {
            if (forbidIfUnsupported) {
                call.close(e.getStatus(), meta);
                return new ServerCall.Listener<>() {
                };
            }

            return next.startCall(call, headers);
        }

        if (!supported) {
            metricReporter.unsupportedClientVersionCalls.inc();

            if (forbidIfUnsupported) {
                var status = Status.FAILED_PRECONDITION.withDescription(
                    "Your client version is unsupported in this installation, please update it to more recent version")
                    .asException();

                call.close(status.getStatus(), meta);
                return new ServerCall.Listener<>() {
                };
            }
        }

        return next.startCall(call, headers);
    }
}
