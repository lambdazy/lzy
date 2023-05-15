package ai.lzy.service.util;

import ai.lzy.service.LzyServiceMetrics;
import ai.lzy.service.config.ClientVersions;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import io.grpc.*;
import jakarta.inject.Singleton;

import java.util.Set;

@Singleton
public class ClientVersionInterceptor implements ServerInterceptor {
    private static final Set<String> forbiddenMethods = Set.of(
        LzyWorkflowServiceGrpc.getStartWorkflowMethod().getFullMethodName()
    );

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

        if (version == null) {
            return next.startCall(call, headers);
        }

        final boolean supported;
        try {
            supported = ClientVersions.isSupported(version);
        } catch (StatusException e) {
            if (forbidIfUnsupported) {
                call.close(e.getStatus(), new Metadata());
                return new ServerCall.Listener<>() {
                };
            }

            return next.startCall(call, headers);
        }

        if (!supported) {
            metricReporter.unsupportedClientVersionCalls.inc();

            if (forbidIfUnsupported) {
                var status = Status.UNIMPLEMENTED.withDescription(
                    "Your client version is unsupported in this installation, please update it to more recent version")
                    .asException();

                call.close(status.getStatus(), new Metadata());
                return new ServerCall.Listener<>() {
                };
            }
        }

        return next.startCall(call, headers);
    }
}
