package ai.lzy.service.util;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.logs.LogContextKey;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

public class ExecutionIdInterceptor implements ServerInterceptor {
    private static final Logger LOG = LogManager.getLogger(ExecutionIdInterceptor.class);

    private final IdGenerator idGenerator = new RandomIdGenerator();

    @Override
    public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers,
                                                                 ServerCallHandler<ReqT, RespT> next)
    {
        if (!LzyWorkflowServiceGrpc.SERVICE_NAME.equals(call.getMethodDescriptor().getServiceName())) {
            LOG.error("Unexpected intercepted method call: {}", call.getMethodDescriptor().getFullMethodName());
            return next.startCall(call, headers);
        }

        var listener = next.startCall(call, headers);
        return new ServerCall.Listener<>() {
            @Nullable
            private String execId = null;

            @Override
            public void onMessage(ReqT req) {
                execId = findExecutionId(req, call.getMethodDescriptor().getFullMethodName());

                exec(execId, () -> listener.onMessage(req));
            }

            @Override
            public void onHalfClose() {
                exec(execId, listener::onHalfClose);
            }

            @Override
            public void onCancel() {
                exec(execId, listener::onCancel);
            }

            @Override
            public void onComplete() {
                exec(execId, listener::onComplete);
            }

            @Override
            public void onReady() {
                exec(execId, listener::onReady);
            }
        };
    }

    @Nullable
    private String findExecutionId(Object request, String method) {
        if (request.getClass() == LWFS.StartWorkflowRequest.class) {
            var req = (LWFS.StartWorkflowRequest) request;

            return idGenerator.generate(req.getWorkflowName() + "_");
        }

        if (request.getClass() == LWFS.FinishWorkflowRequest.class) {
            return ((LWFS.FinishWorkflowRequest) request).getExecutionId();
        }

        if (request.getClass() == LWFS.AbortWorkflowRequest.class) {
            return ((LWFS.AbortWorkflowRequest) request).getExecutionId();
        }

        if (request.getClass() == LWFS.ExecuteGraphRequest.class) {
            return ((LWFS.ExecuteGraphRequest) request).getExecutionId();
        }

        if (request.getClass() == LWFS.GraphStatusRequest.class) {
            return ((LWFS.GraphStatusRequest) request).getExecutionId();
        }

        if (request.getClass() == LWFS.StopGraphRequest.class) {
            return ((LWFS.StopGraphRequest) request).getExecutionId();
        }

        if (request.getClass() == LWFS.ReadStdSlotsRequest.class) {
            return ((LWFS.ReadStdSlotsRequest) request).getExecutionId();
        }

        if (request.getClass() == LWFS.GetAvailablePoolsRequest.class) {
            return ((LWFS.GetAvailablePoolsRequest) request).getExecutionId();
        }

        if (request.getClass() == LWFS.GetOrCreateDefaultStorageRequest.class) {
            return null;
        }

        LOG.error("Unexpected method call: {}", method);
        throw Status.UNIMPLEMENTED.withDescription("Unknown method").asRuntimeException();
    }

    private static void exec(@Nullable String execId, Runnable r) {
        if (execId != null) {
            GrpcHeaders
                .createContext(Map.of(GrpcHeaders.X_EXECUTION_ID, execId))
                .run(() -> {
                    try (var ignored = CloseableThreadContext.put(LogContextKey.EXECUTION_ID, execId)) {
                        r.run();
                    }
                });
        } else {
            r.run();
        }
    }
}
