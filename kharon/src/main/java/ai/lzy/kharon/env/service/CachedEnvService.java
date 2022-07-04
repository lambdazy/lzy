package ai.lzy.kharon.env.service;

import ai.lzy.common.GrpcConverter;
import ai.lzy.kharon.env.CachedEnv;
import ai.lzy.kharon.env.manager.CachedEnvManager;
import ai.lzy.priv.v1.LCES;
import ai.lzy.priv.v1.LzyCachedEnvServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class CachedEnvService extends LzyCachedEnvServiceGrpc.LzyCachedEnvServiceImplBase {

    private static final Logger LOG = LogManager.getLogger(CachedEnvService.class);

    private final CachedEnvManager envManager;

    @Inject
    public CachedEnvService(CachedEnvManager envManager) {
        this.envManager = envManager;
    }

    @Override
    public void saveEnvConfig(
        LCES.SaveEnvConfigRequest request,
        StreamObserver<LCES.SaveEnvConfigResponse> responseObserver
    ) {
        LOG.debug("Received saveEnvConfig request {}", request);
        try {
            if (request.getWorkflowName().isBlank()) {
                String errorMessage = "Illegal argument exception: empty workflow name";
                LOG.error(errorMessage);
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }
            CachedEnv env = envManager.saveEnvConfig(
                request.getWorkflowName(),
                request.getDockerImage(),
                request.getYamlConfig(),
                GrpcConverter.from(request.getDiskType())
            );
            responseObserver.onNext(LCES.SaveEnvConfigResponse.newBuilder()
                .setEnvId(env.envId())
                .setDisk(GrpcConverter.to(env.disk()))
                .build()
            );
            LOG.info("Received saveEnvConfig request done, {}", env.envId());
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal argument exception:", e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asException());
        } catch (Exception e) {
            LOG.error("Internal error:", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }

    @Override
    public void markEnvReady(LCES.MarkEnvReadyRequest request, StreamObserver<LCES.MarkEnvReadyResponse> responseObserver) {
        LOG.debug("Received markEnvReady request {}", request);
        try {
            if (request.getWorkflowName().isBlank() || request.getDiskId().isBlank()) {
                String errorMessage = "Illegal argument exception: empty field";
                LOG.error(
                    errorMessage + "workflowName='{}', diskId='{}'",
                    request.getWorkflowName(), request.getDiskId()
                );
                responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errorMessage).asException());
                return;
            }
            envManager.markEnvReady(request.getWorkflowName(), request.getDiskId());
            responseObserver.onNext(LCES.MarkEnvReadyResponse.getDefaultInstance());
            LOG.info("Received markEnvReady request done");
            responseObserver.onCompleted();
        } catch (IllegalArgumentException e) {
            LOG.error("Illegal argument exception:", e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withCause(e).asException());
        } catch (Exception e) {
            LOG.error("Internal error:", e);
            responseObserver.onError(Status.INTERNAL.withCause(e).asException());
        }
    }
}
