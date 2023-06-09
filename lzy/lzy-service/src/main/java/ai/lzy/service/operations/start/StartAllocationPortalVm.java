package ai.lzy.service.operations.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.service.config.PortalServiceSpec;
import ai.lzy.service.dao.StartExecutionState;
import ai.lzy.service.operations.ExecutionStepContext;
import ai.lzy.service.operations.RetryableFailStep;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

final class StartAllocationPortalVm extends StartExecutionContextAwareStep
    implements Supplier<StepResult>, RetryableFailStep
{
    private final PortalServiceSpec spec;
    private final AllocatorBlockingStub allocClient;
    private final LongRunningServiceBlockingStub allocOpClient;

    public StartAllocationPortalVm(ExecutionStepContext stepCtx, StartExecutionState state, PortalServiceSpec spec,
                                   AllocatorBlockingStub allocClient, LongRunningServiceBlockingStub allocOpClient)
    {
        super(stepCtx, state);
        this.spec = spec;
        this.allocClient = allocClient;
        this.allocOpClient = allocOpClient;
    }

    @Override
    public StepResult get() {
        if (portalApiAddress() != null) {
            log().debug("{} Portal VM already allocated, skip step...", logPrefix());
            return StepResult.ALREADY_DONE;
        }

        // todo: remove execID wfName it's already noticed in logPrefix
        log().info("{} Allocate portal VM: { wfName: {}, execId: {} }", logPrefix(), wfName(), execId());

        var cfg = prepareConfig();
        var args = formatToArgs(cfg);
        var ports = Map.of(
            (int) cfg.get("portal.slots-api-port"), (int) cfg.get("portal.slots-api-port"),
            (int) cfg.get("portal.portal-api-port"), (int) cfg.get("portal.portal-api-port")
        );
        var portalEnvPKEY = "LZY_PORTAL_PKEY";

        var allocateVmClient = (idempotencyKey() == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey() + "_alloc_portal_vm");
        final LongRunning.Operation op;

        var allocateRequest = VmAllocatorApi.AllocateRequest.newBuilder()
            .setSessionId(allocatorSessionId())
            .setPoolLabel(spec.poolLabel())
            .setZone(spec.poolZone())
            .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.SYSTEM)
            .addWorkload(VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                .setName("portal")
                .setImage(spec.dockerImage())
                .addAllArgs(args)
                .putEnv(portalEnvPKEY, spec.rsaKeys().privateKey())
                .putAllPortBindings(ports)
                .build())
            .build();

        log().debug("{} Allocate VM request: {}", logPrefix(), safePrinter().shortDebugString(allocateRequest));

        try {
            op = allocateVmClient.allocate(allocateRequest);
        } catch (StatusRuntimeException sre) {
            return retryableFail(sre, "Error in AllocatorGrpcClient::allocate call", () -> {}, sre);
        }

        setAllocateVmOpId(op.getId());

        VmAllocatorApi.AllocateMetadata allocateMetadata;
        try {
            allocateMetadata = op.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        } catch (InvalidProtocolBufferException e) {
            log().error("{} Cannot parse AllocateMetadata from operation with id='{}': {}", logPrefix(), op.getId(),
                e.getMessage(), e);
            return StepResult.RESTART;
        }

        setPortalVmId(allocateMetadata.getVmId());

        try {
            withRetries(log(), () -> execDao().updateAllocateOperationData(execId(), op.getId(), portalVmId(), null));
        } catch (Exception e) {
            Runnable dropAllocVm = () -> {
                try {
                    //noinspection ResultOfMethodCallIgnored
                    allocOpClient.cancel(LongRunning.CancelOperationRequest.newBuilder()
                        .setOperationId(op.getId()).build());
                } catch (StatusRuntimeException sre) {
                    log().warn("{} Cannot cancel allocate portal VM operation with id='{}' after error {}: ",
                        logPrefix(), op.getId(), e.getMessage(), sre);
                }

                var freeVmAllocClient = (idempotencyKey() == null) ? allocClient :
                    withIdempotencyKey(allocClient, idempotencyKey() + "_free_portal_vm");
                try {
                    //noinspection ResultOfMethodCallIgnored
                    freeVmAllocClient.free(VmAllocatorApi.FreeRequest.newBuilder().setVmId(portalVmId()).build());
                } catch (StatusRuntimeException sre) {
                    log().warn("{} Cannot free portal VM with id='{}' after error {}: ", logPrefix(), portalVmId(),
                        e.getMessage(), sre);
                }
            };

            return retryableFail(e, "Cannot save data about allocate portal VM operation with id='%s'"
                .formatted(op.getId()), dropAllocVm, Status.INTERNAL.withDescription("Cannot allocate portal VM")
                .asRuntimeException());
        }

        return StepResult.CONTINUE;
    }

    private Map<String, Object> prepareConfig() {
        return new HashMap<>(Map.of(
            "portal.portal-id", portalId(),
            "portal.portal-api-port", spec.portalPort(),
            "portal.slots-api-port", spec.slotsApiPort(),
            "portal.channel-manager-address", spec.channelManagerAddress(),
            "portal.iam-address", spec.iamAddress(),
            "portal.whiteboard-address", spec.whiteboardAddress(),
            "portal.concurrency.workers-pool-size", spec.workersPoolSize(),
            "portal.concurrency.downloads-pool-size", spec.downloadPoolSize(),
            "portal.concurrency.chunks-pool-size", spec.chunksPoolSize())
        );
    }

    private static List<String> formatToArgs(Map<String, Object> cfg) {
        return cfg.entrySet().stream().map(entry -> "-" + entry.getKey() + "=" + entry.getValue()).toList();
    }
}
