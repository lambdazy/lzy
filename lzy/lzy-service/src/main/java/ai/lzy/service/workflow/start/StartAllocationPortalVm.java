package ai.lzy.service.workflow.start;

import ai.lzy.longrunning.OperationRunnerBase.StepResult;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.service.data.dao.ExecutionDao;
import ai.lzy.v1.AllocatorGrpc.AllocatorBlockingStub;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static ai.lzy.model.db.DbHelper.withRetries;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class StartAllocationPortalVm implements Supplier<StepResult> {
    private final ExecutionDao execDao;
    private final String wfName;
    private final String execId;
    private final String idempotencyKey;
    private final AllocatorBlockingStub allocClient;
    private final LongRunningServiceBlockingStub allocOpClient;
    private final AllocPortalVmSpec spec;
    private final Consumer<String> allocOpIdConsumer;
    private final Function<StatusRuntimeException, StepResult> failAction;
    private final Logger log;
    private final String logPrefix;

    public StartAllocationPortalVm(ExecutionDao execDao, String wfName, String execId,
                                   @Nullable String idempotencyKey, AllocatorBlockingStub allocClient,
                                   LongRunningServiceBlockingStub allocOpClient,
                                   AllocPortalVmSpec spec, Consumer<String> allocOpIdConsumer,
                                   Function<StatusRuntimeException, StepResult> failAction,
                                   Logger log, String logPrefix)
    {
        this.execDao = execDao;
        this.wfName = wfName;
        this.execId = execId;
        this.idempotencyKey = idempotencyKey;
        this.allocClient = allocClient;
        this.allocOpClient = allocOpClient;
        this.spec = spec;
        this.allocOpIdConsumer = allocOpIdConsumer;
        this.failAction = failAction;
        this.log = log;
        this.logPrefix = logPrefix;
    }

    @Override
    public StepResult get() {
        log.info("{} Allocate portal VM: { wfName: {}, execId: {} }", logPrefix, wfName, execId);
        log.debug("{} Portal spec: {}", logPrefix, spec);

        var cfg = prepareConfig();
        var args = formatToArgs(cfg);
        var ports = Map.of(
            (int) cfg.get("portal.slots-api-port"), (int) cfg.get("portal.slots-api-port"),
            (int) cfg.get("portal.portal-api-port"), (int) cfg.get("portal.portal-api-port")
        );
        var portalEnvPKEY = "LZY_PORTAL_PKEY";

        var allocateVmClient = (idempotencyKey == null) ? allocClient :
            withIdempotencyKey(allocClient, idempotencyKey + "_alloc_portal_vm");
        final LongRunning.Operation allocateVmOp;

        try {
            allocateVmOp = allocateVmClient.allocate(
                VmAllocatorApi.AllocateRequest.newBuilder()
                    .setSessionId(spec.getSessionId())
                    .setPoolLabel(spec.getPoolLabel())
                    .setZone(spec.getPoolZone())
                    .setClusterType(VmAllocatorApi.AllocateRequest.ClusterType.SYSTEM)
                    .addWorkload(VmAllocatorApi.AllocateRequest.Workload.newBuilder()
                        .setName("portal")
                        .setImage(spec.getDockerImage())
                        .addAllArgs(args)
                        .putEnv(portalEnvPKEY, spec.getPrivateKey())
                        .putAllPortBindings(ports)
                        .build())
                    .build());
        } catch (StatusRuntimeException sre) {
            log.error("{} Error in Alloc::allocate call: {}", logPrefix, sre.getMessage(), sre);
            return failAction.apply(sre);
        }

        allocOpIdConsumer.accept(allocateVmOp.getId());

        VmAllocatorApi.AllocateMetadata allocateMetadata;
        try {
            allocateMetadata = allocateVmOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class);
        } catch (InvalidProtocolBufferException e) {
            log.error("{} Cannot parse AllocateMetadata from operation with id='{}': {}", logPrefix,
                allocateVmOp.getId(), e.getMessage(), e);
            return StepResult.RESTART;
        }

        var vmId = allocateMetadata.getVmId();
        try {
            withRetries(log, () -> execDao.updateAllocateOperationData(execId, allocateVmOp.getId(), vmId, null));
        } catch (Exception e) {
            log.error("{} Cannot save data about allocate portal VM operation with id='{}': {}", logPrefix,
                allocateVmOp.getId(), e.getMessage(), e);

            try {
                //noinspection ResultOfMethodCallIgnored
                allocOpClient.cancel(LongRunning.CancelOperationRequest.newBuilder()
                    .setOperationId(allocateVmOp.getId()).build());
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot cancel allocate portal VM operation with id='{}' after error {}: ", logPrefix,
                    allocateVmOp.getId(), e.getMessage(), sre);
            }

            var freeVmAllocClient = (idempotencyKey == null) ? allocClient :
                withIdempotencyKey(allocClient, idempotencyKey + "_free_portal_vm");
            try {
                //noinspection ResultOfMethodCallIgnored
                freeVmAllocClient.free(VmAllocatorApi.FreeRequest.newBuilder().setVmId(vmId).build());
            } catch (StatusRuntimeException sre) {
                log.warn("{} Cannot free portal VM with id='{}' after error {}: ", logPrefix, vmId, e.getMessage(),
                    sre);
            }

            return failAction.apply(Status.INTERNAL.withDescription("Cannot allocate portal VM").asRuntimeException());
        }

        return StepResult.CONTINUE;
    }

    private Map<String, Object> prepareConfig() {
        var actualPortalPort = (spec.getPortalPort() == -1) ? FreePortFinder.find(10000, 11000)
            : spec.getPortalPort();
        var actualSlotsApiPort = (spec.getSlotsApiPort() == -1) ? FreePortFinder.find(11000, 12000)
            : spec.getSlotsApiPort();

        var args = new HashMap<String, Object>(Map.of(
            "portal.portal-id", spec.getPortalId(),
            "portal.portal-api-port", actualPortalPort,
            "portal.slots-api-port", actualSlotsApiPort,
            "portal.channel-manager-address" + spec.getChannelManagerAddress(),
            "portal.iam-address" + spec.getIamAddress(),
            "portal.whiteboard-address" + spec.getWhiteboardAddress(),
            "portal.concurrency.workers-pool-size" + spec.getWorkersPoolSize(),
            "portal.concurrency.downloads-pool-size" + spec.getDownloadPoolSize(),
            "portal.concurrency.chunks-pool-size" + spec.getChunksPoolSize()));

        if (spec.getStdoutChannelId() != null) {
            args.put("portal.stdout-channel-id", spec.getStdoutChannelId());
        }

        if (spec.getStderrChannelId() != null) {
            args.put("portal.stderr-channel-id", spec.getStderrChannelId());
        }

        return args;
    }

    private static List<String> formatToArgs(Map<String, Object> cfg) {
        return cfg.entrySet().stream().map(entry -> "-" + entry.getKey() + "=" + entry.getValue()).toList();
    }
}
