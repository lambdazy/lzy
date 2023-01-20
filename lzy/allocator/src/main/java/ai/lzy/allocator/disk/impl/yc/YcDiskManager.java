package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.*;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.exceptions.InvalidConfigurationException;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.OperationsExecutor;
import ai.lzy.longrunning.dao.OperationDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.api.compute.v1.DiskOuterClass;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.DiskServiceGrpc.DiskServiceBlockingStub;
import yandex.cloud.api.compute.v1.SnapshotServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc.OperationServiceBlockingStub;
import yandex.cloud.sdk.ServiceFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static yandex.cloud.api.compute.v1.DiskServiceOuterClass.GetDiskRequest;
import static yandex.cloud.api.compute.v1.SnapshotServiceGrpc.SnapshotServiceBlockingStub;

@Requires(property = "allocator.yc-credentials.enabled", value = "true")
@Singleton
public class YcDiskManager implements DiskManager {
    private static final Logger LOG = LogManager.getLogger(YcDiskManager.class);
    private static final Pattern DISK_NAME_PATTERN = Pattern.compile("[a-z]([-a-z0-9]{0,61}[a-z0-9])?");

    static final int GB_SHIFT = 30;
    static final String USER_ID_LABEL = "user-id";
    static final String LZY_OP_LABEL = "lzy-op-id";

    private final String instanceId;
    private final String folderId;
    private final AllocatorDataSource storage;
    private final DiskDao diskDao;
    private final DiskOpDao diskOpDao;
    private final OperationDao operationsDao;
    private final ObjectMapper objectMapper;
    private final DiskMetrics metrics;
    private final OperationsExecutor executor;
    private final DiskServiceBlockingStub ycDiskService;
    private final SnapshotServiceBlockingStub ycSnapshotService;
    private final OperationServiceBlockingStub ycOperationService;

    @Inject
    public YcDiskManager(ServiceConfig config, ServiceConfig.DiskManagerConfig diskConfig, AllocatorDataSource storage,
                         @Named("AllocatorObjectMapper") ObjectMapper objectMapper, DiskMetrics metrics,
                         DiskDao diskDao, DiskOpDao diskOpDao, ServiceFactory serviceFactory,
                         @Named("AllocatorOperationDao") OperationDao operationsDao,
                         @Named("AllocatorOperationsExecutor") OperationsExecutor executor)
    {
        this.instanceId = config.getInstanceId();
        this.folderId = diskConfig.getFolderId();
        this.storage = storage;
        this.diskDao = diskDao;
        this.diskOpDao = diskOpDao;
        this.operationsDao = operationsDao;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.executor = executor;

        ycDiskService = serviceFactory.create(DiskServiceBlockingStub.class,
            DiskServiceGrpc::newBlockingStub);
        ycSnapshotService = serviceFactory.create(SnapshotServiceBlockingStub.class,
            SnapshotServiceGrpc::newBlockingStub);
        ycOperationService = serviceFactory.create(OperationServiceBlockingStub.class,
            OperationServiceGrpc::newBlockingStub);
    }

    @Nullable
    @Override
    public Disk get(String id) throws StatusException {
        LOG.info("Searching disk with id={}", id);
        try {
            final DiskOuterClass.Disk disk = ycDiskService.get(
                GetDiskRequest.newBuilder()
                    .setDiskId(id)
                    .build());
            return new Disk(
                id,
                new DiskSpec(
                    disk.getName(),
                    DiskType.fromYcName(disk.getTypeId()),
                    (int) (disk.getSize() >> GB_SHIFT),
                    disk.getZoneId()
                ),
                new DiskMeta(disk.getLabelsOrThrow(USER_ID_LABEL))
            );
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                LOG.warn("Disk with id={} was not found", id);
                return null;
            }
            throw e.getStatus().asException();
        }
    }

    @Override
    public DiskOperation newCreateDiskOperation(OuterOperation outerOp, DiskSpec spec, DiskMeta meta)
        throws InvalidConfigurationException
    {
        // Name of the disk. Value must match the regular expression [a-z]([-a-z0-9]{0,61}[a-z0-9])?.
        if (!DISK_NAME_PATTERN.matcher(spec.name()).matches()) {
            throw new InvalidConfigurationException("Disk name %s doesn't match pattern %s"
                .formatted(spec.name(), DISK_NAME_PATTERN.pattern()));
        }

        final var state = new YcCreateDiskState("", folderId, null, spec, meta);
        return new DiskOperation(
            outerOp.opId(),
            outerOp.descr(),
            outerOp.startedAt(),
            outerOp.deadline(),
            instanceId,
            DiskOperation.Type.CREATE,
            toJson(state),
            new YcCreateDiskAction(outerOp, state, this));
    }

    @Override
    public DiskOperation newCloneDiskOperation(OuterOperation outerOp, Disk originDisk, DiskSpec newDiskSpec,
                                               DiskMeta newDiskMeta)
    {
        if (newDiskSpec.sizeGb() < originDisk.spec().sizeGb()) {
            throw new IllegalArgumentException("Cannot decrease size during clone");
        }

        final var state = new YcCloneDiskState("", "", "", folderId, null, originDisk, newDiskSpec, newDiskMeta, null);
        return new DiskOperation(
            outerOp.opId(),
            outerOp.descr(),
            outerOp.startedAt(),
            outerOp.deadline(),
            instanceId,
            DiskOperation.Type.CLONE,
            toJson(state),
            new YcCloneDiskAction(outerOp, state, this));
    }

    @Override
    public DiskOperation newDeleteDiskOperation(OuterOperation outerOp, String diskId) {
        final var state = new YcDeleteDiskState("", folderId, diskId);
        return new DiskOperation(
            outerOp.opId(),
            outerOp.descr(),
            outerOp.startedAt(),
            outerOp.deadline(),
            instanceId,
            DiskOperation.Type.DELETE,
            toJson(state),
            new YcDeleteDiskAction(outerOp, state, this));
    }

    @Override
    public DiskOperation restoreDiskOperation(DiskOperation template) {
        var outerOp = new DiskManager.OuterOperation(
            template.opId(), template.descr(), template.startedAt(), template.deadline());
        return switch (template.diskOpType()) {
            case CREATE -> {
                var state = fromJson(template.state(), YcCreateDiskState.class);
                var action = new YcCreateDiskAction(outerOp, state, this);
                yield template.withDeferredAction(action);
            }
            case CLONE -> {
                var state = fromJson(template.state(), YcCloneDiskState.class);
                var action = new YcCloneDiskAction(outerOp, state, this);
                yield template.withDeferredAction(action);
            }
            case DELETE -> {
                var state = fromJson(template.state(), YcDeleteDiskState.class);
                var action = new YcDeleteDiskAction(outerOp, state, this);
                yield template.withDeferredAction(action);
            }
        };
    }

    AllocatorDataSource storage() {
        return storage;
    }

    DiskDao diskDao() {
        return diskDao;
    }

    DiskOpDao diskOpDao() {
        return diskOpDao;
    }

    OperationDao operationsDao() {
        return operationsDao;
    }

    ObjectMapper objectMapper() {
        return objectMapper;
    }

    DiskMetrics metrics() {
        return metrics;
    }

    ScheduledExecutorService executor() {
        return executor;
    }

    DiskServiceBlockingStub ycDiskService() {
        return ycDiskService;
    }

    SnapshotServiceBlockingStub ycSnapshotService() {
        return ycSnapshotService;
    }

    OperationServiceBlockingStub ycOperationService() {
        return ycOperationService;
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T fromJson(String obj, Class<T> type) {
        try {
            return objectMapper.readValue(obj, type);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
