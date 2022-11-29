package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.disk.*;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.dao.OperationDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.Status;
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
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;

import static yandex.cloud.api.compute.v1.DiskServiceOuterClass.GetDiskRequest;
import static yandex.cloud.api.compute.v1.SnapshotServiceGrpc.SnapshotServiceBlockingStub;

@Requires(property = "allocator.yc-credentials.enabled", value = "true")
@Singleton
public class YcDiskManager implements DiskManager {
    private static final Logger LOG = LogManager.getLogger(YcDiskManager.class);
    static final int GB_SHIFT = 30;
    static final String USER_ID_LABEL = "user-id";

    private final String folderId;
    private final AllocatorDataSource storage;
    private final DiskDao diskDao;
    private final DiskOpDao diskOpDao;
    private final OperationDao operationsDao;
    private final ObjectMapper objectMapper;
    private final ScheduledExecutorService executor;
    private final DiskServiceBlockingStub ycDiskService;
    private final SnapshotServiceBlockingStub ycSnapshotService;
    private final OperationServiceBlockingStub ycOperationService;

    @Inject
    public YcDiskManager(ServiceConfig.DiskManagerConfig config, AllocatorDataSource storage, DiskDao diskDao,
                         DiskOpDao diskOpDao, OperationDao operationsDao, ObjectMapper objectMapper,
                         ServiceFactory serviceFactory, @Named("AllocatorExecutor") ScheduledExecutorService executor)
    {
        this.folderId = config.getFolderId();
        this.storage = storage;
        this.diskDao = diskDao;
        this.diskOpDao = diskOpDao;
        this.operationsDao = operationsDao;
        this.objectMapper = objectMapper;
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
    public Disk get(String id) {
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
            throw new RuntimeException(e);
        }
    }

    @Override
    public DiskOperation newCreateDiskOperation(OuterOperation outerOp, DiskSpec spec, DiskMeta meta) {
        final var state = new YcCreateDiskState("", folderId, null, spec, meta);
        return new DiskOperation(
            outerOp.opId(),
            outerOp.startedAt(),
            outerOp.deadline(),
            DiskOperation.Type.CREATE,
            toJson(state),
            new YcCreateDiskAction(outerOp.opId(), state, storage, diskDao, diskOpDao, operationsDao, executor,
                objectMapper, ycDiskService, ycOperationService));
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
            outerOp.startedAt(),
            outerOp.deadline(),
            DiskOperation.Type.CLONE,
            toJson(state),
            new YcCloneDiskAction(outerOp.opId(), state, storage, diskDao, diskOpDao, operationsDao, executor,
                objectMapper, ycDiskService, ycSnapshotService, ycOperationService));
    }

    @Override
    public DiskOperation newDeleteDiskOperation(OuterOperation outerOp, String diskId) {
        final var state = new YcDeleteDiskState("", folderId, diskId);
        return new DiskOperation(
            outerOp.opId(),
            outerOp.startedAt(),
            outerOp.deadline(),
            DiskOperation.Type.DELETE,
            toJson(state),
            new YcDeleteDiskAction(outerOp.opId(), state, storage, diskDao, diskOpDao, operationsDao, executor,
                objectMapper, ycDiskService, ycOperationService));
    }

    @Override
    public DiskOperation restoreDiskOperation(DiskOperation template) {
        return switch (template.diskOpType()) {
            case CREATE -> {
                var state = fromJson(template.state(), YcCreateDiskState.class);
                var action = new YcCreateDiskAction(template.opId(), state, storage, diskDao, diskOpDao, operationsDao,
                    executor, objectMapper, ycDiskService, ycOperationService);
                yield template.withDeferredAction(action);
            }
            case CLONE -> {
                var state = fromJson(template.state(), YcCloneDiskState.class);
                var action = new YcCloneDiskAction(template.opId(), state, storage, diskDao, diskOpDao, operationsDao,
                    executor, objectMapper, ycDiskService, ycSnapshotService, ycOperationService);
                yield template.withDeferredAction(action);
            }
            case DELETE -> {
                var state = fromJson(template.state(), YcDeleteDiskState.class);
                var action = new YcDeleteDiskAction(template.opId(), state, storage, diskDao, diskOpDao, operationsDao,
                    executor, objectMapper, ycDiskService, ycOperationService);
                yield template.withDeferredAction(action);
            }
        };
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
