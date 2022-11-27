package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.dao.OperationDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.SnapshotServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

abstract class YcDiskActionBase<S> implements Runnable {

    protected final String opId;
    protected S state;
    protected final AllocatorDataSource storage;
    protected final DiskDao diskDao;
    protected final DiskOpDao diskOpDao;
    protected final OperationDao operationsDao;
    protected final ScheduledExecutorService executor;
    protected final ObjectMapper objectMapper;
    protected final DiskServiceGrpc.DiskServiceBlockingStub ycDiskService;
    protected final SnapshotServiceGrpc.SnapshotServiceBlockingStub ycSnapshotService;
    protected final OperationServiceGrpc.OperationServiceBlockingStub ycOperationService;

    protected YcDiskActionBase(String opId, S state, AllocatorDataSource storage, DiskDao diskDao, DiskOpDao diskOpDao,
                               OperationDao operationsDao, ScheduledExecutorService executor, ObjectMapper objectMapper,
                               DiskServiceGrpc.DiskServiceBlockingStub ycDiskService,
                               SnapshotServiceGrpc.SnapshotServiceBlockingStub ycSnapshotService,
                               OperationServiceGrpc.OperationServiceBlockingStub ycOperationService)
    {
        this.opId = opId;
        this.state = state;
        this.storage = storage;
        this.diskDao = diskDao;
        this.diskOpDao = diskOpDao;
        this.operationsDao = operationsDao;
        this.executor = executor;
        this.objectMapper = objectMapper;
        this.ycDiskService = ycDiskService;
        this.ycSnapshotService = ycSnapshotService;
        this.ycOperationService = ycOperationService;
    }

    protected final void restart() {
        executor.schedule(this, 1, TimeUnit.SECONDS);
    }

    protected final String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
