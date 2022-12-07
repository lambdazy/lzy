package ai.lzy.allocator.disk.impl.yc;

import ai.lzy.allocator.disk.DiskManager;
import ai.lzy.allocator.disk.DiskMetrics;
import ai.lzy.allocator.disk.dao.DiskDao;
import ai.lzy.allocator.disk.dao.DiskOpDao;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.dao.OperationDao;
import com.fasterxml.jackson.core.JsonProcessingException;
import yandex.cloud.api.compute.v1.DiskServiceGrpc;
import yandex.cloud.api.compute.v1.SnapshotServiceGrpc;
import yandex.cloud.api.operation.OperationServiceGrpc;

import java.util.concurrent.TimeUnit;

abstract class YcDiskActionBase<S> implements Runnable {

    protected final DiskManager.OuterOperation op;
    protected S state;
    protected final YcDiskManager diskManager;

    protected YcDiskActionBase(DiskManager.OuterOperation op, S state, YcDiskManager diskManager) {
        this.op = op;
        this.state = state;
        this.diskManager = diskManager;
    }

    protected final String opId() {
        return op.opId();
    }

    protected final void restart() {
        diskManager.executor().schedule(this, 1, TimeUnit.SECONDS);
    }

    protected final String toJson(Object obj) {
        try {
            return diskManager.objectMapper().writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    protected final AllocatorDataSource storage() {
        return diskManager.storage();
    }

    protected final DiskDao diskDao() {
        return diskManager.diskDao();
    }

    protected final DiskOpDao diskOpDao() {
        return diskManager.diskOpDao();
    }

    protected final OperationDao operationsDao() {
        return diskManager.operationsDao();
    }

    protected final DiskMetrics metrics() {
        return diskManager.metrics();
    }

    protected final DiskServiceGrpc.DiskServiceBlockingStub ycDiskService() {
        return diskManager.ycDiskService();
    }

    protected final SnapshotServiceGrpc.SnapshotServiceBlockingStub ycSnapshotService() {
        return diskManager.ycSnapshotService();
    }

    protected final OperationServiceGrpc.OperationServiceBlockingStub ycOperationService() {
        return diskManager.ycOperationService();
    }
}
