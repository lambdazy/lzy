package ai.lzy.allocator.dao;

import ai.lzy.allocator.db.TransactionManager;
import ai.lzy.allocator.db.TransactionManager.TransactionHandle;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public interface VmDao {
    Vm create(String sessionId, String poolId, List<Workload> workload, @Nullable TransactionHandle transaction);
    void update(Vm vm, @Nullable TransactionHandle transaction);

    List<Vm> list(String sessionId, @Nullable TransactionHandle transaction);
    @Nullable
    Vm get(String vmId, TransactionHandle transaction);
    List<Vm> getExpired(int limit, @Nullable TransactionHandle transaction);

    /**
     * Find vm with this session and pool id with status IDLING and set its status to RUNNING
     */
    @Nullable
    Vm acquire(String sessionId, String poolId, @Nullable TransactionHandle transaction);
}
