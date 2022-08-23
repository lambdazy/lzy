package ai.lzy.allocator.dao;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public interface VmDao {
    Vm create(String sessionId, String poolId, String zone, List<Workload> workload, String allocationOpId,
              @Nullable TransactionHandle transaction);
    void update(Vm vm, @Nullable TransactionHandle transaction);

    List<Vm> list(String sessionId, @Nullable TransactionHandle transaction);
    List<Vm> list(@Nullable TransactionHandle transaction);
    @Nullable
    Vm get(String vmId, TransactionHandle transaction);
    List<Vm> getExpired(int limit, @Nullable TransactionHandle transaction);

    /**
     * Find vm with this session and pool id with status IDLING and set its status to RUNNING
     */
    @Nullable
    Vm acquire(String sessionId, String poolId, String zone, @Nullable TransactionHandle transaction);

    void saveAllocatorMeta(String vmId, Map<String, String> meta, @Nullable TransactionHandle transaction);
    @Nullable
    Map<String, String> getAllocatorMeta(String vmId, @Nullable TransactionHandle transaction);
}
