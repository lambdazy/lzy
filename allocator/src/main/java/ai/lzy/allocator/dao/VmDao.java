package ai.lzy.allocator.dao;

import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.Workload;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public interface VmDao {
    Vm create(String sessionId, String poolId, List<Workload> workload);
    void update(Vm vm);

    List<Vm> list(String sessionId);
    @Nullable
    Vm get(String vmId);
    List<Vm> getExpired(int limit);

    /**
     * Find vm with this session and pool id with status IDLING and set its status to RUNNING
     */
    @Nullable
    Vm acquire(String sessionId, String poolId);
}
