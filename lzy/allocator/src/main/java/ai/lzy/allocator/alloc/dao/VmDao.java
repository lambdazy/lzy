package ai.lzy.allocator.alloc.dao;

import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.model.VolumeClaim;
import ai.lzy.model.db.TransactionHandle;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

public interface VmDao {

    @Nullable
    Vm get(String vmId, @Nullable TransactionHandle tx) throws SQLException;

    List<Vm> getSessionVms(String sessionId, @Nullable TransactionHandle tx) throws SQLException;

    Vm create(Vm.Spec vmSpec, Vm.AllocateState allocState, @Nullable TransactionHandle tx) throws SQLException;

    void delete(String vmId, Vm.DeletingState deleteState, @Nullable TransactionHandle tx) throws SQLException;

    void cleanupVm(String vmId, @Nullable TransactionHandle tx) throws SQLException;

    /**
     * Find an IDLE VM with given spec and set its status to RUNNING
     */
    @Nullable
    Vm acquire(Vm.Spec vmSpec, @Nullable TransactionHandle tx) throws SQLException;

    void release(String vmId, Instant deadline, @Nullable TransactionHandle tx) throws SQLException;

    record CachedVms(
        int atPoolAndSession,
        int atSession,
        int atOwner
    ) {}

    CachedVms countCachedVms(Vm.Spec vmSpec, String owner, @Nullable TransactionHandle tx) throws SQLException;


    void setAllocatorMeta(String vmId, Map<String, String> meta, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    Map<String, String> getAllocatorMeta(String vmId, @Nullable TransactionHandle tx) throws SQLException;

    void setTunnelPod(String vmId, String tunnelPodName, @Nullable TransactionHandle tx) throws SQLException;

    void setMountPod(String vmId, String mountPodName, @Nullable TransactionHandle tx) throws SQLException;

    void setVolumeClaims(String vmId, List<VolumeClaim> volumeClaims, @Nullable TransactionHandle tx)
        throws SQLException;

    List<VolumeClaim> getVolumeClaims(String vmId, @Nullable TransactionHandle tx) throws SQLException;

    void setVmRunning(String vmId, Map<String, String> vmMeta, Instant activityDeadline, TransactionHandle tx)
        throws SQLException;

    void updateActivityDeadline(String vmId, Instant deadline) throws SQLException;

    @VisibleForTesting
    List<Vm> listAlive() throws SQLException;

    List<Vm> listExpiredVms(int limit) throws SQLException;

    List<Vm> loadActiveVmsActions(String workerId, @Nullable TransactionHandle tx) throws SQLException;

    List<Vm> loadRunningVms(String workerId, @Nullable TransactionHandle tx) throws SQLException;

    List<Vm> loadByIds(Set<String> vmIds, @Nullable TransactionHandle tx) throws SQLException;

    @VisibleForTesting
    boolean hasDeadVm(String vmId) throws SQLException;

    @Nullable
    Vm findVmByOtt(String vmOtt, @Nullable TransactionHandle tx) throws SQLException;

    @Nullable
    String resetVmOtt(String vmOtt, @Nullable TransactionHandle tx) throws SQLException;
}
