package ai.lzy.allocator.dao;

import ai.lzy.allocator.model.Vm;
import ai.lzy.allocator.volume.VolumeClaim;
import ai.lzy.model.db.TransactionHandle;
import com.google.common.annotations.VisibleForTesting;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

public interface VmDao {

    @Nullable
    Vm get(String vmId, TransactionHandle transaction) throws SQLException;


    Vm create(Vm.Spec vmSpec, String opId, Instant startedAt, Instant opDeadline, String vmOtt,
              @Nullable TransactionHandle tx) throws SQLException;

    void delete(String sessionId) throws SQLException;


    /**
     * Find an IDLE VM with given spec and set its status to RUNNING
     */
    @Nullable
    Vm acquire(Vm.Spec vmSpec, @Nullable TransactionHandle transaction) throws SQLException;

    void release(String vmId, Instant deadline, @Nullable TransactionHandle transaction) throws SQLException;


    void setAllocatorMeta(String vmId, Map<String, String> meta, @Nullable TransactionHandle transaction)
        throws SQLException;

    void setVolumeClaims(String vmId, List<VolumeClaim> volumeClaims, @Nullable TransactionHandle transaction)
        throws SQLException;

    void setVmSubjectId(String vmId, String vmSubjectId, @Nullable TransactionHandle transaction) throws SQLException;

    void setTunnelPod(String vmId, String tunnelPodName, @Nullable TransactionHandle transaction) throws SQLException;

    void setVmRunning(String vmId, Map<String, String> vmMeta, Instant activityDeadline, TransactionHandle transaction)
        throws SQLException;

    void setStatus(String vmId, Vm.Status status, @Nullable TransactionHandle transaction) throws SQLException;

    void setLastActivityTime(String vmId, Instant time) throws SQLException;

    void setDeadline(String vmId, Instant time) throws SQLException;


    @VisibleForTesting
    List<Vm> list(String sessionId) throws SQLException;

    @VisibleForTesting
    List<Vm> listAlive() throws SQLException;

    List<Vm> listExpired(int limit) throws SQLException;

    @Nullable
    Map<String, String> getAllocatorMeta(String vmId, @Nullable TransactionHandle transaction) throws SQLException;

    List<VolumeClaim> getVolumeClaims(String vmId, @Nullable TransactionHandle transaction) throws SQLException;

    List<Vm> loadNotCompletedVms(@Nullable TransactionHandle transaction) throws SQLException;
}
