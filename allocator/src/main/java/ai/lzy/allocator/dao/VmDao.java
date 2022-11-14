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
    /**
     * @return vmId
     */
    String create(Vm.Spec vmSpec, String opId, @Nullable TransactionHandle transaction) throws SQLException;

    void update(String vmId, Vm.State state, @Nullable TransactionHandle transaction) throws SQLException;
    void updateStatus(String vmId, Vm.VmStatus status, @Nullable TransactionHandle transaction) throws SQLException;
    void updateLastActivityTime(String vmId, Instant time) throws SQLException;

    List<Vm> list(String sessionId) throws SQLException;

    void delete(String sessionId) throws SQLException;

    @VisibleForTesting
    List<Vm> listAlive() throws SQLException;

    List<Vm> listExpired(int limit) throws SQLException;

    @Nullable
    Vm get(String vmId, TransactionHandle transaction) throws SQLException;

    /**
     * Find vm with given spec and status IDLING and set its status to RUNNING
     */
    @Nullable
    Vm acquire(Vm.Spec vmSpec, @Nullable TransactionHandle transaction) throws SQLException;

    void release(String vmId, Instant deadline, @Nullable TransactionHandle transaction) throws SQLException;

    void saveAllocatorMeta(String vmId, Map<String, String> meta, @Nullable TransactionHandle transaction)
        throws SQLException;

    @Nullable
    Map<String, String> getAllocatorMeta(String vmId, @Nullable TransactionHandle transaction) throws SQLException;

    void setVolumeClaims(String vmId, List<VolumeClaim> volumeClaims, @Nullable TransactionHandle transaction)
        throws SQLException;

    List<VolumeClaim> getVolumeClaims(String vmId, @Nullable TransactionHandle transaction) throws SQLException;

    void setVmSubjectId(String vmId, String vmSubjectId, @Nullable TransactionHandle transaction) throws SQLException;
}
