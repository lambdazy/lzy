package ai.lzy.scheduler.db;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.JobService;
import jakarta.annotation.Nullable;

import java.sql.SQLException;
import java.util.List;

public interface JobDao {
    void insert(JobService.Job job, @Nullable TransactionHandle tx) throws SQLException;

    /**
     * Get job for execution. If job not in WAITING_TO_START status or not found, returns null
     */
    void executing(String id, @Nullable TransactionHandle tx) throws SQLException;

    void complete(String id, JobService.JobStatus currentStatus, @Nullable TransactionHandle tx) throws SQLException;

    List<JobService.Job> listToRestore(@Nullable TransactionHandle tx) throws SQLException;
}
