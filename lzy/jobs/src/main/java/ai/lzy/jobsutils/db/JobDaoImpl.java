package ai.lzy.jobsutils.db;

import ai.lzy.jobsutils.JobService;
import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import jakarta.inject.Singleton;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class JobDaoImpl implements JobDao {
    private static final String FIELDS = "id, provider_class, status, serialized_input, start_after";

    private final Storage storage;

    public JobDaoImpl(JobsDataSource storage) {
        this.storage = storage;
    }

    @Override
    public void insert(JobService.Job job, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, (conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(String.format("""
                INSERT INTO job (%s)
                 VALUES (?, ?, ?, ?, ?)""", FIELDS)))
            {
                ps.setString(1, job.id());
                ps.setString(2, job.providerClass());
                ps.setString(3, job.status().name());
                ps.setString(4, job.serializedInput());
                ps.setTimestamp(5, Timestamp.from(job.startAfter()));

                ps.execute();
            }
        });
    }

    @Nullable
    @Override
    public JobService.Job getForExecution(String id, @Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, (conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(String.format("""
                UPDATE job
                SET status = ?
                 WHERE id = ? AND status = 'CREATED'
                RETURNING %s""", FIELDS)))
            {
                ps.setString(1, JobService.JobStatus.EXECUTING.name());
                ps.setString(2, id);
                var rs = ps.executeQuery();

                if (!rs.next()) {
                    return null;
                }

                var providerClass = rs.getString(2);
                var status = JobService.JobStatus.valueOf(rs.getString(3));
                var serializerInput = rs.getString(4);
                var startAfter = rs.getTimestamp(5).toInstant();

                return new JobService.Job(id, providerClass, status, serializerInput, startAfter);

            }
        });
    }

    @Override
    public void complete(String id, JobService.JobStatus status, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, (conn) -> {
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE job
                 SET status = ?
                 WHERE id = ? AND status = ?"""))
            {
                ps.setString(1, JobService.JobStatus.DONE.name());
                ps.setString(2, id);
                ps.setString(3, status.name());
                var ret = ps.executeUpdate();
                if (ret != 1) {
                    throw new RuntimeException("Cannot update, job not found or status is different");
                }
            }
        });
    }

    @Override
    public List<JobService.Job> listCreated(@Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, (conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(String.format("""
                SELECT %s FROM job
                WHERE status = 'CREATED'
                """, FIELDS)))
            {
                var rs = ps.executeQuery();

                final ArrayList<JobService.Job> list = new ArrayList<>();
                while (rs.next()) {
                    var id = rs.getString(1);
                    var providerClass = rs.getString(2);
                    var status = JobService.JobStatus.valueOf(rs.getString(3));
                    var serializerInput = rs.getString(4);
                    var startAfter = rs.getTimestamp(5).toInstant();

                    list.add(new JobService.Job(id, providerClass, status, serializerInput, startAfter));
                }
                return list;
            }
        });
    }
}
