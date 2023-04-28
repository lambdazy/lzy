package ai.lzy.scheduler.db;

import ai.lzy.model.db.DbOperation;
import ai.lzy.model.db.Storage;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.JobService;
import jakarta.annotation.Nullable;
import jakarta.inject.Singleton;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Singleton
public class JobDaoImpl implements JobDao {
    private static final String FIELDS = "id, provider_class, serializer_class, status, serialized_input, start_after";

    private final Storage storage;

    public JobDaoImpl(SchedulerDataSource storage) {
        this.storage = storage;
    }

    @Override
    public void insert(JobService.Job job, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, (conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(String.format("""
                INSERT INTO job (%s)
                 VALUES (?, ?, ?, ?, ?, ?)""", FIELDS)))
            {
                ps.setString(1, job.id());
                ps.setString(2, job.providerClass());
                ps.setString(3, job.serializerClass());
                ps.setString(4, job.status().name());
                ps.setString(5, job.serializedInput());
                ps.setTimestamp(6, Timestamp.from(job.startAfter()));

                ps.execute();
            }
        });
    }

    @Override
    public void executing(String id, @Nullable TransactionHandle tx) throws SQLException {
        DbOperation.execute(tx, storage, (conn) -> {
            try (PreparedStatement ps = conn.prepareStatement("""
                UPDATE job
                SET status = ?
                 WHERE id = ? AND status = 'CREATED'
                """))
            {
                ps.setString(1, JobService.JobStatus.EXECUTING.name());
                ps.setString(2, id);
                var ret = ps.executeUpdate();
                if (ret != 1) {
                    throw new RuntimeException("Cannot update, job not found or status is different");
                }
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
    public List<JobService.Job> listToRestore(@Nullable TransactionHandle tx) throws SQLException {
        return DbOperation.execute(tx, storage, (conn) -> {
            try (PreparedStatement ps = conn.prepareStatement(String.format("""
                UPDATE job
                SET status = 'CREATED'
                WHERE status != 'DONE'
                RETURNING %s
                """, FIELDS)))
            {
                var rs = ps.executeQuery();

                final ArrayList<JobService.Job> list = new ArrayList<>();
                while (rs.next()) {
                    var id = rs.getString(1);
                    var providerClass = rs.getString(2);
                    var serializerClass = rs.getString(3);
                    var status = JobService.JobStatus.valueOf(rs.getString(4));
                    var serializerInput = rs.getString(5);
                    var startAfter = rs.getTimestamp(6).toInstant();

                    list.add(new JobService.Job(id, providerClass, serializerClass,
                        status, serializerInput, startAfter));
                }
                return list;
            }
        });
    }
}
