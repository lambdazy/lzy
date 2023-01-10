package ai.lzy.jobsutils;

import ai.lzy.jobsutils.db.JobDao;
import ai.lzy.jobsutils.providers.JobProvider;
import ai.lzy.model.db.DbHelper;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import javax.annotation.PreDestroy;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Singleton
public class JobService {
    private static final Logger LOG = LogManager.getLogger(JobService.class);
    private final JobDao dao;
    private final ApplicationContext context;

    // TODO(artolord) add config here
    private final ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(16);

    public JobService(JobDao dao, ApplicationContext context) {
        this.dao = dao;
        this.context = context;
        pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        restore();
    }

    /**
     * Restore jobs pool
     */
    public void restore() {
        final List<Job> jobs;
        try {
            jobs = DbHelper.withRetries(LOG, () -> dao.listCreated(null));
        } catch (Exception e) {
            LOG.error("Cannot restore job service: ", e);
            return;
        }

        for (var job: jobs) {
            pool.schedule(() -> executeJob(job.id), Duration.between(job.startAfter(),
                Instant.now()).toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    public void create(JobProvider provider, @Nullable Object input,
                       @Nullable Duration startAfter) throws JobProvider.SerializationException {
        var id = UUID.randomUUID().toString();

        var dur = startAfter == null ? Duration.ZERO : startAfter;

        var job = new Job(
            id,
            provider.getClass().getName(),
            JobStatus.CREATED,
            provider.serialize(input),
            Instant.now().plus(dur)
        );

        try {
            DbHelper.withRetries(LOG, () -> dao.insert(job, null));
        } catch (Exception e) {
            LOG.error("Error while inserting job: ", e);
            throw new RuntimeException(e);
        }

        pool.schedule(() -> executeJob(id), dur.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void executeJob(String jobId) {
        final Job job;

        // Getting job from dao
        try {
            job = DbHelper.withRetries(LOG, () -> dao.getForExecution(jobId, null));

            if (job == null) {
                LOG.error("Job {} already done or not found", jobId);
                return;
            }
        } catch (Exception e) {
            LOG.error("Error while getting job {} from dao", jobId, e);
            return;
        }

        final JobProvider provider;

        // Getting provider from context
        try {
            provider = (JobProvider) context.getBean(Class.forName(job.providerClass()));
        } catch (Exception e) {
            try {
                LOG.error("Cannot get provider {} for job", job.providerClass);
                DbHelper.withRetries(LOG, () -> dao.complete(jobId, JobStatus.EXECUTING, null));
            } catch (Exception ex) {
                LOG.error("Cannot fail job for provider {}:", job.providerClass, e);
            }

            return;
        }


        // Executing job
        try {
            provider.execute(job.serializedInput);
        } catch (Exception e) {
            LOG.error("Error while executing job for provider {}: ", job.providerClass, e);
        }

        // Completing job
        try {
            DbHelper.withRetries(LOG, () -> dao.complete(jobId, JobStatus.EXECUTING, null));
        } catch (Exception e) {
            LOG.error("Error while completing job {}: ", job.providerClass, e);
        }
    }

    @PreDestroy
    public void stop() {
        this.pool.shutdown();
        try {
            this.pool.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Not all jobs are finished in JobService");
        }
        this.pool.shutdownNow();
    }

    public enum JobStatus {
        CREATED,
        EXECUTING,
        DONE
    }

    public record Job(
        String id,  // Job id
        String providerClass,  // Job provider class name, must be a bean and implementation of JobProvider
        JobStatus status,  // Current status of job
        @Nullable String serializedInput,  // Serialized input of job
        Instant startAfter  // Delay start of job to duration
    ) { }
}
