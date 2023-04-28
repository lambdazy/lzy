package ai.lzy.scheduler;

import ai.lzy.model.db.DbHelper;
import ai.lzy.scheduler.db.JobDao;
import ai.lzy.scheduler.providers.JobProvider;
import ai.lzy.scheduler.providers.JobSerializer;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Singleton
public class JobService {
    private static final Logger LOG = LogManager.getLogger(JobService.class);
    private final JobDao dao;
    private final ApplicationContext context;

    // TODO(artolord) add config here
    private final ScheduledThreadPoolExecutor pool = new ScheduledThreadPoolExecutor(16);
    private final ConcurrentHashMap<String, JobProvider> providers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, JobSerializer> serializers = new ConcurrentHashMap<>();

    public JobService(JobDao dao, ApplicationContext context) {
        this.dao = dao;
        this.context = context;
        pool.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        pool.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        pool.setRemoveOnCancelPolicy(true);
        restore();
    }

    /**
     * Restore jobs pool
     */
    private void restore() {
        final List<Job> jobs;
        try {
            jobs = DbHelper.withRetries(LOG, () -> dao.listToRestore(null));
        } catch (Exception e) {
            LOG.error("Cannot restore job service: ", e);
            return;
        }

        LOG.debug("Restoring {} jobs: {}", jobs.size(), jobs);

        for (var job: jobs) {
            LOG.debug("Restoring job {} for provider {}", job.id, job.providerClass);
            pool.schedule(() -> executeJob(job, null), Duration.between(Instant.now(), job.startAfter()).toMillis(),
                TimeUnit.MILLISECONDS);
        }
    }

    public void create(JobProvider provider, JobSerializer serializer, @Nullable Object input,
                       @Nullable Duration startAfter) throws JobSerializer.SerializationException
    {
        var id = UUID.randomUUID().toString();

        var dur = startAfter == null ? Duration.ZERO : startAfter;

        var job = new Job(
            id,
            provider.getClass().getName(),
            serializer.getClass().getName(),
            JobStatus.CREATED,
            serializer.serialize(input),
            Instant.now().plus(dur)
        );

        try {
            DbHelper.withRetries(LOG, () -> dao.insert(job, null));
        } catch (Exception e) {
            LOG.error("Error while inserting job: ", e);
            throw new RuntimeException(e);
        }
        LOG.debug("Scheduling to execute job {} for provider {} after {} ms", job.id,
            job.providerClass, dur.toMillis());
        pool.schedule(() -> executeJob(job, input), dur.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void executeJob(Job job, @Nullable Object arg) {
        LOG.debug("Starting to execute job {} of provider {}", job.id, job.providerClass);

        // Getting job from dao
        try {
            DbHelper.withRetries(LOG, () -> dao.executing(job.id, null));
        } catch (Exception e) {
            LOG.error("Error while getting job {} from dao", job.id, e);
            return;
        }

        final JobProvider provider;

        if (providers.containsKey(job.providerClass)) {
            provider = providers.get(job.providerClass);
        } else {

            // Getting provider from context
            try {
                provider = (JobProvider) context.getBean(Class.forName(job.providerClass()));
                providers.put(job.providerClass(), provider);
            } catch (Exception e) {
                try {
                    LOG.error("Cannot get provider {} for job", job.providerClass);
                    DbHelper.withRetries(LOG, () -> dao.complete(job.id, JobStatus.EXECUTING, null));
                } catch (Exception ex) {
                    LOG.error("Cannot fail job for provider {}:", job.providerClass, e);
                }
                return;
            }
        }

        // Executing job
        try {
            if (arg != null) {
                provider.execute(arg);
            } else {
                final JobSerializer serializer;
                if (serializers.containsKey(job.serializerClass)) {
                    serializer = serializers.get(job.serializerClass);
                } else {
                    serializer = (JobSerializer) context.getBean(Class.forName(job.serializerClass));
                    serializers.put(job.serializerClass, serializer);
                }

                var input = serializer.deserialize(job.serializedInput);
                provider.execute(input);
            }
        } catch (Exception e) {
            LOG.error("Error while executing job for provider {}: ", job.providerClass, e);
        }

        // Completing job
        try {
            DbHelper.withRetries(LOG, () -> dao.complete(job.id, JobStatus.EXECUTING, null));
        } catch (Exception e) {
            LOG.error("Error while completing job {}: ", job.providerClass, e);
        }

        LOG.debug("Completing job {}", job.id);
    }

    @PreDestroy
    public void stop() {
        LOG.info("Stopping JobService");
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
        String serializerClass,  // Argument serializer class, must be a bean and implementation of JobSerializer
        JobStatus status,  // Current status of job
        @Nullable String serializedInput,  // Serialized input of job
        Instant startAfter  // Delay start of job to duration
    ) { }
}
