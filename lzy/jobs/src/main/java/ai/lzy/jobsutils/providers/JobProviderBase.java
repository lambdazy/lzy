package ai.lzy.jobsutils.providers;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.providers.JobSerializer.SerializationException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import javax.annotation.Nullable;

public abstract class JobProviderBase<T> implements JobProvider {
    private static final Logger LOG = LogManager.getLogger(JobProviderBase.class);

    final JobSerializerBase<T> serializer;
    final JobService jobService;
    final Class<T> type;

    protected JobProviderBase(JobSerializerBase<T> serializer, JobService jobService, Class<T> type) {
        this.serializer = serializer;
        this.jobService = jobService;
        this.type = type;
    }

    protected abstract void executeJob(T arg);

    @Override
    public void execute(Object data) {
        if (type.isInstance(data)) {
            executeJob(type.cast(data));
        } else {
            LOG.error("Cannot cast {} to {}", data.getClass().getName(), type.getName());
            throw new RuntimeException("Error while executing job");
        }
    }

    public void schedule(T arg, @Nullable Duration startAfter) throws SerializationException {
        jobService.create(this, serializer, arg, startAfter);
    }
}
