package ai.lzy.kafka.s3sink;

import ai.lzy.common.IdGenerator;
import ai.lzy.common.RandomIdGenerator;
import ai.lzy.longrunning.Operation;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.kafka.KafkaS3Sink.StartRequest;
import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class JobExecutor {
    private static final Logger LOG = LogManager.getLogger(JobExecutor.class);
    private static final int THREAD_POOL_SIZE = 10;

    private static final ThreadGroup JOB_EXECUTOR_TG = new ThreadGroup("s3-sink-jobs");

    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE,
        r -> new Thread(JOB_EXECUTOR_TG, r, "s3-sink-worker"));
    private final BlockingQueue<JobHandle> queue = new DelayQueue<>();
    private final Map<String, JobHandle> handles = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final Map<String, String> idempotencyKeyToJobId = new ConcurrentHashMap<>();
    private final IdGenerator idGenerator = new RandomIdGenerator();
    private final KafkaHelper helper;
    private final S3SinkMetrics metrics;
    private final ServiceConfig config;

    // For tests only
    private final Map<String, CompletableFuture<Job.JobStatus>> waiters = new ConcurrentHashMap<>();

    public JobExecutor(@Named("S3SinkKafkaHelper") KafkaHelper helper, S3SinkMetrics metrics, ServiceConfig config) {
        this.helper = helper;
        this.metrics = metrics;
        this.config = config;

        for (int i = 0; i < THREAD_POOL_SIZE; i++) {
            threadPool.submit(() -> {
                while (!shutdown.get()) {
                    try {
                        queue.take().run();
                    } catch (Exception e) {
                        LOG.error("Exception while waiting for job from queue, exception is ignored: ", e);
                    }
                }
            });
        }
    }

    @PreDestroy
    public void shutdown() {
        shutdown.set(true);
        threadPool.shutdown();
    }

    public synchronized String submit(StartRequest req, @Nullable Operation.IdempotencyKey idempotencyKey) {
        var token = idempotencyKey == null ? null : idempotencyKey.token();

        if (token != null) {
            var jobId = idempotencyKeyToJobId.get(token);
            if (jobId != null) {
                return jobId;
            }
        }

        var job = new Job(idGenerator.generate("s3sink-"), helper, req, metrics, config);

        var handle = new JobHandle(job, token);
        handles.put(job.id(), handle);
        queue.add(handle);

        if (token != null) {
            idempotencyKeyToJobId.put(token, job.id());
        }

        return job.id();
    }

    public void complete(String id) {
        var handle = handles.get(id);

        if (handle == null) {
            throw Status.NOT_FOUND.asRuntimeException();
        }

        handle.job.complete();
    }

    private class JobHandle implements Delayed {
        private final AtomicReference<Instant> nextRun = new AtomicReference<>(Instant.now());
        private final Job job;
        private final String idempotencyKey;

        private JobHandle(Job job, @Nullable String idempotencyKey) {
            this.job = job;
            this.idempotencyKey = idempotencyKey;
        }

        public synchronized void run() {
            final Job.JobStatus res;
            try {
                res = job.run();
            } catch (Exception e) {
                LOG.error("Error while polling job {}: ", job, e);
                return;  // Error logged, dropping job
            }

            if (res.completed()) {
                handles.remove(job.id());

                if (idempotencyKey != null) {
                    idempotencyKeyToJobId.remove(idempotencyKey);
                }

                var waiter = waiters.remove(job.id());
                if (waiter != null) {  // For tests only
                    waiter.complete(res);
                }

                return;  // Job completed, drop it
            }

            nextRun.set(Instant.now().plus(res.restartAfter()));
            queue.add(this);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return Instant.now().until(nextRun.get(), unit.toChronoUnit());
        }

        @Override
        public int compareTo(Delayed o) {
            return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }
    }

    @VisibleForTesting
    @Nullable
    public CompletableFuture<Job.JobStatus> setupWaiter(String jobId) {
        if (!handles.containsKey(jobId)) {
            return null;
        }

        var fut = new CompletableFuture<Job.JobStatus>();
        waiters.put(jobId, fut);
        return fut;
    }
}
