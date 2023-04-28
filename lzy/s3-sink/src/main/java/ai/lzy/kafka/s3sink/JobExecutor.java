package ai.lzy.kafka.s3sink;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import jakarta.annotation.Nullable;
import jakarta.annotation.PreDestroy;
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

    private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private final BlockingQueue<JobHandle> queue = new DelayQueue<>();
    private final Map<String, JobHandle> handles = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    // For tests only
    private final Map<String, CompletableFuture<Job.PollResult>> waiters = new ConcurrentHashMap<>();

    public JobExecutor() {
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

    public void submit(Job job) {
        var handle = new JobHandle(job);
        handles.put(job.id(), handle);
        queue.add(handle);
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

        private JobHandle(Job job) {
            this.job = job;
        }

        public synchronized void run() {
            final Job.PollResult res;
            try {
                res = job.poll();
            } catch (Exception e) {
                LOG.error("Error while polling job {}: ", job, e);
                return;  // Error logged, dropping job
            }

            if (res.completed()) {
                handles.remove(job.id());

                var waiter = waiters.remove(job.id());
                if (waiter != null)  {  // For tests only
                    waiter.complete(res);
                }

                return;  // Job is completed, dropping it
            }

            nextRun.set(Instant.now().plus(res.pollAfter()));
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
    public CompletableFuture<Job.PollResult> setupWaiter(String jobId) {
        if (!handles.containsKey(jobId)) {
            return null;
        }

        var fut = new CompletableFuture<Job.PollResult>();
        waiters.put(jobId, fut);
        return fut;
    }
}
