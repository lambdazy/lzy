package ai.lzy.longrunning;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

public final class OperationsExecutor {
    private static final Logger LOG = LogManager.getLogger(OperationsExecutor.class);

    private volatile ScheduledThreadPoolExecutor executor;
    private final AtomicInteger counter = new AtomicInteger(1);
    private final Runnable onError;
    private final Predicate<Error> injectedFailure;
    private final AtomicBoolean terminating = new AtomicBoolean(false);
    private final AtomicInteger runningOperations = new AtomicInteger(0);

    public OperationsExecutor(int corePoolSize, int maxPoolSize, Runnable onError, Predicate<Error> injectedFailure) {
        this.onError = onError;
        this.injectedFailure = injectedFailure;
        this.executor = create(corePoolSize, maxPoolSize, onError, injectedFailure);
    }

    public void startNew(Runnable op) {
        if (terminating.get()) {
            throw new RejectedExecutionException("Cannot start new operation, service is terminating...");
        }

        try {
            runningOperations.getAndIncrement();
            executor.submit(op);
        } catch (Exception e) {
            runningOperations.getAndDecrement();
            throw e;
        }
    }

    public void retryAfter(Runnable op, Duration delay) {
        try {
            runningOperations.getAndIncrement();
            executor.schedule(op, delay.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            runningOperations.getAndDecrement();
            throw e;
        }
    }

    @PreDestroy
    public void shutdown() {
        shutdown(Duration.ofMinutes(1));
    }

    public void shutdown(Duration timeout) {
        if (!terminating.compareAndSet(false, true)) {
            while (!executor.isShutdown()) {
                LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
            }
            return;
        }

        if (executor.isShutdown()) {
            return;
        }

        LOG.info("Shutdown OperationsExecutor service. Tasks in queue: {}, running tasks: {}, total: {}.",
            executor.getQueue().size(), executor.getActiveCount(), runningOperations.get());

        var deadline = Instant.now().plus(timeout);
        int step = 0;
        while (runningOperations.get() > 0 && Instant.now().isBefore(deadline)) {
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
            if (++step % 100 == 0) {
                LOG.info("Remains {} running operation(s)...", runningOperations.get());
            }
        }

        if (runningOperations.get() > 0) {
            LOG.error("Not all actions were completed in timeout, tasks in queue: {}, running tasks: {}",
                executor.getQueue().size(), executor.getActiveCount());
        }

        executor.shutdownNow();

        LOG.info("OperationsExecutor terminated");
    }

    @VisibleForTesting
    public void dropAll() throws InterruptedException {
        LOG.info("Drop all tasks");
        var q = executor.shutdownNow();
        var ok = executor.awaitTermination(5, TimeUnit.SECONDS);
        assert ok;
        LOG.info("{} tasks dropped", q.size());
        LockSupport.parkNanos(Duration.ofMillis(300).toNanos());
        executor = create(executor.getCorePoolSize(), executor.getMaximumPoolSize(), onError, injectedFailure);
    }

    private ScheduledThreadPoolExecutor create(int corePoolSize, int maxPoolSize, Runnable onError,
                                               Predicate<Error> injectedFailure)
    {
        var executor = new ScheduledThreadPoolExecutor(
            corePoolSize,
            r -> {
                var th = new Thread(r, "operations-executor-" + counter.getAndIncrement());
                th.setUncaughtExceptionHandler((t, e) -> {
                    onError.run();
                    LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e);
                });
                return th;
            })
        {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                runningOperations.getAndDecrement();
                super.afterExecute(r, t);
                if (t == null && r instanceof Future<?> f) {
                    try {
                        f.get();
                    } catch (CancellationException ce) {
                        t = ce;
                    } catch (ExecutionException ee) {
                        t = ee.getCause();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt(); // ignore/reset
                    } catch (Error e) {
                        if (injectedFailure.test(e)) {
                            LOG.error("Got InjectedFailure exception at {}: {}",
                                r.getClass().getName(), e.getMessage());
                        } else {
                            throw e;
                        }
                    }
                }
                if (t != null) {
                    onError.run();
                    LOG.error("Unexpected exception {}: {}", t.getClass().getSimpleName(), t.getMessage(), t);
                }
            }
        };

        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.setMaximumPoolSize(maxPoolSize);

        return executor;
    }
}
