package ai.lzy.longrunning;

import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.PreDestroy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;

public final class OperationsExecutor {
    private static final Logger LOG = LogManager.getLogger(OperationsExecutor.class);

    private volatile ScheduledThreadPoolExecutor delegate;
    private final AtomicInteger counter = new AtomicInteger(1);
    private final Runnable onError;
    private final Predicate<Error> injectedFailure;

    public OperationsExecutor(int corePoolSize, int maxPoolSize, Runnable onError, Predicate<Error> injectedFailure) {
        this.onError = onError;
        this.injectedFailure = injectedFailure;
        this.delegate = create(corePoolSize, maxPoolSize, onError, injectedFailure);
    }

    public Future<?> submit(Runnable task) {
        return delegate.submit(task);
    }

    public Future<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return delegate.schedule(command, delay, unit);
    }

    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown OperationsExecutor service. Tasks in queue: {}, running tasks: {}.",
            delegate.getQueue().size(), delegate.getActiveCount());

        delegate.shutdown();

        try {
            var allDone = delegate.awaitTermination(1, TimeUnit.MINUTES);
            if (!allDone) {
                LOG.error("Not all actions were completed in timeout, tasks in queue: {}, running tasks: {}",
                    delegate.getQueue().size(), delegate.getActiveCount());
                delegate.shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Graceful termination interrupted, tasks in queue: {}, running tasks: {}.",
                delegate.getQueue().size(), delegate.getActiveCount());
        }
    }

    @VisibleForTesting
    public void dropAll() throws InterruptedException {
        LOG.info("Drop all tasks");
        var q = delegate.shutdownNow();
        var ok = delegate.awaitTermination(5, TimeUnit.SECONDS);
        assert ok;
        LOG.info("{} tasks dropped", q.size());
        LockSupport.parkNanos(Duration.ofMillis(300).toNanos());
        delegate = create(delegate.getCorePoolSize(), delegate.getMaximumPoolSize(), onError, injectedFailure);
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
