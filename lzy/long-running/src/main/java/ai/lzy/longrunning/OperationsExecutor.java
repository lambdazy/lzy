package ai.lzy.longrunning;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

public final class OperationsExecutor extends ScheduledThreadPoolExecutor {
    private static final Logger LOG = LogManager.getLogger(OperationsExecutor.class);

    private final Runnable onError;
    private final Predicate<Error> injectedFailure;

    public OperationsExecutor(int corePoolSize, int maxPoolSize, Runnable onError, Predicate<Error> injectedFailure) {
        super(corePoolSize, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                var th = new Thread(r, "operations-executor-" + counter.getAndIncrement());
                th.setUncaughtExceptionHandler((t, e) -> {
                    onError.run();
                    LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e);
                });
                return th;
            }
        });

        this.onError = onError;
        this.injectedFailure = injectedFailure;

        setKeepAliveTime(1, TimeUnit.MINUTES);
        setMaximumPoolSize(maxPoolSize);
    }

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
                    LOG.error("Got InjectedFailure exception at {}: {}", r.getClass().getName(), e.getMessage());
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

    @Override
    public void shutdown() {
        LOG.info("Shutdown OperationsExecutor service. Tasks in queue: {}, running tasks: {}.",
            getQueue().size(), getActiveCount());

        super.shutdown();

        try {
            var allDone = awaitTermination(1, TimeUnit.MINUTES);
            if (!allDone) {
                LOG.error("Not all actions were completed in timeout, tasks in queue: {}, running tasks: {}",
                    getQueue().size(), getActiveCount());
                shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Graceful termination interrupted, tasks in queue: {}, running tasks: {}.",
                getQueue().size(), getActiveCount());
        }
    }

    @VisibleForTesting
    public void dropAll() {
        LOG.info("Drop all tasks");
        getQueue().clear();
    }
}
