package ai.lzy.channelmanager.operation;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.channelmanager.dao.ChannelManagerDataSource;
import jakarta.annotation.Nonnull;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
public class ChannelOperationExecutor extends ScheduledThreadPoolExecutor {

    private static final Logger LOG = LogManager.getLogger(ChannelOperationExecutor.class);


    public ChannelOperationExecutor(ChannelManagerConfig config, ChannelManagerDataSource storage) {
        super(config.getExecutorThreadsCount(), new ExecutorThreadFactory());
        this.setKeepAliveTime(1, TimeUnit.MINUTES);
        this.setMaximumPoolSize(20);
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
            }
        }
        if (t != null) {
            LOG.error("Unexpected exception: {}", t.getMessage(), t);
        }
    }

    @Override
    @PreDestroy
    public void shutdown() {
        LOG.info("Shutdown executor, tasks in queue: {}, running tasks: {}", getQueue().size(), getActiveCount());
        super.shutdown();

        try {
            if (!awaitTermination(1, TimeUnit.MINUTES)) {
                shutdownNow();
            }
        } catch (InterruptedException e) {
            LOG.error("Shutdown executor interrupted, tasks in queue: {}, running tasks: {}",
                getQueue().size(), getActiveCount());
        } finally {
            shutdownNow();
        }
    }

    private static class ExecutorThreadFactory implements ThreadFactory {

        private final AtomicInteger counter = new AtomicInteger(1);

        @Override
        public Thread newThread(@Nonnull Runnable r) {
            var th = new Thread(r, "executor-" + counter.getAndIncrement());
            th.setUncaughtExceptionHandler(
                (t, e) -> LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
            return th;
        }

    }
}
