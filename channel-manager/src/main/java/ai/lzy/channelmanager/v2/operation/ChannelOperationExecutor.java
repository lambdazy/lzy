package ai.lzy.channelmanager.v2.operation;

import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import io.micronaut.context.annotation.Bean;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

@Singleton
@Bean(preDestroy = "shutdown")
public class ChannelOperationExecutor extends ScheduledThreadPoolExecutor {

    private static final Logger LOG = LogManager.getLogger(ChannelOperationExecutor.class);

    public ChannelOperationExecutor(ChannelManagerConfig config) {
        super(config.getExecutorThreadsCount(), new ExecutorThreadFactory());
        this.setKeepAliveTime(1, TimeUnit.MINUTES);
        this.setMaximumPoolSize(20);
    }

    @Override
    public void shutdown() {
        LOG.info("Shutdown executor, tasks in queue: {}, running tasks: {}",
            getQueue().size(), getActiveCount());
        super.shutdown();

        try {
            //noinspection ResultOfMethodCallIgnored
            awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            LOG.error("Shutdown executor interrupted, tasks in queue: {}, running tasks: {}",
                getQueue().size(), getActiveCount());
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        return super.shutdownNow();
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
