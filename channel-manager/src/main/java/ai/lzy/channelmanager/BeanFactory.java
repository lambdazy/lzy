package ai.lzy.channelmanager;

import ai.lzy.channelmanager.db.ChannelManagerDataSource;
import ai.lzy.channelmanager.lock.GrainedLock;
import ai.lzy.longrunning.OperationService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

@Factory
public class BeanFactory {

    @Singleton
    public GrainedLock lockManager(ChannelManagerConfig config) {
        return new GrainedLock(config.getLockBucketsCount());
    }

    @Singleton
    public OperationDao operationDao(ChannelManagerDataSource dataSource) {
        return new OperationDaoImpl(dataSource);
    }

    @Singleton
    public OperationService operationService(OperationDao operationDao) {
        return new OperationService(operationDao);
    }

    @Singleton
    @Named("ChannelManagerExecutor")
    @Bean(preDestroy = "shutdown")
    public ScheduledExecutorService executorService() {
        final var logger = LogManager.getLogger("ChannelManagerExecutor");

        var executor = new ScheduledThreadPoolExecutor(5, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(1);

            @Override
            public Thread newThread(@Nonnull Runnable r) {
                var th = new Thread(r, "executor-" + counter.getAndIncrement());
                th.setUncaughtExceptionHandler(
                    (t, e) -> logger.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
                return th;
            }
        }) {
            @Override
            public void shutdown() {
                logger.info("Shutdown ChannelManagerExecutor, tasks in queue: {}, running tasks: {}",
                    getQueue().size(), getActiveCount());
                super.shutdown();

                try {
                    //noinspection ResultOfMethodCallIgnored
                    awaitTermination(1, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    logger.error("Shutdown ChannelManagerExecutor interrupted, tasks in queue: {}, running tasks: {}",
                        getQueue().size(), getActiveCount());
                }
            }
        };

        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.setMaximumPoolSize(20);

        return executor;
    }

}
