package ai.lzy.storage;

import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.storage.data.StorageDataSource;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.annotation.Nonnull;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

@Factory
public class BeanFactory {

    @Singleton
    @Named("StorageServiceServerExecutor")
    public ExecutorService workersPool() {
        return Executors.newFixedThreadPool(16,
            new ThreadFactory() {
                private static final Logger LOG = LogManager.getLogger(StorageServiceGrpc.class);

                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    var th = new Thread(r, "storage-service-worker-" + counter.getAndIncrement());
                    th.setUncaughtExceptionHandler(
                        (t, e) -> LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
                    return th;
                }
            });
    }

    @Singleton
    @Requires(beans = StorageDataSource.class)
    @Named("StorageOperationDao")
    public OperationDao operationDao(StorageDataSource storage) {
        return new OperationDaoImpl(storage);
    }
}
