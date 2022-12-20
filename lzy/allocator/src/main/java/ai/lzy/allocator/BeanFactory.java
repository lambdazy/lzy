package ai.lzy.allocator;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.allocator.storage.AllocatorDataSource;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.metrics.DummyMetricReporter;
import ai.lzy.metrics.LogMetricReporter;
import ai.lzy.metrics.MetricReporter;
import ai.lzy.metrics.PrometheusMetricReporter;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import io.prometheus.client.CollectorRegistry;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import yandex.cloud.sdk.ServiceFactory;
import yandex.cloud.sdk.auth.Auth;
import yandex.cloud.sdk.auth.provider.CredentialProvider;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.inject.Named;

@Factory
public class BeanFactory {

    private static final Duration YC_CALL_TIMEOUT = Duration.ofSeconds(30);

    @Singleton
    @Requires(property = "allocator.yc-credentials.enabled", value = "true")
    public ServiceFactory serviceFactory(CredentialProvider credentialProvider,
                                         ServiceConfig.YcCredentialsConfig config)
    {
        return ServiceFactory.builder()
            .credentialProvider(credentialProvider)
            .endpoint(config.getEndpoint())
            .requestTimeout(YC_CALL_TIMEOUT)
            .build();
    }

    @Singleton
    @Requires(property = "allocator.yc-credentials.enabled", value = "true")
    public CredentialProvider credentialProvider(ServiceConfig.YcCredentialsConfig config) {
        return Auth.apiKeyBuilder()
            .fromFile(Path.of(config.getServiceAccountFile()))
            .cloudIAMEndpoint(config.getIamEndpoint())
            .build();
    }

    @Singleton
    @Named("AllocatorObjectMapper")
    public ObjectMapper mapper() {
        return new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("AllocatorIamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return GrpcUtils.newGrpcChannel(config.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    @Bean(preDestroy = "stop")
    @Requires(beans = ServiceConfig.MetricsConfig.class)
    public MetricReporter metricReporter(ServiceConfig.MetricsConfig config) {
        CollectorRegistry.defaultRegistry.clear();

        return switch (config.getKind()) {
            case Disabled -> new DummyMetricReporter();
            case Logger -> new LogMetricReporter(config.getLoggerName(),
                Level.valueOf(config.getLoggerLevel().toUpperCase()));
            case Prometheus -> new PrometheusMetricReporter(config.getPort());
        };
    }

    @Singleton
    @Named("AllocatorIamToken")
    public RenewableJwt renewableIamToken(ServiceConfig config) {
        return config.getIam().createRenewableToken();
    }

    @Singleton
    @Requires(beans = AllocatorDataSource.class)
    @Named("AllocatorOperationDao")
    public OperationDao operationDao(AllocatorDataSource storage) {
        return new OperationDaoImpl(storage);
    }

    @Singleton
    @Named("AllocatorExecutor")
    @Bean(preDestroy = "shutdown")
    @Requires(bean = AllocatorDataSource.class)
    public ScheduledExecutorService executorService(AllocatorDataSource storage) {
        final var logger = LogManager.getLogger("AllocatorExecutor");

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
                    logger.error("Unexpected exception: {}", t.getMessage(), t);
                }
            }

            @Override
            @PreDestroy
            public void shutdown() {
                logger.info("Shutdown AllocatorExecutor service. Tasks in queue: {}, running tasks: {}.",
                    getQueue().size(), getActiveCount());

                super.shutdown();

                try {
                    var allDone = awaitTermination(1, TimeUnit.MINUTES);
                    if (!allDone) {
                        logger.error("Not all actions were completed in timeout, tasks in queue: {}, running tasks: {}",
                            getQueue().size(), getActiveCount());
                    }
                } catch (InterruptedException e) {
                    logger.error("Graceful termination interrupted, tasks in queue: {}, running tasks: {}.",
                        getQueue().size(), getActiveCount());
                }
            }
        };

        executor.setKeepAliveTime(1, TimeUnit.MINUTES);
        executor.setMaximumPoolSize(20);

        return executor;
    }
}
