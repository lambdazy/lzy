package ai.lzy.whiteboard;

import ai.lzy.iam.clients.AccessBindingClient;
import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.whiteboard.grpc.WhiteboardService;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
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
    @Named("WhiteboardServiceServerExecutor")
    public ExecutorService workersPool() {
        return Executors.newFixedThreadPool(16,
            new ThreadFactory() {
                private static final Logger LOG = LogManager.getLogger(WhiteboardService.class);

                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    var th = new Thread(r, "whiteboard-service-worker-" + counter.getAndIncrement());
                    th.setUncaughtExceptionHandler(
                        (t, e) -> LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
                    return th;
                }
            });
    }

    @Singleton
    @Named("WhiteboardIamToken")
    public RenewableJwt iamToken(AppConfig config) {
        return config.getIam().createRenewableToken();
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("WhiteboardIamGrpcChannel")
    public ManagedChannel iamChannel(AppConfig config) {
        return GrpcUtils.newGrpcChannel(config.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    @Named("WhiteboardIamAccessBindingClient")
    public AccessBindingClient iamAccessBindingClient(
        @Named("WhiteboardIamGrpcChannel") ManagedChannel iamChannel,
        @Named("WhiteboardIamToken") RenewableJwt iamToken)
    {
        return new AccessBindingServiceGrpcClient(WhiteboardApp.APP, iamChannel, iamToken::get);
    }

    @Singleton
    @Named("WhiteboardIamAccessClient")
    public AccessClient iamAccessClient(
        @Named("WhiteboardIamGrpcChannel") ManagedChannel iamChannel,
        @Named("WhiteboardIamToken") RenewableJwt iamToken)
    {
        return new AccessServiceGrpcClient(WhiteboardApp.APP, iamChannel, iamToken::get);
    }

    @Singleton
    @Named("WhiteboardIamSubjectClient")
    public SubjectServiceClient iamSubjectClient(
        @Named("WhiteboardIamGrpcChannel") ManagedChannel iamChannel,
        @Named("WhiteboardIamToken") RenewableJwt iamToken)
    {
        return new SubjectServiceGrpcClient(WhiteboardApp.APP, iamChannel, iamToken::get);
    }

    @Singleton
    @Requires(beans = WhiteboardDataSource.class)
    @Named("WhiteboardOperationDao")
    public OperationDao operationDao(WhiteboardDataSource dataSource) {
        return new OperationDaoImpl(dataSource);
    }
}
