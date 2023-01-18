package ai.lzy.service;

import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.service.data.storage.LzyServiceStorage;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmPoolServiceGrpc;
import ai.lzy.v1.channel.LzyChannelManagerPrivateGrpc;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import ai.lzy.v1.iam.LzyAccessBindingServiceGrpc;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.storage.LzyStorageServiceGrpc;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hubspot.jackson.datatype.protobuf.ProtobufModule;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Factory
public class BeanFactory {

    @Singleton
    @Named("LzyServiceServerExecutor")
    public ExecutorService workersPool() {
        return Executors.newFixedThreadPool(16,
            new ThreadFactory() {
                private static final Logger LOG = LogManager.getLogger(LzyService.class);

                private final AtomicInteger counter = new AtomicInteger(1);

                @Override
                public Thread newThread(@Nonnull Runnable r) {
                    var th = new Thread(r, "lzy-service-worker-" + counter.getAndIncrement());
                    th.setUncaughtExceptionHandler(
                        (t, e) -> LOG.error("Unexpected exception in thread {}: {}", t.getName(), e.getMessage(), e));
                    return th;
                }
            });
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("AllocatorServiceChannel")
    public ManagedChannel allocatorChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getAllocatorAddress(), AllocatorGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME, VmPoolServiceGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("StorageServiceChannel")
    public ManagedChannel storageChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getStorage().getAddress(), LzyStorageServiceGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("ChannelManagerServiceChannel")
    public ManagedChannel channelManagerChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getChannelManagerAddress(), LzyChannelManagerPrivateGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("IamServiceChannel")
    public ManagedChannel iamChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getIam().getAddress(), LzySubjectServiceGrpc.SERVICE_NAME,
            LzyAccessBindingServiceGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("GraphExecutorServiceChannel")
    public ManagedChannel graphExecutorChannel(LzyServiceConfig config) {
        return newGrpcChannel(config.getGraphExecutorAddress(), GraphExecutorGrpc.SERVICE_NAME,
            LongRunningServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    @Named("LzyServiceIamToken")
    public RenewableJwt renewableIamToken(LzyServiceConfig config) {
        return config.getIam().createRenewableToken();
    }

    @Singleton
    @Named("LzyServiceOperationDao")
    @Requires(notEnv = "test-mock")
    public OperationDao operationDao(LzyServiceStorage storage) {
        return new OperationDaoImpl(storage);
    }

    @Singleton
    @Named("GraphDaoObjectMapper")
    public ObjectMapper mapper() {
        return new ObjectMapper().registerModule(new ProtobufModule());
    }
}
