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
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Factory
public class BeanFactory {

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
    @Requires(beans = LzyServiceStorage.class)
    @Named("LzyServiceOperationDao")
    public OperationDao operationDao(LzyServiceStorage storage) {
        return new OperationDaoImpl(storage);
    }
}
