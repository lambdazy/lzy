package ai.lzy.channelmanager;

import ai.lzy.channelmanager.v2.config.ChannelManagerConfig;
import ai.lzy.channelmanager.v2.dao.ChannelManagerDataSource;
import ai.lzy.channelmanager.v2.lock.GrainedLock;
import ai.lzy.longrunning.OperationService;
import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.longrunning.dao.OperationDaoImpl;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {

    @Singleton
    public GrainedLock lockManager(ChannelManagerConfig config) {
        return new GrainedLock(config.getLockBucketsCount());
    }

    @Singleton
    @Named("ChannelManagerOperationDao")
    public OperationDao operationDao(ChannelManagerDataSource dataSource) {
        return new OperationDaoImpl(dataSource);
    }

    @Singleton
    public OperationService operationService(@Named("ChannelManagerOperationDao") OperationDao operationDao)
    {
        return new OperationService(operationDao);
    }

    @Singleton
    @Named("ChannelManagerIamToken")
    public RenewableJwt iamToken(ChannelManagerConfig config) {
        return config.getIam().createRenewableToken();
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("ChannelManagerIamGrpcChannel")
    public ManagedChannel iamChannel(ChannelManagerConfig config) {
        return GrpcUtils.newGrpcChannel(config.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("ChannelManagerWorkflowGrpcChannel")
    public ManagedChannel workflowGrpcChannel(ChannelManagerConfig config) {
        return GrpcUtils.newGrpcChannel(config.getLzyServiceAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

}
