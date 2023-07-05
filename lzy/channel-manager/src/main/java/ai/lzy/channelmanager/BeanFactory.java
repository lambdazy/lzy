package ai.lzy.channelmanager;

import ai.lzy.channelmanager.config.ChannelManagerConfig;
import ai.lzy.iam.clients.AccessClient;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.longrunning.OperationsService;
import ai.lzy.longrunning.dao.OperationDao;
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
    public static final String TEST_ENV_NAME = "local-test";

    @Singleton
    @Named("ChannelManagerOperationService")
    public OperationsService operationService(@Named("ChannelManagerOperationDao") OperationDao operationDao) {
        return new OperationsService(operationDao);
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

    @Singleton
    @Named("ChannelManagerIamAccessClient")
    public AccessClient iamAccessClient(
        @Named("ChannelManagerIamGrpcChannel") ManagedChannel iamChannel,
        @Named("ChannelManagerIamToken") RenewableJwt iamToken)
    {
        return new AccessServiceGrpcClient(ChannelManagerMain.APP, iamChannel, iamToken::get);
    }

    @Singleton
    @Named("ChannelManagerIamSubjectClient")
    public SubjectServiceClient iamSubjectClient(
        @Named("ChannelManagerIamGrpcChannel") ManagedChannel iamChannel,
        @Named("ChannelManagerIamToken") RenewableJwt iamToken)
    {
        return new SubjectServiceGrpcClient(ChannelManagerMain.APP, iamChannel, iamToken::get);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("ChannelManagerWorkflowGrpcChannel")
    public ManagedChannel workflowGrpcChannel(ChannelManagerConfig config) {
        return GrpcUtils.newGrpcChannel(config.getLzyServiceAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }
}
