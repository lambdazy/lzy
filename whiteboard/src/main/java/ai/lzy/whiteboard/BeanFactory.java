package ai.lzy.whiteboard;

import ai.lzy.iam.clients.AccessBindingClient;
import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;
import javax.sql.DataSource;

@Factory
public class BeanFactory {

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("WhiteboardIamGrpcChannel")
    public ManagedChannel iamChannel(AppConfig config) {
        return ChannelBuilder
            .forAddress(config.getIam().getAddress())
            .usePlaintext()
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
    }

    @Singleton
    @Named("WhiteboardIamAccessBindingClient")
    public AccessBindingClient iamAccessBindingClient(
        @Named("WhiteboardIamGrpcChannel") ManagedChannel iamChannel, AppConfig config)
    {
        return new AccessBindingServiceGrpcClient(iamChannel, config.getIam()::createCredentials);
    }

    @Singleton
    @Named("WhiteboardIamSubjectClient")
    public SubjectServiceClient iamSubjectClient(
        @Named("WhiteboardIamGrpcChannel") ManagedChannel iamChannel, AppConfig config)
    {
        return new SubjectServiceGrpcClient(iamChannel, config.getIam()::createCredentials);
    }

}
