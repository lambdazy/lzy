package ai.lzy.site;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.SchedulerGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

@Factory
public class BeanFactory {

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("SchedulerGrpcChannel")
    public ManagedChannel schedulerChannel(ServiceConfig config) {
        return ChannelBuilder
            .forAddress(HostAndPort.fromString(config.getSchedulerAddress()))
            .usePlaintext()
            .enableRetry(SchedulerGrpc.SERVICE_NAME)
            .build();
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("AllocatorIamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return ChannelBuilder
            .forAddress(config.getIam().getAddress())
            .usePlaintext() // TODO
            .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
            .build();
    }

    @Bean
    @Singleton
    public SubjectServiceGrpcClient subjectService(ServiceConfig config) {
        final IamClientConfiguration iam = config.getIam();
        return new SubjectServiceGrpcClient(
            GrpcConfig.from(iam.getAddress()),
            iam::createCredentials
        );
    }

    @Bean
    @Singleton
    @Named("Scheduler")
    public SchedulerGrpc.SchedulerBlockingStub scheduler(
        ServiceConfig serviceConfig,
        @Named("SchedulerGrpcChannel") ManagedChannel schedulerChannel)
    {
        return SchedulerGrpc.newBlockingStub(schedulerChannel)
            .withInterceptors(
                ClientHeaderInterceptor.header(
                    GrpcHeaders.AUTHORIZATION,
                    () -> serviceConfig.getIam().createCredentials().token()
                )
            );
    }
}
