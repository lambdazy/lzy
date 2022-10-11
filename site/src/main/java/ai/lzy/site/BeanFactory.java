package ai.lzy.site;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Factory
public class BeanFactory {

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("SchedulerGrpcChannel")
    public ManagedChannel schedulerChannel(ServiceConfig config) {
        return newGrpcChannel(config.getSchedulerAddress(), SchedulerGrpc.SERVICE_NAME);
    }

    @Bean
    @Singleton
    @Named("Scheduler")
    public SchedulerGrpc.SchedulerBlockingStub scheduler(
        ServiceConfig serviceConfig,
        @Named("SchedulerGrpcChannel") ManagedChannel schedulerChannel)
    {
        return newBlockingClient(SchedulerGrpc.newBlockingStub(schedulerChannel), "LzySite",
            () -> serviceConfig.getIam().createCredentials().token());
    }

    @Bean
    @Singleton
    public SubjectServiceGrpcClient subjectService(ServiceConfig config) {
        final IamClientConfiguration iam = config.getIam();
        return new SubjectServiceGrpcClient(
            "LzySite",
            GrpcConfig.from(iam.getAddress()),
            iam::createCredentials
        );
    }
}
