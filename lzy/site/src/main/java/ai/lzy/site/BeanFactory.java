package ai.lzy.site;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.util.auth.credentials.RenewableJwt;
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

    @Singleton
    @Named("SiteIamToken")
    public RenewableJwt iamToken(ServiceConfig serviceConfig) {
        return serviceConfig.getIam().createRenewableToken();
    }

    @Singleton
    @Named("Scheduler")
    public SchedulerGrpc.SchedulerBlockingStub scheduler(
        @Named("SchedulerGrpcChannel") ManagedChannel schedulerChannel,
        @Named("SiteIamToken") RenewableJwt iamToken)
    {
        return newBlockingClient(SchedulerGrpc.newBlockingStub(schedulerChannel), "LzySite",
            () -> iamToken.get().token());
    }

    @Singleton
    public SubjectServiceGrpcClient subjectService(ServiceConfig config, @Named("SiteIamToken") RenewableJwt iamToken) {
        return new SubjectServiceGrpcClient(
            "LzySite",
            GrpcConfig.from(config.getIam().getAddress()),
            iamToken::get
        );
    }

    @Singleton
    public AuthenticateService authService(ServiceConfig config) {
        return new AuthenticateServiceGrpcClient(
                "LzySite",
                GrpcConfig.from(config.getIam().getAddress())
        );
    }
}
