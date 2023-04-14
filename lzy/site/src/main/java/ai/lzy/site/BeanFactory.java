package ai.lzy.site;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
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
    @Named("SiteIamChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return newGrpcChannel(config.getIam().getAddress(), LzySubjectServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    public SubjectServiceGrpcClient subjectService(@Named("SiteIamChannel") ManagedChannel iamChannel,
                                                   @Named("SiteIamToken") RenewableJwt iamToken)
    {
        return new SubjectServiceGrpcClient("LzySite", iamChannel, iamToken::get);
    }

    @Singleton
    public AuthenticateService authService(@Named("SiteIamChannel") ManagedChannel iamChannel) {
        return new AuthenticateServiceGrpcClient("LzySite", iamChannel);
    }
}
