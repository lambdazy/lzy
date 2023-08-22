package ai.lzy.worker;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.iam.grpc.client.AccessServiceGrpcClient;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AccessServerInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.worker.WorkerApiGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.util.Set;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcServer;

@Factory
public class BeanFactory {

    @Singleton
    @Named("WorkerOperationService")
    public LocalOperationService localOperationService(ServiceConfig config) {
        return new LocalOperationService(config.getVmId());
    }

    @Singleton
    @Bean(preDestroy = "shutdown")
    @Named("WorkerIamGrpcChannel")
    public ManagedChannel iamChannel(ServiceConfig config) {
        return GrpcUtils.newGrpcChannel(config.getIamAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    @Bean(preDestroy = "shutdown")
    @Named("WorkerChannelManagerGrpcChannel")
    public ManagedChannel channelManagerChannel(ServiceConfig config) {
        return GrpcUtils.newGrpcChannel(config.getChannelManagerAddress(), LzyChannelManagerGrpc.SERVICE_NAME);
    }

    @Singleton
    @Named("InternalOnly")
    public AccessServerInterceptor internalOnlyInterceptor(ServiceConfig config,
                                                          @Named("WorkerIamGrpcChannel") ManagedChannel iamChannel)
    {
        var app = "Worker-" + config.getVmId();
        return new AccessServerInterceptor(
            new AccessServiceGrpcClient(app, iamChannel), () -> new JwtCredentials("empty"),
            Set.of(WorkerApiGrpc.getInitMethod()), Root.INSTANCE, AuthPermission.INTERNAL_AUTHORIZE);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("WorkerServer")
    public Server server(ServiceConfig config,
                         @Named("WorkerIamGrpcChannel") ManagedChannel iamChannel,
                         @Named("WorkerOperationService") LocalOperationService localOperationService,
                         @Named("InternalOnly") AccessServerInterceptor internalOnly,
                         WorkerApiImpl workerApi)
    {
        var app = "Worker-" + config.getVmId();
        var auth = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(app, iamChannel));

        return newGrpcServer("0.0.0.0", config.getApiPort(), auth)
            .addService(ServerInterceptors.intercept(workerApi, internalOnly))
            .addService(ServerInterceptors.intercept(localOperationService, internalOnly))
            .build();
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("AllocatorAgent")
    public AllocatorAgent allocatorAgent(ServiceConfig config) {
        return new AllocatorAgent(config.getAllocatorToken(), config.getVmId(), config.getAllocatorAddress(),
            config.getAllocatorHeartbeatPeriod());
    }

    @Singleton
    @Named("WorkerKafkaHelper")
    public KafkaHelper kafkaHelper(ServiceConfig config) {
        return new KafkaHelper(config.getKafka());
    }
}
