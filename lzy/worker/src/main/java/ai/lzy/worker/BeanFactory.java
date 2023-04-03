package ai.lzy.worker;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AllowInternalUserOnlyInterceptor;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.kafka.KafkaHelper;
import ai.lzy.v1.channel.LzyChannelManagerGrpc;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Named;
import jakarta.inject.Singleton;

import java.nio.file.Path;

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
        return GrpcUtils.newGrpcChannel(config.getIam().getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
    }

    @Singleton
    @Bean(preDestroy = "shutdown")
    @Named("WorkerChannelManagerGrpcChannel")
    public ManagedChannel channelManagerChannel(ServiceConfig config) {
        return GrpcUtils.newGrpcChannel(config.getChannelManagerAddress(), LzyChannelManagerGrpc.SERVICE_NAME);
    }

    @Singleton
    public LzyFsServer lzyFsServer(ServiceConfig config,
                                   @Named("WorkerIamGrpcChannel") ManagedChannel iamChannel,
                                   @Named("WorkerChannelManagerGrpcChannel") ManagedChannel channelManagerChannel,
                                   @Named("WorkerOperationService") LocalOperationService localOperationService)
    {
        return new LzyFsServer(
            config.getVmId(),
            Path.of(config.getMountPoint()),
            HostAndPort.fromParts(config.getHost(), config.getFsPort()),
            channelManagerChannel,
            iamChannel,
            config.getIam().createRenewableToken(),
            localOperationService,
            /* isPortal */ false
        );
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("WorkerServer")
    public Server server(ServiceConfig config,
                         @Named("WorkerIamGrpcChannel") ManagedChannel iamChannel,
                         @Named("WorkerOperationService") LocalOperationService localOperationService,
                         WorkerApiImpl workerApi)
    {
        var app = "Worker-" + config.getVmId();
        var auth = new AuthServerInterceptor(new AuthenticateServiceGrpcClient(app, iamChannel));
        var internalOnly = new AllowInternalUserOnlyInterceptor(app, iamChannel);

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
    @Requires(property = "worker.kafka.enabled", value = "true")
    public KafkaHelper kafkaHelper(ServiceConfig config) {
        return new KafkaHelper(config.getKafka());
    }
}
