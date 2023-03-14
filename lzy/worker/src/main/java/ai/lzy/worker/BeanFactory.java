package ai.lzy.worker;

import ai.lzy.allocator.AllocatorAgent;
import ai.lzy.fs.LzyFsServer;
import ai.lzy.longrunning.LocalOperationService;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.worker.env.EnvironmentFactory;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.micronaut.context.annotation.Bean;
import io.micronaut.context.annotation.Factory;
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
    public EnvironmentFactory environmentFactory(ServiceConfig config) {
        return new EnvironmentFactory(config.getGpuCount());
    }

    @Singleton
    public LzyFsServer lzyFsServer(ServiceConfig config,
                                   @Named("WorkerOperationService") LocalOperationService localOperationService)
    {
        final var cm = HostAndPort.fromString(config.getChannelManagerAddress());
        final var iam = HostAndPort.fromString(config.getIam().getAddress());

        return new LzyFsServer(
            config.getVmId(),
            Path.of(config.getMountPoint()),
            HostAndPort.fromParts(config.getHost(), config.getFsPort()),
            cm,
            iam,
            config.getIam().createRenewableToken(),
            localOperationService,
            false
        );
    }

    @Singleton
    public WorkerApiImpl workerApiImpl(ServiceConfig config,
                                       @Named("WorkerOperationService") LocalOperationService localOperationService,
                                       EnvironmentFactory environmentFactory,
                                       LzyFsServer lzyFsServer)
    {
        return new WorkerApiImpl(config, localOperationService, environmentFactory, lzyFsServer);
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("WorkerServer")
    public Server server(ServiceConfig config,
                         @Named("WorkerOperationService") LocalOperationService localOperationService,
                         WorkerApiImpl workerApi)
    {
        // TODO: https://st.yandex-team.ru/CLOUD-130892
        return newGrpcServer("0.0.0.0", config.getApiPort(), GrpcUtils.NO_AUTH)
            .addService(workerApi)
            .addService(localOperationService)
            .build();
    }

    @Bean(preDestroy = "shutdown")
    @Singleton
    @Named("AllocatorAgent")
    public AllocatorAgent allocatorAgent(ServiceConfig config) {
        return new AllocatorAgent(config.getAllocatorToken(), config.getVmId(), config.getAllocatorAddress(),
            config.getAllocatorHeartbeatPeriod());
    }
}
