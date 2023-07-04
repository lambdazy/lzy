package ai.lzy.test.impl.v2;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.service.App;
import ai.lzy.service.BeanFactory;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.test.impl.Utils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;

@Singleton
public class WorkflowContext {
    public static final HostAndPort address = HostAndPort.fromParts("localhost", 13579);

    private final LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub stub;
    private final LongRunningServiceGrpc.LongRunningServiceBlockingStub opsStub;
    private final ApplicationContext ctx;
    private final App main;
    private final ManagedChannel chn;

    @Inject
    public WorkflowContext(
        GraphExecutorContext graph,
        AllocatorContext.WorkerAllocatorContext allocator,
        ChannelManagerContext channel,
        IamContext iam,
        StorageContext storage,
        WhiteboardContext whiteboard,
        KafkaContext kafka
    )
    {
        var opts = Utils.loadModuleTestProperties("lzy-service");
        opts.putAll(Utils.createModuleDatabase("lzy-service"));

        opts.put("lzy-service.kafka.bootstrap-servers", kafka.getBootstrapServers());
        opts.putAll(Map.of(
            "lzy-service.whiteboard-address", whiteboard.privateAddress(),
            "lzy-service.allocator-address", allocator.address(),
            "lzy-service.graph-executor-address", graph.address(),
            "lzy-service.channel-manager-address", channel.address(),
            "lzy-service.address", address,
            "lzy-service.iam.address", iam.address(),
            "lzy-service.storage.address", storage.address()
        ));

        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(true);

        ctx = ApplicationContext.run(opts, BeanFactory.TEST_ENV_NAME);
        main = ctx.getBean(App.class);
        try {
            main.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        chn = ChannelBuilder.forAddress(address)
            .usePlaintext()
            .enableRetry(LzyWorkflowServiceGrpc.SERVICE_NAME)
            .build();

        var creds = ctx.getBean(ServiceConfig.class).getIam().createRenewableToken();
        stub = LzyWorkflowServiceGrpc.newBlockingStub(chn)
            .withInterceptors(ClientHeaderInterceptor.authorization(() -> creds.get().token()));
        opsStub = newBlockingClient(LongRunningServiceGrpc.newBlockingStub(chn), "TestClient",
            () -> creds.get().token());
    }

    public HostAndPort address() {
        return address;
    }

    public LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub stub() {
        return stub;
    }

    public LongRunningServiceGrpc.LongRunningServiceBlockingStub opsStub() {
        return opsStub;
    }

    @PreDestroy
    private void close() throws InterruptedException {
        chn.shutdownNow();
        chn.awaitTermination(10, TimeUnit.SECONDS);
        main.shutdown(true);
        main.awaitTermination();
        ctx.close();
    }
}
