package ai.lzy.test.impl.v2;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.logs.KafkaConfig;
import ai.lzy.service.App;
import ai.lzy.test.impl.Utils;
import ai.lzy.test.impl.v2.AllocatorContext.PortalAllocatorContext;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import com.google.common.net.HostAndPort;
import io.github.embeddedkafka.*;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import scala.collection.immutable.Map$;

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

    private final EmbeddedK kafka;

    @Inject
    public WorkflowContext(
        GraphExecutorContext graph,
        PortalAllocatorContext allocator,
        ChannelManagerContext channel,
        IamContext iam,
        StorageContext storage,
        WhiteboardContext whiteboard
    )
    {
        scala.collection.immutable.Map<String, String> conf = Map$.MODULE$.empty();
        var config = EmbeddedKafkaConfig$.MODULE$.apply(
            8001,
            8002,
            conf,
            conf,
            conf
        );
        kafka = EmbeddedKafka.start(config);
        var opts = Utils.loadModuleTestProperties("lzy-service");
        opts.putAll(Utils.createModuleDatabase("lzy-service"));

        KafkaConfig.KafkaHelper.setUseAuth(false);

        opts.putAll(Map.of(
            "lzy-service.kafka.bootstrap-servers", "localhost:8001",
            "lzy-service.kafka.username", "test",
            "lzy-service.kafka.password", "test",
            "lzy-service.kafka.enabled", "true"
        ));
        opts.putAll(Map.of(
            "lzy-service.whiteboard-address", whiteboard.privateAddress(),
            "lzy-service.allocator-address", allocator.address(),
            "lzy-service.graph-executor-address", graph.address(),
            "lzy-service.channel-manager-address", channel.address(),
            "lzy-service.address", address,
            "lzy-service.iam.address", iam.address(),
            "lzy-service.storage.address", storage.address()
        ));

        ctx = ApplicationContext.run(opts);
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
        kafka.stop(true);
        chn.shutdownNow();
        chn.awaitTermination(10, TimeUnit.SECONDS);
        main.shutdown(true);
        main.awaitTermination();
        ctx.close();
    }
}
