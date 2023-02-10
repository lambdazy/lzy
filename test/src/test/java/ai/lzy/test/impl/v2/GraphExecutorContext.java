package ai.lzy.test.impl.v2;

import ai.lzy.allocator.configs.ServiceConfig;
import ai.lzy.graph.GraphExecutorApi;
import ai.lzy.test.impl.Utils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.v1.graph.GraphExecutorGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Singleton
public class GraphExecutorContext  {
    private static final int GRAPH_EXECUTOR_PORT = 19475;

    private final HostAndPort address;
    private final ApplicationContext context;
    private final GraphExecutorApi graphExecutor;
    private final ManagedChannel channel;
    private final GraphExecutorGrpc.GraphExecutorBlockingStub stub;
    private final RenewableJwt internalUserCreds;

    @Inject
    public GraphExecutorContext(SchedulerContext scheduler, IamContext iam) {

        this.address = HostAndPort.fromParts("localhost", GRAPH_EXECUTOR_PORT);
        final var dbOpts = Utils.createModuleDatabase("graph-executor");
        var opts = Utils.loadModuleTestProperties("graph-executor");

        opts.putAll(dbOpts);

        opts.putAll(new HashMap<String, Object>(Map.of(
                "graph-executor.port", GRAPH_EXECUTOR_PORT,
                "graph-executor.executors-count", 1,
                "graph-executor.scheduler.host", scheduler.address().getHost(),
                "graph-executor.scheduler.port", scheduler.address().getPort(),
                "graph-executor.iam.address", iam.address()
        )));

        this.context = ApplicationContext.run(opts);
        graphExecutor = context.getBean(GraphExecutorApi.class);
        try {
            graphExecutor.start();
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }

        this.channel = ChannelBuilder.forAddress(address)
                .usePlaintext()
                .enableRetry(GraphExecutorGrpc.SERVICE_NAME)
                .build();

        internalUserCreds = context.getBean(ServiceConfig.class).getIam().createRenewableToken();

        this.stub = GraphExecutorGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.authorization(() -> internalUserCreds.get().token()));
    }

    @PreDestroy
    public void close() {
        channel.shutdown();
        try {
            channel.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        graphExecutor.close();
        context.stop();
    }

    public HostAndPort address() {
        return address;
    }

    public GraphExecutorGrpc.GraphExecutorBlockingStub stub() {
        return stub;
    }

    public RenewableJwt internalUserCreds() {
        return internalUserCreds;
    }
}
