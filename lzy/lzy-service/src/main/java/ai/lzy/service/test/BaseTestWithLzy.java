package ai.lzy.service.test;

import ai.lzy.service.App;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import static ai.lzy.test.GrpcUtils.rollPort;
import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class BaseTestWithLzy {
    private ApplicationContext lzyCtx;
    private App lzyApp;
    private int port;

    private ManagedChannel grpcChannel;
    private LzyWorkflowServiceBlockingStub grpcClient;
    private LzyWorkflowPrivateServiceBlockingStub privateGrpcClient;
    private LongRunningServiceBlockingStub grpcLongRunningClient;

    public void before() throws IOException, InterruptedException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var lzyProps = new YamlPropertySourceLoader().read("lzy-service",
            new FileInputStream("../lzy-service/src/main/resources/application-test.yml"));
        lzyProps.putAll(overrides);
        lzyCtx = ApplicationContext.run(PropertySource.of(lzyProps));

        var config = lzyCtx.getBean(LzyServiceConfig.class);
        port = rollPort();
        config.setAddress("localhost:" + port);

        lzyApp = lzyCtx.getBean(App.class);
        lzyApp.start();

        grpcChannel = newGrpcChannel(
            "localhost:" + getPort(), LzyWorkflowServiceGrpc.SERVICE_NAME, LongRunningServiceGrpc.SERVICE_NAME
        );
        grpcClient = newBlockingClient(LzyWorkflowServiceGrpc.newBlockingStub(grpcChannel), "Test-Client", null);
        privateGrpcClient = newBlockingClient(
            LzyWorkflowPrivateServiceGrpc.newBlockingStub(grpcChannel), "Test-Client", null
        );
        grpcLongRunningClient = newBlockingClient(
            LongRunningServiceGrpc.newBlockingStub(grpcChannel), "Test-Client", null
        );
    }

    public void after() {
        grpcChannel.shutdown();
        lzyApp.shutdown(false);
        lzyCtx.close();
    }

    public int getPort() {
        return this.port;
    }

    public LzyWorkflowServiceBlockingStub getGrpcClient() {
        return grpcClient;
    }

    public LzyWorkflowPrivateServiceBlockingStub getPrivateGrpcClient() {
        return privateGrpcClient;
    }

    public LongRunningServiceBlockingStub getGrpcLongRunningClient() {
        return grpcLongRunningClient;
    }
}
