package ai.lzy.service.test;

import ai.lzy.longrunning.dao.OperationDaoDecorator;
import ai.lzy.service.App;
import ai.lzy.service.BeanFactory;
import ai.lzy.service.config.LzyServiceConfig;
import ai.lzy.test.GrpcUtils;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowPrivateServiceGrpc.LzyWorkflowPrivateServiceBlockingStub;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

public class LzyServiceTestContext {
    private static final String grpcClientName = "Test-Client";

    private ApplicationContext lzyMicronautContext;
    private App lzyApp;
    private LzyServiceConfig serviceConfig;
    private ManagedChannel grpcChannel;

    public int servicePort() {
        return serviceAddress().getPort();
    }

    public HostAndPort serviceAddress() {
        return HostAndPort.fromString(serviceConfig.getAddress());
    }

    public LzyWorkflowServiceBlockingStub grpcClient() {
        return newBlockingClient(LzyWorkflowServiceGrpc.newBlockingStub(grpcChannel), grpcClientName, null);
    }

    public LzyWorkflowPrivateServiceBlockingStub privateGrpcClient() {
        return newBlockingClient(LzyWorkflowPrivateServiceGrpc.newBlockingStub(grpcChannel), grpcClientName, null);
    }

    public LongRunningServiceBlockingStub grpcLongRunningClient() {
        return newBlockingClient(LongRunningServiceGrpc.newBlockingStub(grpcChannel), grpcClientName, null);
    }

    public OperationDaoDecorator operationsDao() {
        return lzyMicronautContext.getBean(OperationDaoDecorator.class);
    }

    public void before() throws IOException, InterruptedException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> configOverrides) throws IOException {
        var lzyProps = new YamlPropertySourceLoader().read("lzy-service",
            new FileInputStream("../lzy-service/src/main/resources/application-test.yml"));
        lzyProps.putAll(configOverrides);
        lzyProps.put("lzy-service.address", HostAndPort.fromParts("localhost", GrpcUtils.rollPort()).toString());

        lzyMicronautContext = ApplicationContext.run(PropertySource.of(lzyProps), BeanFactory.testEnvName);
        serviceConfig = lzyMicronautContext.getBean(LzyServiceConfig.class);

        lzyApp = lzyMicronautContext.getBean(App.class);
        lzyApp.start();

        grpcChannel = newGrpcChannel(
            serviceAddress(), LzyWorkflowServiceGrpc.SERVICE_NAME, LongRunningServiceGrpc.SERVICE_NAME
        );
    }

    public void after() {
        grpcChannel.shutdownNow();
        lzyApp.shutdown(true);
        lzyMicronautContext.close();
    }
}
