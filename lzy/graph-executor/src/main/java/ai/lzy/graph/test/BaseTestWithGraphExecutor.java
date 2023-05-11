package ai.lzy.graph.test;

import ai.lzy.graph.config.ServiceConfig;
import ai.lzy.test.GrpcUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class BaseTestWithGraphExecutor {
    private ApplicationContext context;
    private GraphExecutorDecorator graphExecutor;

    private int port;

    public void before() throws IOException, InterruptedException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException, InterruptedException {
        var graphExecutorConfig = new YamlPropertySourceLoader()
            .read("graph-executor", new FileInputStream("../graph-executor/src/main/resources/application-test.yml"));
        graphExecutorConfig.putAll(overrides);
        context = ApplicationContext.run(PropertySource.of(graphExecutorConfig), "test-mock");
        var config = context.getBean(ServiceConfig.class);
        if (config.getPort() == 0) {
            config.setPort(GrpcUtils.rollPort());
        }
        port = config.getPort();
        this.graphExecutor = context.getBean(GraphExecutorDecorator.class);
        this.graphExecutor.start();
    }

    public void after() throws SQLException, InterruptedException {
        graphExecutor.close();
        graphExecutor.awaitTermination();
        context.stop();
    }

    public ApplicationContext getContext() {
        return context;
    }

    public int getPort() {
        return port;
    }
}
