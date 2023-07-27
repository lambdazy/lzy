package ai.lzy.graph.test;

import ai.lzy.graph.GraphExecutorApi;
import ai.lzy.test.context.GraphExecutorContext;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import jakarta.inject.Singleton;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static ai.lzy.graph.test.GraphExecutorContextImpl.ENV_NAME;

@Singleton
@Requires(env = ENV_NAME)
public class GraphExecutorContextImpl implements GraphExecutorContext {
    public static final String ENV_NAME = "common_graph_executor_context";

    private ApplicationContext micronautContext;
    private GraphExecutorApi graphExecutorApp;

    @Override
    public void setUp(String baseConfigPath, Map<String, Object> configOverrides, String... environments)
        throws IOException, InterruptedException
    {
        try (var file = new FileInputStream(Objects.requireNonNull(baseConfigPath))) {
            var actualConfig = new YamlPropertySourceLoader().read("graph-executor", file);
            actualConfig.putAll(configOverrides);
            micronautContext = ApplicationContext.run(PropertySource.of(actualConfig), environments);
        }

        graphExecutorApp = micronautContext.getBean(GraphExecutorApi.class);
        graphExecutorApp.start();
    }

    @Override
    public void tearDown() {
        graphExecutorApp.close();
        try {
            graphExecutorApp.awaitTermination();
        } catch (InterruptedException e) {
            // intentionally blank
        } finally {
            micronautContext.stop();
        }
    }

    public GraphExecutorDecorator getGraphExecutor() {
        return micronautContext.getBean(GraphExecutorDecorator.class);
    }
}
