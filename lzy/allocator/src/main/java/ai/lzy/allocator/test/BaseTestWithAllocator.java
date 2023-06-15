package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
import ai.lzy.allocator.configs.ServiceConfig;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class BaseTestWithAllocator {
    private ApplicationContext context;
    private AllocatorMain allocator;

    private int port;

    public void before() throws IOException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var allocatorConfig = new YamlPropertySourceLoader()
            .read("allocator", new FileInputStream("../allocator/src/main/resources/application-test.yml"));
        allocatorConfig.putAll(overrides);
        context = ApplicationContext.run(PropertySource.of(allocatorConfig), "test-mock");
        var cfg = context.getBean(ServiceConfig.class);
        port = cfg.getPort();
        allocator = context.getBean(AllocatorMain.class);
        allocator.start();
    }

    public void after() throws SQLException, InterruptedException {
        allocator.destroyAllForTests();
        allocator.stop(false);
        allocator.awaitTermination();
        context.stop();
    }

    public ApplicationContext getContext() {
        return context;
    }

    public AllocatorServiceDecorator allocator() {
        return context.getBean(AllocatorServiceDecorator.class);
    }

    public int getPort() {
        return port;
    }

    public String getAddress() {
        return "localhost:" + getPort();
    }
}
