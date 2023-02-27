package ai.lzy.allocator.test;

import ai.lzy.allocator.AllocatorMain;
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

    public void before() throws IOException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var allocatorConfig = new YamlPropertySourceLoader()
            .read("allocator", new FileInputStream("../allocator/src/main/resources/application-test.yml"));
        allocatorConfig.putAll(overrides);
        context = ApplicationContext.run(PropertySource.of(allocatorConfig), "test-mock");
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
}
