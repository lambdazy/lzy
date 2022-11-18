package ai.lzy.storage.test;

import ai.lzy.storage.App;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;

public class BaseTestWithStorage {
    private ApplicationContext context;
    private App storage;

    public void before() throws IOException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var storageConfig = new YamlPropertySourceLoader()
            .read("storage", new FileInputStream("../storage/src/main/resources/application-test.yml"));
        storageConfig.putAll(overrides);
        context = ApplicationContext.run(PropertySource.of(storageConfig));
        storage = new App(context);
        storage.start();
    }

    public void after() throws SQLException, InterruptedException {
        storage.close(false);
        storage.awaitTermination();
        context.stop();
    }
}
