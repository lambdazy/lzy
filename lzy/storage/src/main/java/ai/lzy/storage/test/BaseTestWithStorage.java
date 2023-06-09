package ai.lzy.storage.test;

import ai.lzy.storage.App;
import ai.lzy.storage.config.StorageConfig;
import ai.lzy.test.GrpcUtils;
import com.google.common.net.HostAndPort;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

public class BaseTestWithStorage {
    private ApplicationContext context;
    private App storage;

    private int port;

    public void before() throws IOException {
        setUp(Map.of());
    }

    public void setUp(Map<String, Object> overrides) throws IOException {
        var storageConfig = new YamlPropertySourceLoader()
            .read("storage", new FileInputStream("../storage/src/main/resources/application-test.yml"));
        storageConfig.putAll(overrides);

        context = ApplicationContext.run(PropertySource.of(storageConfig));

        var config = context.getBean(StorageConfig.class);
        if (config.getAddress() == null) {
            config.setAddress("localhost:" + GrpcUtils.rollPort());
        }

        port = HostAndPort.fromString(config.getAddress()).getPort();

        storage = context.getBean(App.class);
        storage.start();
    }

    public void after() throws InterruptedException {
        storage.close(false);
        storage.awaitTermination();
        context.stop();
    }

    public int getPort() {
        return port;
    }

    public String getAddress() {
        return "localhost:" + getPort();
    }
}
