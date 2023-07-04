package ai.lzy.storage.test;

import java.nio.file.Path;
import java.util.Map;

public interface StorageContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
