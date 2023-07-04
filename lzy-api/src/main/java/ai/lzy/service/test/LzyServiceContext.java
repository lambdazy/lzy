package ai.lzy.service.test;

import java.nio.file.Path;
import java.util.Map;

public interface LzyServiceContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
