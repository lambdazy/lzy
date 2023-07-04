package ai.lzy.allocator.test;

import java.nio.file.Path;
import java.util.Map;

public interface AllocatorContext {
    void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) throws Exception;

    void tearDown();
}
