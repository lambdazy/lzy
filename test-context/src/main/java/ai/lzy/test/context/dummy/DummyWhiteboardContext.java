package ai.lzy.test.context.dummy;

import ai.lzy.whiteboard.test.WhiteboardContext;
import io.micronaut.context.annotation.Requires;
import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;

import java.nio.file.Path;
import java.util.Map;

import static ai.lzy.test.context.LzyContext.DUMMY_CONTEXT_NAME;

@Singleton
@Secondary
@Requires(env = DUMMY_CONTEXT_NAME)
public class DummyWhiteboardContext implements WhiteboardContext {
    @Override
    public void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) {}

    @Override
    public void tearDown() {}
}
