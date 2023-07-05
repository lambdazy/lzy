package ai.lzy.test.context.dummy;

import ai.lzy.test.context.*;
import io.micronaut.context.annotation.Secondary;
import jakarta.inject.Singleton;

import java.nio.file.Path;
import java.util.Map;

@Singleton
@Secondary
public class DummyServiceContext implements IamContext, AllocatorContext, ChannelManagerContext, GraphExecutorContext,
    SchedulerContext, LzyServiceContext, StorageServiceContext, WhiteboardServiceContext
{
    @Override
    public void setUp(Path config, Map<String, Object> runtimeConfig, String... environments) {}

    @Override
    public void tearDown() {}
}
