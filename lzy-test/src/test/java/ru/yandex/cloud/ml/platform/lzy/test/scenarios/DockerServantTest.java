package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.test.LzyServerTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

public class DockerServantTest extends LocalScenario {

    @Before
    public void setUp() {
        setUp(LzyServerTestContext.LocalServantAllocatorType.DOCKER_ALLOCATOR);
        startTerminalWithDefaultConfig();
    }

    @Test
    public void testSingleDockerEcho42() {
        testEcho42("docker-single", null);
    }

    @Test
    public void testDefaultEnvEcho42() {
        testEcho42("default-env", "default");
    }

    @Test
    public void testCustomEnvEcho42() {
        testEcho42("custom-env", "lzydock/default-env:for-cpu-tests");
    }

    private void testEcho42(String operationName, String baseEnv) {
        final FileIOOperation echo42 = new FileIOOperation(
            operationName,
            Collections.emptyList(),
            Collections.emptyList(),
            "echo 42",
            baseEnv
        );

        //Act
        terminal.publish(echo42);
        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.run(echo42.name(), "", Map.of());

        //Assert
        Assert.assertEquals("42\n", result.stdout());
    }

    @Test
    @Ignore
    public void testDefaultEnvPython() {
        // TODO (lindvv) python api
    }

}
