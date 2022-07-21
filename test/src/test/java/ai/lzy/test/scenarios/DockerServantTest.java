package ai.lzy.test.scenarios;

import ai.lzy.test.LzyServerTestContext;
import ai.lzy.test.LzyTerminalTestContext;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class DockerServantTest extends LocalScenario {

    @Before
    public void setUp() {
        setUp(LzyServerTestContext.LocalServantAllocatorType.DOCKER_ALLOCATOR);
        startTerminalWithDefaultConfig();
    }

    @Test
    @Ignore
    public void testSingleDockerEcho42() {
        testEcho42("docker-single", null);
    }

    @Test
    @Ignore
    public void testDefaultEnvEcho42() {
        testEcho42("default-env", "default");
    }

    @Test
    @Ignore
    public void testCustomEnvEcho42() {
        testEcho42("custom-env", "lzydock/default-env:for-tests");
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

        Instant beforeRun = Instant.now();
        System.out.println("DockerServantTest: run " + operationName + " at " + beforeRun.toString());

        final LzyTerminalTestContext.Terminal.ExecutionResult result = terminal.run(echo42.name(), "", Map.of());

        Instant afterRun = Instant.now();
        System.out.println("DockerServantTest: run finished " + operationName + " at " + afterRun.toString()
                           + "; spent " + Duration.between(beforeRun, afterRun).getSeconds() + "s");

        Assert.assertEquals("42\n", result.stdout());
    }

    @Test
    public void customImageTest() {
        evalAndAssertScenarioResult(terminal, "custom-image-cpu");
    }

}
