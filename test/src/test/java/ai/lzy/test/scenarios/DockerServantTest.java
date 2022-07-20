package ai.lzy.test.scenarios;

import ai.lzy.test.LzyServerTestContext;
import ai.lzy.test.LzyTerminalTestContext;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
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
    public void customImageTest() {
        evalAndAssertScenarioResult(terminal, "custom-image-cpu");
    }

}
