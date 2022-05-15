package ru.yandex.cloud.ml.platform.lzy.test.scenarios;


import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.impl.TerminalThreadContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static ru.yandex.cloud.ml.platform.lzy.test.impl.LzyPythonTerminalDockerContext.condaPrefix;

public abstract class LzyBaseTest {
    protected static final int DEFAULT_TIMEOUT_SEC = 30;
    protected static final int DEFAULT_SERVANT_PORT = 9999;
    protected static final int DEFAULT_SERVANT_FS_PORT = 9998;
    protected static final String LZY_MOUNT = "/tmp/lzy";
    protected LzyTerminalTestContext terminalContext;
    protected LzyTerminalTestContext.Terminal terminal;

    @Before
    public void setUp() {
        terminalContext = new TerminalThreadContext();
    }

    @After
    public void tearDown() {
        terminalContext.close();
    }

    public LzyTerminalTestContext.Terminal.ExecutionResult execInCondaEnv(String cmd) {
        return terminal.execute(Map.of(), "bash", "-c", condaPrefix + cmd);
    }

    public void runAndCompareWithExpectedFile(String scenarioName, Logger LOG) {
        runAndCompareWithExpectedFile(List.of(), scenarioName, LOG);
    }

    public void forEachLineInFile(File file, Consumer<String> action) throws IOException {
        LineIterator out_it = FileUtils.lineIterator(file, "UTF-8");
        try {
            while (out_it.hasNext()) {
                action.accept(out_it.nextLine().stripTrailing());
            }
        } finally {
            out_it.close();
        }
    }

    public void runAndCompareWithExpectedFile(List<String> extraPyLibs, String scenarioName, Logger LOG) {
        // Arrange
        final Path scenario = Paths.get("../lzy-python/tests/scenarios/").resolve(scenarioName);
        if (!scenario.toFile().exists()) {
            // TODO: early fail if there is no code for this particular scenario
        }

        // install extra python libraries if provided any
        if (!extraPyLibs.isEmpty()) {
            execInCondaEnv("pip install " + String.join(" ", extraPyLibs));
        }

        //Act
        final LzyTerminalTestContext.Terminal.ExecutionResult result =
                execInCondaEnv("python " + scenario.resolve("__init__.py"));

        LOG.info(scenarioName + ": STDOUT: {}", result.stdout());
        LOG.info(scenarioName + ": STDERR: {}", result.stderr());

        //Assert
        File stdout_file = scenario.resolve("expected_stdout").toFile();
        File stderr_file = scenario.resolve("expected_stderr").toFile();

        try {
            forEachLineInFile(stdout_file, line -> {
                LOG.info("assert check if stdout contains: {}", line);
                Assert.assertTrue(result.stdout().contains(line));
            });

            forEachLineInFile(stderr_file, line -> {
                LOG.info("assert check if stderr contains: {}", line);
                Assert.assertTrue(result.stderr().contains(line));
            });
        } catch (IOException ioexc) {
            LOG.error("Happened while was reading one of expected files: ", ioexc);
            Assert.fail();
        }
    }

}
