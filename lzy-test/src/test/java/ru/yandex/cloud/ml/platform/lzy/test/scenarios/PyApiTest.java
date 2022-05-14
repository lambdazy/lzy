package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.servant.agents.AgentStatus;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static ru.yandex.cloud.ml.platform.lzy.test.impl.LzyPythonTerminalDockerContext.condaPrefix;

public class PyApiTest extends LzyBaseTest {
    private static final Logger LOG = LogManager.getLogger(PyApiTest.class);

    private LzyTerminalTestContext.Terminal terminal;

    public void arrangeTerminal(String user) {
        this.arrangeTerminal(LZY_MOUNT, FreePortFinder.find(20000, 21000), FreePortFinder.find(21000, 22000),
            kharonContext.serverAddress(), user, null);
    }

    public void arrangeTerminal(String mount, int port, int fsPort, String serverAddress, String user,
                                String keyPath) {
        int debugPort = FreePortFinder.find(22000, 23000);
        terminal = terminalContext.startTerminalAtPathAndPort(mount, port, fsPort, serverAddress,
            debugPort, user, keyPath);
        terminal.waitForStatus(
            AgentStatus.EXECUTING,
            DEFAULT_TIMEOUT_SEC,
            TimeUnit.SECONDS
        );
    }

    public LzyTerminalTestContext.Terminal.ExecutionResult execInCondaEnv(String cmd) {
        return terminal.execute(Map.of(), "bash", "-c", condaPrefix + cmd);
    }

    public void runAndCompareWithExpectedFile(String scenarioName) {
        runAndCompareWithExpectedFile(List.of(), scenarioName);
    }

    public String joinOutput(String stdout, String stderr) {
        return String.join("\n",
                "STDOUT: ", stderr,
                "STDERR: ", stdout);
    }

    public void forEveryLineOfFile(File file, Consumer<String> action) throws IOException {
        LineIterator out_it = FileUtils.lineIterator(file, "UTF-8");
        try {
            while (out_it.hasNext()) {
                action.accept(out_it.nextLine().stripTrailing());
            }
        } finally {
            out_it.close();
        }
    }

    public void runAndCompareWithExpectedFile(List<String> extraPyLibs, String scenarioName) {
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
            forEveryLineOfFile(stdout_file, line -> {
                LOG.info("assert check if stdout contains: {}", line);
                Assert.assertTrue(result.stdout().contains(line));
            });

            forEveryLineOfFile(stderr_file, line -> {
                LOG.info("assert check if stderr contains: {}", line);
                Assert.assertTrue(result.stderr().contains(line));
            });
        } catch (IOException ioexc) {
            LOG.error("Happened while was reading one of expected files: ", ioexc);
            Assert.fail();
        }
    }

    @Test
    public void testSimpleCatboostGraph() {
        /* This scenario checks for:
                1. Importing external modules (catboost)
                2. Functions which accept and return complex objects
         */
        arrangeTerminal("testUser");
        runAndCompareWithExpectedFile(List.of("catboost"), "catboost_integration_cpu");
    }

    @Test
    public void testExecFail() {
        //Arrange
        arrangeTerminal("phil");
        runAndCompareWithExpectedFile("exec_fail");
    }

    @Test
    public void testEnvFail() {
        //Arrange
        arrangeTerminal("phil");
        runAndCompareWithExpectedFile("env_fail");
    }

    @Test
    public void testCache() {
        //Arrange
        arrangeTerminal("testUser");
        runAndCompareWithExpectedFile("test_cache");
    }

    @Test
    public void testImportFile() {
        /* This scenario checks for:
                1. Importing local file package 
         */

        //Arrange
        arrangeTerminal("testUser");
        runAndCompareWithExpectedFile("import");
    }

    @Test
    public void testNoneResult() {
        /* This scenario checks for:
                1. Calling @op with None as result
         */

        //Arrange
        arrangeTerminal("testUser");
        runAndCompareWithExpectedFile("none_result");
    }

    @Test
    public void testWhiteboards() {
        /* This scenario checks for:
                1. Whiteboards/Views machinery
         */
        //Arrange
        arrangeTerminal("testUser");
        runAndCompareWithExpectedFile("whiteboards");
    }
}
