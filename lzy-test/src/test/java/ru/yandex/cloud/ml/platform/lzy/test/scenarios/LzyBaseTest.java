package ru.yandex.cloud.ml.platform.lzy.test.scenarios;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext;
import ru.yandex.cloud.ml.platform.lzy.test.LzyTerminalTestContext.Terminal;
import ru.yandex.cloud.ml.platform.lzy.test.impl.TerminalThreadContext;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class LzyBaseTest {
    protected static final int    DEFAULT_TIMEOUT_SEC     = 30;
    protected static final int    DEFAULT_SERVANT_PORT    = 9999;
    protected static final int    DEFAULT_SERVANT_FS_PORT = 9998;
    protected static final int    DEFAULT_DEBUG_PORT      = 5006;
    protected static final String LZY_MOUNT               = "/tmp/lzy";

    protected static Path scenarios = Paths.get("../lzy-python/tests/scenarios/");
    protected static final String condaPrefix = "eval \"$(conda shell.bash hook)\" && " +
            "conda activate py39 && ";

    protected LzyTerminalTestContext terminalContext;
    @Before
    public void setUp() {
        terminalContext = new TerminalThreadContext();
    }

    @After
    public void tearDown() {
        terminalContext.close();
    }

    public LzyTerminalTestContext.Terminal.ExecutionResult execInCondaEnv(String cmd, Terminal term) {
        return execInCondaEnv(Map.of(), cmd, term);
    }

    public LzyTerminalTestContext.Terminal.ExecutionResult execInCondaEnv(Map<String, String> env, String cmd, Terminal term) {
        return term.execute(env, "bash", "-c", condaPrefix + cmd);
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


    public LzyTerminalTestContext.Terminal.ExecutionResult evalScenario(List<String> extraPyLibs, Path scenario,
                                                                        Logger LOG, Terminal term, String customMnt) {
        return evalScenario(extraPyLibs, scenario, LOG, term, Map.of("LZY_MOUNT", customMnt));
    }

    public LzyTerminalTestContext.Terminal.ExecutionResult evalScenario(List<String> extraPyLibs, String scenario,
                                                                        Logger LOG, Terminal term) {
        return evalScenario(extraPyLibs, scenarios.resolve(scenario), LOG, term, Map.of());
    }

    public LzyTerminalTestContext.Terminal.ExecutionResult evalScenario(String scenarioName, Logger LOG, Terminal term) {
        return evalScenario(List.of(), scenarioName, LOG, term);
    }
    public LzyTerminalTestContext.Terminal.ExecutionResult evalScenario(List<String> extraPyLibs, Path scenario,
                                                                        Logger LOG, Terminal term, Map<String, String> env) {
        if (!scenario.toFile().exists()) {
            LOG.error("THERE IS NO SUCH SCENARIO: {}", scenario);
            Assert.fail();
        }

        // install extra python libraries if provided any
        if (!extraPyLibs.isEmpty()) {
            execInCondaEnv(env, "pip install " + String.join(" ", extraPyLibs), term);
        }

        //Act
        return execInCondaEnv(env, "python " + scenario.resolve("__init__.py"), term);
    }

    public void assertWithExpected(String scenarioName, LzyTerminalTestContext.Terminal.ExecutionResult result, Logger LOG) {
        assertWithExpected(scenarios.resolve(scenarioName), result, LOG);
    }

    public void assertWithExpected (Path scenario, LzyTerminalTestContext.Terminal.ExecutionResult result, Logger LOG) {
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

    public void runAndCompareWithExpectedFile(List<String> extraPyLibs, String scenarioName, Logger LOG, Terminal term) {
        final Path scenario = scenarios.resolve(scenarioName);
        LzyTerminalTestContext.Terminal.ExecutionResult result = evalScenario(extraPyLibs, scenario, LOG, term, Map.of());
        LOG.info(scenarioName + ": STDOUT: {}", result.stdout());
        LOG.info(scenarioName + ": STDERR: {}", result.stderr());
        assertWithExpected(scenario, result, LOG);
    }

    public void runAndCompareWithExpectedFile(String scenarioName, Logger LOG, Terminal term) {
        runAndCompareWithExpectedFile(List.of(), scenarioName, LOG, term);
    }
}
