package ai.lzy.test.scenarios;

import ai.lzy.test.LzyTerminalTestContext;
import ai.lzy.test.LzyTerminalTestContext.Terminal;
import ai.lzy.test.LzyTerminalTestContext.Terminal.ExecutionResult;
import ai.lzy.test.impl.TerminalThreadContext;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public abstract class LzyBaseTest {

    private static final Logger LOG = LogManager.getLogger(LzyBaseTest.class);

    protected static Path scenarios = Paths.get("../pylzy/tests/scenarios/");
    protected static final String condaPrefix = "eval \"$(conda shell.bash hook)\" && "
        + "conda activate py39 && ";

    protected LzyTerminalTestContext terminalContext;

    @Before
    public void setUp() {
        terminalContext = new TerminalThreadContext();
    }

    @After
    public void tearDown() {
        terminalContext.close();
    }

    public static ExecutionResult execInCondaEnv(Terminal term, Map<String, String> env, String cmd) {
        return term.execute(env, "bash", "-c", condaPrefix + cmd);
    }

    public static ExecutionResult execInCondaEnv(Terminal term, String cmd) {
        return execInCondaEnv(term, Map.of(), cmd);
    }

    public static void forEachLineInFile(File file, Consumer<String> action) throws IOException {
        LineIterator outIt = FileUtils.lineIterator(file, "UTF-8");
        try {
            while (outIt.hasNext()) {
                action.accept(outIt.nextLine().stripTrailing());
            }
        } finally {
            outIt.close();
        }
    }

    public static ExecutionResult evalScenario(Terminal term, String scenario, List<String> extraPyLibs,
                                               String customMnt)
    {
        return evalScenario(term, Map.of("LZY_MOUNT", customMnt), scenario, extraPyLibs);
    }

    public static ExecutionResult evalScenario(Terminal term, String scenarioName) {
        return evalScenario(term, Map.of(), scenarioName, List.of());
    }

    public static ExecutionResult evalScenario(Terminal term, Map<String, String> env, String scenario,
                                               List<String> extraPyLibs)
    {
        final Path scenarioPath = scenarios.resolve(scenario);
        if (!scenarioPath.toFile().exists()) {
            LOG.error("THERE IS NO SUCH SCENARIO: {}", scenario);
            Assert.fail();
        }

        // install extra python libraries if provided any
        if (!extraPyLibs.isEmpty()) {
            final String pipCmd = "pip install " + String.join(" ", extraPyLibs);
            final ExecutionResult pipInstallResult = execInCondaEnv(term, env, pipCmd);
            if (!pipInstallResult.stdout().isEmpty()) {
                LOG.info(scenario + " pip install : STDOUT: {}", pipInstallResult.stdout());
            }
            if (!pipInstallResult.stderr().isEmpty()) {
                LOG.info(scenario + " pip install : STDERR: {}", pipInstallResult.stderr());
            }
        }

        // run scenario and return result
        final String pythonCmd = "python " + scenarioPath.resolve("__init__.py");
        return execInCondaEnv(term, env, pythonCmd);
    }

    public static void assertWithExpected(String scenarioName, ExecutionResult result) {
        try {
            final Path scenario = scenarios.resolve(scenarioName);
            final File stdout_file = scenario.resolve("expected_stdout").toFile();
            forEachLineInFile(stdout_file, line -> {
                LOG.info("assert check if stdout contains: {}", line);
                Assert.assertTrue("'" + result.stdout() + "' doesn't contain '" + line + "'",
                    result.stdout().contains(line));
            });

            final File stderr_file = scenario.resolve("expected_stderr").toFile();
            forEachLineInFile(stderr_file, line -> {
                LOG.info("assert check if stderr contains: {}", line);
                Assert.assertTrue("'" + result.stderr() + "' doesn't contain '" + line + "'",
                    result.stderr().contains(line));
            });
        } catch (IOException ioexc) {
            LOG.error("Happened while was reading one of expected files: ", ioexc);
            Assert.fail();
        }
    }

    public static void evalAndAssertScenarioResult(Terminal term, String scenarioName) {
        evalAndAssertScenarioResult(term, scenarioName, List.of());
    }

    public static void evalAndAssertScenarioResult(Terminal term, String scenarioName, List<String> extraPyLibs) {
        LOG.info("Starting scenario: " + scenarioName);
        ExecutionResult result = evalScenario(term, Map.of(), scenarioName, extraPyLibs);
        LOG.info(scenarioName + ": STDOUT: {}", result.stdout());
        LOG.info(scenarioName + ": STDERR: {}", result.stderr());
        assertWithExpected(scenarioName, result);
    }
}
