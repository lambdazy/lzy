package ai.lzy.test.impl.v2;

import ai.lzy.test.impl.Utils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class PythonContextBase {
    private static final Logger LOG = LogManager.getLogger(PythonContext.class);
    protected static final Path scenarios = Paths.get("../pylzy/tests/scenarios/");
    protected static final String condaPrefix = "eval \"$(conda shell.bash hook)\" && "
        + "conda activate py39 && ";

    private final Map<String, String> envs;

    public PythonContextBase(String endpoint, String wbEndpoint, String username, String keyPath) {

        envs = Map.of(
            "LZY_ENDPOINT", endpoint,
            "LZY_KEY_PATH", keyPath,
            "LZY_USER", username,
            "LZY_WHITEBOARD_ENDPOINT", wbEndpoint,
            "FETCH_STATUS_PERIOD_SEC", "0"
        );

        for (var entry : envs.entrySet()) {
            LOG.info(entry.getKey() + ":\t" + entry.getValue());
        }
    }

    public ExecResult execInCondaEnv(Map<String, String> env, String cmd, Path pwd) {
        var processBuilder = new ProcessBuilder();
        processBuilder.environment().putAll(env);
        processBuilder.environment().putAll(envs);
        processBuilder.directory(pwd.toFile());
        processBuilder.command("/bin/bash", "-c", condaPrefix + cmd);
        Process process;
        try {
            process = processBuilder.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            var outBuilder = new StringBuilder();
            var errBuilder = new StringBuilder();

            var out = new BufferedReader(new InputStreamReader(process.getInputStream()));
            var err = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            var threadOut = new Thread(() -> {
                while (true) {
                    try {
                        var l = out.readLine();
                        if (l == null) {
                            break;
                        }
                        outBuilder.append(l)
                            .append("\n");
                        LOG.debug(l);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }

            });

            var threadErr = new Thread(() -> {
                while (true) {
                    try {
                        var l = err.readLine();
                        if (l == null) {
                            break;
                        }
                        errBuilder.append(l)
                            .append("\n");
                        LOG.debug(l);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            threadOut.start();
            threadErr.start();

            var rc = process.waitFor();

            threadOut.join();
            threadErr.join();

            return new ExecResult(rc, outBuilder.toString(), errBuilder.toString());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void forEachLineInFile(File file, Consumer<String> action) throws IOException {
        try (LineIterator outIt = FileUtils.lineIterator(file, "UTF-8")) {
            while (outIt.hasNext()) {
                action.accept(outIt.nextLine().stripTrailing());
            }
        }
    }

    private static boolean containsAsSubstring(List<String> strs, String substr) {
        var iterator = strs.iterator();
        while (iterator.hasNext()) {
            var line = iterator.next();
            line = line.replaceAll("\\x1b\\[[0-9]+m", ""); // remove colors
            line = line.replaceFirst("\\[LZY-REMOTE-[^\\[]*\\] ", "");
            line = line.replaceFirst("\\[LZY-LOCAL\\] ", "");
            if (line.contains(substr)) {
                iterator.remove();
                return true;
            }
        }
        return false;
    }

    private ExecResult evalScenario(Map<String, String> env, String scenario, List<String> extraPyLibs) {
        final Path scenarioPath = scenarios.resolve(scenario).toAbsolutePath().normalize();
        if (!scenarioPath.toFile().exists()) {
            LOG.error("THERE IS NO SUCH SCENARIO: {}", scenario);
            Assert.fail();
        }

        // install extra python libraries if provided any
        if (!extraPyLibs.isEmpty()) {
            final String pipCmd = "pip install " + String.join(" ", extraPyLibs);
            LOG.info("Install extra python libs: " + pipCmd);
            var pipInstallResult = execInCondaEnv(env, pipCmd, scenarioPath);
            if (!pipInstallResult.stdout().isEmpty()) {
                LOG.info(scenario + " pip install : STDOUT: {}", pipInstallResult.stdout());
            }
            if (!pipInstallResult.stderr().isEmpty()) {
                LOG.info(scenario + " pip install : STDERR: {}", pipInstallResult.stderr());
            }
        }

        // run scenario and return result
        final String pythonCmd = "python __init__.py";
        return execInCondaEnv(env, pythonCmd, scenarioPath);
    }

    public void assertWithExpected(String scenarioName, ExecResult result) {
        try {
            final Path scenario = scenarios.resolve(scenarioName);
            final File stdout_file = scenario.resolve("expected_stdout").toFile();
            final List<String> stdout = new LinkedList<>(Arrays.asList(result.stdout.split("\n")));
            forEachLineInFile(stdout_file, line -> {
                LOG.info("assert check if stdout contains: {}", line);
                Assert.assertTrue("'" + result.stdout() + "' doesn't contain '" + line + "'",
                    containsAsSubstring(stdout, line));
            });

            final File stderr_file = scenario.resolve("expected_stderr").toFile();
            final List<String> stderr = new LinkedList<>(Arrays.asList(result.stderr.split("\n")));
            forEachLineInFile(stderr_file, line -> {
                LOG.info("assert check if stderr contains: {}", line);
                Assert.assertTrue("'" + result.stderr() + "' doesn't contain '" + line + "'",
                    containsAsSubstring(stderr, line));
            });
        } catch (IOException ioexc) {
            LOG.error("Happened while was reading one of expected files: ", ioexc);
            Assert.fail();
        }
    }

    public void evalAndAssertScenarioResult(String scenarioName) {
        evalAndAssertScenarioResult(scenarioName, List.of());
    }

    public void evalAndAssertScenarioResult(String scenarioName, List<String> extraPyLibs) {
        LOG.info("Starting scenario: " + scenarioName);
        var startTime = Instant.now();
        var result = evalScenario(Map.of(), scenarioName, extraPyLibs);
        var finishTime = Instant.now();
        LOG.info(scenarioName + ": STDOUT: {}", result.stdout());
        LOG.info(scenarioName + ": STDERR: {}", result.stderr());
        LOG.info(scenarioName + ": time spent: {}", Utils.toFormattedString(Duration.between(startTime, finishTime)));
        assertWithExpected(scenarioName, result);
    }

    private record ExecResult(int rc, String stdout, String stderr) {}
}
