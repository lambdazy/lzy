package ai.lzy.test.impl.v2;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.utils.GrpcConfig;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.util.auth.credentials.RsaUtils;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Singleton
public class PythonContext {
    private static final Logger LOG = LogManager.getLogger(PythonContext.class);
    protected static final Path scenarios = Paths.get("../pylzy/tests/scenarios/");
    protected static final String condaPrefix = "eval \"$(conda shell.bash hook)\" && "
        + "conda activate py39 && ";

    private final Map<String, String> envs;
    private final Path file;

    public PythonContext(WorkflowContext workflow, WhiteboardContext whiteboard, IamContext iam, ServiceConfig cfg)
            throws IOException, InterruptedException
    {
        var client = new SubjectServiceGrpcClient(
            "subject-service",
            new GrpcConfig(iam.address().getHost(), iam.address().getPort()),
            () -> cfg.getIam().createRenewableToken().get()
        );

        var keys = RsaUtils.generateRsaKeys();
        final Path path = Path.of(System.getProperty("user.dir"), "../lzy-test-cert.pem");
        if (Files.exists(path)) {
            Files.delete(path);
        }
        file = Files.createFile(path);
        FileUtils.write(file.toFile(), keys.privateKey(), StandardCharsets.UTF_8);


        client.createSubject(
            AuthProvider.GITHUB, "test", SubjectType.USER, SubjectCredentials.publicKey("test", keys.publicKey()));

        envs = Map.of(
            "LZY_ENDPOINT", workflow.address().toString(),
            "LZY_KEY_PATH", file.toAbsolutePath().toString(),
            "LZY_USER", "test",
            "LZY_WHITEBOARD_ENDPOINT", whiteboard.publicAddress().toString(),
            "FETCH_STATUS_PERIOD_SEC", "0"
        );
        for (var entry : envs.entrySet()) {
            LOG.info(entry.getKey() + ":\t" + entry.getValue());
        }
    }

    @PreDestroy
    public void close() {
        try {
            Files.delete(file);
        } catch (IOException e) {
            throw new RuntimeException(e);
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

    public void evalAndAssertScenarioResult(String scenarioName) {
        evalAndAssertScenarioResult(scenarioName, List.of());
    }

    public void evalAndAssertScenarioResult(String scenarioName, List<String> extraPyLibs) {
        LOG.info("Starting scenario: " + scenarioName);
        var result = evalScenario(Map.of(), scenarioName, extraPyLibs);
        LOG.info(scenarioName + ": STDOUT: {}", result.stdout());
        LOG.info(scenarioName + ": STDERR: {}", result.stderr());
        assertWithExpected(scenarioName, result);
    }

    private record ExecResult(int rc, String stdout, String stderr) {}
}
