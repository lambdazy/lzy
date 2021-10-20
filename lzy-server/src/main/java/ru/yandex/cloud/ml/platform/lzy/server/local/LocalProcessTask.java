package ru.yandex.cloud.ml.platform.lzy.server.local;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;

public class LocalProcessTask extends BaseTask {
    private static final Logger LOG = LogManager.getLogger(LocalProcessTask.class);

    LocalProcessTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        ChannelsManager channels,
        URI serverURI
    ) {
        super(owner, tid, workload, assignments, channels, serverURI);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Override
    protected void runServantAndWaitFor(String serverHost, int serverPort, String servantHost, int servantPort, UUID tid, String token) {
        try {
            final File taskDir = File.createTempFile("lzy", "task");
            taskDir.delete();
            taskDir.mkdirs();
            taskDir.mkdir();
            final Process process = runJvm(
                "ru.yandex.cloud.ml.platform.lzy.servant.LzyServant", taskDir,
                new String[]{
                    "-z", serverHost + ":" + serverPort,
                    "-p", String.valueOf(servantPort),
                    "-m", taskDir.toString() + "/lzy"
                },
                Map.of(
                    "LZYTASK", tid.toString(),
                    "LZYTOKEN", token
                )
            );
            process.getOutputStream().close();
            ForkJoinPool.commonPool().execute(() -> {
                try (LineNumberReader lnr = new LineNumberReader(new InputStreamReader(
                    process.getInputStream(),
                    StandardCharsets.UTF_8
                ))) {
                    String line;
                    while ((line = lnr.readLine()) != null) {
                        LOG.info(line);
                    }
                } catch (IOException e) {
                    LOG.warn("Exception in local task", e);
                }
            });
            ForkJoinPool.commonPool().execute(() -> {
                try (LineNumberReader lnr = new LineNumberReader(new InputStreamReader(
                    process.getErrorStream(),
                    StandardCharsets.UTF_8
                ))) {
                    String line;
                    while ((line = lnr.readLine()) != null) {
                        LOG.warn(line);
                    }
                } catch (IOException e) {
                    LOG.warn("Exception in local task", e);
                }
            });
            final int rc = process.waitFor();
            LOG.info("LocalTask process exited with exit code: " + rc);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static Process runJvm(
        final String mainClass,
        File wd,
        final String[] args,
        final Map<String, String> env
    ) {

        try {
            ProcessBuilder pb = new ProcessBuilder();
            pb.directory(wd);
            final List<String> parameters = pb.command();
            parameters.add(System.getProperty("java.home") + "/bin/java");
            parameters.add("-Xmx1g");
            parameters.add("-classpath");
            parameters.add(System.getProperty("java.class.path"));
            parameters.add(mainClass);
            parameters.addAll(Arrays.asList(args));
            pb.environment().putAll(env);
            return pb.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
