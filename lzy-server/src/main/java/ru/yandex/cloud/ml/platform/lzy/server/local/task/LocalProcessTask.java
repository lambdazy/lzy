package ru.yandex.cloud.ml.platform.lzy.server.local.task;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.Zygote;
import ru.yandex.cloud.ml.platform.lzy.server.ChannelsManager;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;

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

import ru.yandex.cloud.ml.platform.lzy.server.configs.TasksConfig;
import ru.yandex.qe.s3.util.Environment;

public class LocalProcessTask extends LocalTask {
    private static final Logger LOG = LogManager.getLogger(LocalProcessTask.class);
    private final TasksConfig.LocalProcessTaskConfig config;

    public LocalProcessTask(
        String owner,
        UUID tid,
        Zygote workload,
        Map<Slot, String> assignments,
        SnapshotMeta meta,
        ChannelsManager channels,
        URI serverURI,
        TasksConfig.LocalProcessTaskConfig config
    ) {
        super(owner, tid, workload, assignments, meta, channels, serverURI);
        this.config = config;
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
                config.servantJarPath(), taskDir,
                new String[]{
                    "-z", serverHost + ":" + serverPort,
                    "--host", servantHost,
                    "-p", String.valueOf(servantPort),
                    "-m", taskDir.getAbsolutePath()
                },
                Map.ofEntries(
                    Map.entry("LZYTASK", tid.toString()),
                    Map.entry("LZYTOKEN", token),
                    Map.entry("LZY_MOUNT", taskDir.getAbsolutePath()),
                    Map.entry("LZYWHITEBOARD", Environment.getLzyWhiteboard()),
                    Map.entry("BUCKET_NAME", owner),
                    Map.entry("ACCESS_KEY", Environment.getAccessKey()),
                    Map.entry("SECRET_KEY", Environment.getSecretKey()),
                    Map.entry("REGION", Environment.getRegion()),
                    Map.entry("SERVICE_ENDPOINT", Environment.getServiceEndpoint()),
                    Map.entry("PATH_STYLE_ACCESS_ENABLED", Environment.getPathStyleAccessEnabled()),
                    Map.entry("LOGS_PATH", "/tmp/logs")
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
        final String pathToJar,
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
            parameters.add("-jar");
            parameters.add(new File(pathToJar).getAbsolutePath());
            parameters.addAll(Arrays.asList(args));
            pb.environment().putAll(env);
            return pb.start();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
