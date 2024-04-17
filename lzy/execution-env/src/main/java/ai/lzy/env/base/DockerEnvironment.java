package ai.lzy.env.base;

import ai.lzy.env.logs.LogStream;
import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.async.ResultCallback;
import com.github.dockerjava.api.async.ResultCallbackTemplate;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ExecCreateCmd;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.command.PullImageResultCallback;
import com.github.dockerjava.api.exception.DockerClientException;
import com.github.dockerjava.api.exception.DockerException;
import com.github.dockerjava.api.exception.NotFoundException;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DockerClientImpl;
import com.github.dockerjava.httpclient5.ApacheDockerHttpClient;
import io.github.resilience4j.core.IntervalFunction;
import io.github.resilience4j.retry.Retry;
import io.github.resilience4j.retry.RetryConfig;
import jakarta.annotation.Nullable;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DockerEnvironment extends BaseEnvironment {

    private static final Logger LOG = LogManager.getLogger(DockerEnvironment.class);
    private static final long GB_AS_BYTES = 1073741824;
    private static final String NO_MATCHING_MANIFEST_ERROR = "no matching manifest";
    private static final String NOT_MATCH_PLATFORM_ERROR = "was found but does not match the specified platform";

    @Nullable
    public String containerId = null;
    private final DockerEnvDescription config;
    private final DockerClient client;
    private final Retry retry;

    public DockerEnvironment(DockerEnvDescription config) {
        this(
            config,
            RetryConfig.custom()
                .maxAttempts(3)
                .intervalFunction(IntervalFunction.ofExponentialBackoff(1000))
                .retryExceptions(DockerException.class, DockerClientException.class)
                .build()
        );
    }

    public DockerEnvironment(DockerEnvDescription config, RetryConfig retryConfig) {
        this.config = config;
        this.client = DockerClientImpl.getInstance(
            config.dockerClientConfig(),
            new ApacheDockerHttpClient.Builder()
                .dockerHost(config.dockerClientConfig().getDockerHost())
                .build());
        retry = Retry.of("docker-client-retry", retryConfig);
    }

    @Override
    public void install(LogStream outStream, LogStream errStream) throws InstallationException {
        if (containerId != null) {
            outStream.log("Using already running container from cache");
            LOG.info("Using already running container from cache; containerId: {}", containerId);
            return;
        }

        var image = config.image();

        pullImageIfNeeded(image, outStream, errStream);

        var containerId = createContainer(image, outStream, errStream).getId();

        startContainer(containerId, image, outStream, errStream);

        this.containerId = containerId;
    }

    @Override
    public LzyProcess runProcess(String[] command, @Nullable String[] envp, @Nullable String workingDir) {
        assert containerId != null;

        final int bufferSize = 4096;
        final PipedInputStream stdoutPipe = new PipedInputStream(bufferSize);
        final PipedInputStream stderrPipe = new PipedInputStream(bufferSize);
        final PipedOutputStream stdout;
        final PipedOutputStream stderr;
        try {
            stdout = new PipedOutputStream(stdoutPipe);
            stderr = new PipedOutputStream(stderrPipe);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        LOG.info("Creating cmd {}", String.join(" ", command));
        final ExecCreateCmd execCmd = client.execCreateCmd(containerId)
            .withCmd(command)
            .withUser(config.user())
            .withAttachStdout(true)
            .withAttachStderr(true);

        if (workingDir != null) {
            execCmd.withWorkingDir(workingDir);
        }

        if (envp != null && envp.length > 0) {
            execCmd.withEnv(List.of(envp));
        }
        final ExecCreateCmdResponse exec = retry.executeSupplier(execCmd::exec);
        LOG.info("Executing cmd {}", String.join(" ", command));

        var feature = new CompletableFuture<>();

        var startCmd = retry.executeSupplier(() ->
            client.execStartCmd(exec.getId())
                .exec(new ResultCallbackTemplate<>() {
                    @Override
                    public void onComplete() {
                        LOG.info("Closing stdout, stderr of cmd {}", String.join(" ", command));
                        try {
                            stdout.close();
                            stderr.close();
                        } catch (IOException e) {
                            LOG.error("Cannot close stderr/stdout slots", e);
                        } catch (Exception e) {
                            LOG.error("Error while completing docker env process: ", e);
                        } finally {
                            feature.complete(null);
                        }
                    }

                    @Override
                    public void onNext(Frame item) {
                        switch (item.getStreamType()) {
                            case STDOUT -> {
                                try {
                                    stdout.write(item.getPayload());
                                    stdout.flush();
                                } catch (IOException e) {
                                    LOG.error("Error while write into stdout log", e);
                                }
                            }
                            case STDERR -> {
                                try {
                                    stderr.write(item.getPayload());
                                    stderr.flush();
                                } catch (IOException e) {
                                    LOG.error("Error while write into stderr log", e);
                                }
                            }
                            default -> LOG.warn("Drop frame of {} bytes from unknown stream '{}'",
                                item.getPayload().length, item.getStreamType());
                        }
                    }
                }));

        return new LzyProcess() {
            @Override
            public OutputStream in() {
                return OutputStream.nullOutputStream();
            }

            @Override
            public InputStream out() {
                return stdoutPipe;
            }

            @Override
            public InputStream err() {
                return stderrPipe;
            }

            @Override
            public int waitFor() throws InterruptedException, OutOfMemoryException {
                try {
                    feature.get();
                    var rc = Math.toIntExact(
                        retry.executeSupplier(() -> client.inspectExecCmd(exec.getId()).exec())
                            .getExitCodeLong());

                    if (rc == 0) {
                        return 0;
                    }

                    if (isOomKilled()) {
                        throw new OutOfMemoryException();
                    }

                    return rc;
                } catch (InterruptedException e) {
                    try {
                        startCmd.close();
                    } catch (IOException ex) {
                        LOG.error("Error while closing cmd: ", ex);
                    }
                    throw e;
                } catch (ExecutionException e) {
                    // ignored
                    return 1;
                }
            }

            @Override
            public void signal(int sigValue) {
                if (containerId != null) {
                    retry.executeSupplier(() -> client.killContainerCmd(containerId)
                        .withSignal(String.valueOf(sigValue))
                        .exec());
                }
            }
        };
    }

    @Override
    public void close() throws Exception {
        if (containerId != null) {
            retry.executeSupplier(() -> client.killContainerCmd(containerId).exec());
            retry.executeSupplier(() -> client.pruneCmd(PruneType.CONTAINERS).exec());
            containerId = null;
        }
    }

    void pullImageIfNeeded(String image, LogStream out, LogStream err) throws InstallationException {
        try {
            var resp = client.inspectImageCmd(image).exec();
            var msg = "Image '%s' exists".formatted(image);
            LOG.info(msg);
            out.log(msg);
            validateImagePlatform(resp, err);
            return;
        } catch (NotFoundException ignored) {
            // ignored
        }

        var msg = "Image '%s' not found in cached images, try to pull it...".formatted(image);
        LOG.info(msg);
        out.log(msg);

        try (var x = withLoggerLevel(PullImageResultCallback.class, Level.INFO)) {
            var attempt = new AtomicInteger(0);

            try (var resp = retry.executeCallable(
                () -> {
                    LOG.info("Pull image '{}', attempt #{}", image, attempt.incrementAndGet());
                    if (config.allowedPlatforms().isEmpty()) {
                        return pullWithPlatform(image, null);
                    }

                    for (String platform : config.allowedPlatforms()) {
                        try {
                            return pullWithPlatform(image, platform);
                        } catch (DockerClientException e) {
                            var emsg = e.getMessage();
                            if (emsg.contains(NO_MATCHING_MANIFEST_ERROR) || emsg.contains(NOT_MATCH_PLATFORM_ERROR)) {
                                var str = "Image '%s' for platform '%s' not found: %s".formatted(image, platform, emsg);
                                LOG.info(str);
                                out.log(str);
                            } else {
                                throw e;
                            }
                        }
                    }
                    return null;
                }))
            {
                if (resp == null) {
                    var str = "Image '%s' for platforms [%s] not found"
                        .formatted(image, String.join(", ", config.allowedPlatforms()));
                    LOG.error(str);
                    err.log(str);
                    throw new InstallationException(str);
                }
            }

            msg = "Image '%s' pull done".formatted(image);
            LOG.info(msg);
            out.log(msg);
        } catch (InstallationException e) {
            throw e;
        } catch (InterruptedException e) {
            LOG.error("Image {} pull was interrupted", image);
            err.log("Image pull was interrupted");
            throw new InstallationException("Image pull was interrupted");
        } catch (DockerException e) {
            LOG.error("Image {} pull failed: {}", image, e.getMessage(), e);
            err.log("Image pull failed with error " + e.getMessage());
            throw new InstallationException("Image pull failed with error " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Image {} pull failed: {}", image, e.getMessage(), e);
            err.log("Image pull filed");
            throw new InstallationException("Image pull failed");
        }
    }

    private ResultCallback.Adapter<PullResponseItem> pullWithPlatform(String image, @Nullable String platform)
        throws InterruptedException
    {
        var pullingImage = client.pullImageCmd(image);
        if (platform != null) {
            pullingImage = pullingImage.withPlatform(platform);
        }
        return pullingImage.exec(new PullImageResultCallback()).awaitCompletion();
    }

    private void validateImagePlatform(InspectImageResponse inspect, LogStream err)
        throws InstallationException
    {
        if (config.allowedPlatforms().isEmpty()) {
            return;
        }

        var platform = inspect.getOs() + "/" + inspect.getArch();
        if (!config.allowedPlatforms().contains(platform)) {
            var msg = "Image '%s' with platform '%s' is not in the allowed platforms [%s]"
                .formatted(config.image(), platform, String.join(", ", config.allowedPlatforms()));
            LOG.error(msg);
            err.log(msg);

            throw new InstallationException(msg);
        }
    }

    private CreateContainerResponse createContainer(String image, LogStream outStream, LogStream errStream)
        throws InstallationException
    {
        LOG.info("Creating container from image {} ...", image);
        outStream.log("Creating container from image %s ...".formatted(image));

        final List<Mount> dockerMounts = new ArrayList<>();
        config.mounts().forEach(m -> {
            var mount = new Mount().withType(MountType.BIND).withSource(m.source()).withTarget(m.target());
            if (m.isRshared()) {
                mount.withBindOptions(new BindOptions().withPropagation(BindPropagation.R_SHARED));
            }
            dockerMounts.add(mount);
        });

        final HostConfig hostConfig = new HostConfig();
        hostConfig.withMounts(dockerMounts);

        // --gpus all --ipc=host --shm-size=1G
        if (config.needGpu()) {
            hostConfig.withDeviceRequests(
                List.of(new DeviceRequest()
                    .withDriver("nvidia")
                    .withCapabilities(List.of(List.of("gpu")))))
                .withIpcMode("host")
                .withShmSize(GB_AS_BYTES);
        }
        if (config.networkMode() != null) {
            hostConfig.withNetworkMode(config.networkMode());
        }
        if (config.memLimitMb() != null) {
            hostConfig.withMemory(config.memLimitMb() * 1024 * 1024);
        }

        try {
            var attempt = new AtomicInteger(0);

            var container = retry.executeSupplier(() -> {
                LOG.info("Creating container {}... (attempt {}); image: {}, config: {}",
                    config.name(), attempt.incrementAndGet(), image, config);
                outStream.log("Creating container... (attempt %d)".formatted(attempt.get()));

                return client.createContainerCmd(image)
                    .withName(config.name())
                    .withHostConfig(hostConfig)
                    .withAttachStdout(true)
                    .withAttachStderr(true)
                    .withTty(true)
                    .withUser(config.user())
                    .withEnv(config.envVars())
                    .withEntrypoint("/bin/sh")
                    .exec();
            });

            outStream.log("Container %s from image %s created".formatted(config.name(), image));
            LOG.info("Container {} created; containerId: {}, image: {}", config.name(), container.getId(), image);

            return container;
        } catch (DockerException e) {
            LOG.error("Create container failed with error: {}", e.getMessage(), e);
            errStream.log("Container creation failed with error " + e.getMessage());
            throw new InstallationException("Container creation failed with error " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Create container failed with error: {}", e.getMessage(), e);
            errStream.log("Container creation failed");
            throw new InstallationException("Container creation failed");
        }
    }

    private void startContainer(String containerId, String image, LogStream outStream, LogStream errStream)
        throws InstallationException
    {
        outStream.log("Start environment container %s ...".formatted(config.name()));

        try {
            var attempt = new AtomicInteger(0);
            retry.executeSupplier(() -> {
                LOG.info("Starting env container {}... (attempt {}); containerId: {}, image: {}",
                    config.name(), attempt.incrementAndGet(), containerId, image);
                return client.startContainerCmd(containerId).exec();
            });

            outStream.log("Environment container %s started".formatted(config.name()));
            LOG.info("Starting env container {} done; containerId: {}, image: {}", config.name(), containerId, image);
        } catch (DockerException e) {
            LOG.error("Cannot start container {}: {}", containerId, e.getMessage(), e);
            errStream.log("Environment container start failed with error " + e.getMessage());
            throw new InstallationException(
                "Environment container start failed with error " + e.getMessage());
        } catch (Exception e) {
            LOG.error("Cannot start container {}: {}", containerId, e.getMessage(), e);
            errStream.log("Environment container start failed");
            throw new InstallationException("Environment container start failed");
        }
    }

    private boolean isOomKilled() {
        if (containerId == null) {
            return false;
        }

        // container OOM
        try {
            var killed = client.inspectContainerCmd(containerId)
                .exec()
                .getState()
                .getOOMKilled();
            if (killed != null && killed) {
                return true;
            }
        } catch (DockerException e) {
            LOG.error("Inspect container {} failed: {}", containerId, e.getMessage(), e);
        }

        // workload OOM
        try {
            var killed = new boolean[]{false};
            var latch = new CountDownLatch(1);

            client.eventsCmd()
                .withContainerFilter(containerId)
                .withEventFilter("oom")
                .withSince("" + Instant.now().minusSeconds(60).getEpochSecond())
                .exec(new ResultCallback.Adapter<Event>() {
                    @Override
                    public void onNext(Event event) {
                        killed[0] = true;
                        latch.countDown();
                    }

                    @Override
                    public void onComplete() {
                        latch.countDown();
                    }
                });

            latch.await(10, TimeUnit.SECONDS);
            return killed[0];
        } catch (DockerException e) {
            LOG.error("Docker events for {} failed: {}", containerId, e.getMessage(), e);
        } catch (InterruptedException ignored) {
            return false;
        }

        return false;
    }

    private static AutoCloseable withLoggerLevel(Class<?> loggerClass, Level level) {
        var logger = LogManager.getContext().getLogger(loggerClass);

        if (logger instanceof org.apache.logging.log4j.core.Logger log) {
            var prev = log.getLevel();
            log.setLevel(level);
            return () -> log.setLevel(prev);
        }

        return () -> {};
    }
}
