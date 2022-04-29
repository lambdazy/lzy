package ru.yandex.cloud.ml.platform.lzy.server.local.allocators;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.model.utils.FreePortFinder;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocatorBase;
import ru.yandex.cloud.ml.platform.lzy.server.configs.ServerConfig;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


@Singleton
@Requires(property = "server.threadAllocator.enabled", value = "true")
public class ThreadServantsAllocator extends ServantsAllocatorBase {
    private static final Logger LOG = LogManager.getLogger(ThreadServantsAllocator.class);

    private final Method servantMain;
    private final AtomicInteger servantCounter = new AtomicInteger(0);
    private final ServerConfig serverConfig;
    private final ConcurrentHashMap<UUID, ServantDescription> servantThreads = new ConcurrentHashMap<>();

    public ThreadServantsAllocator(ServerConfig serverConfig, Authenticator authenticator) {
        super(authenticator, 1);
        this.serverConfig = serverConfig;
        try {
            final File servantJar = new File(serverConfig.getThreadAllocator().getFilePath());
            final URLClassLoader classLoader = new URLClassLoader(new URL[] {servantJar.toURI().toURL()},
                ClassLoader.getSystemClassLoader());
            final Class<?> servantClass = Class.forName(serverConfig.getThreadAllocator().getServantClassName(),
                true, classLoader);
            servantMain = servantClass.getDeclaredMethod("execute", String[].class);

        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void requestAllocation(UUID servantId, String servantToken,
                                     Provisioning provisioning, String bucket) {
        int servantNumber = servantCounter.incrementAndGet();
        LOG.info("Allocating servant {}", servantId);

        @SuppressWarnings("CheckStyle")
        Thread task = new Thread("servant-" + servantId.toString()) {
            @Override
            public void run() {
                try {
                    servantMain.invoke(null, (Object) new String[]{
                        "--lzy-address", serverConfig.getServerUri().toString(),
                        "--lzy-whiteboard", serverConfig.getWhiteboardUri().toString(),
                        "--lzy-mount", "/tmp/lzy" + servantNumber,
                        "--host", serverConfig.getServerUri().getHost(),
                        "--port", Integer.toString(FreePortFinder.find(10000, 11000)),
                        "--fs-port", Integer.toString(FreePortFinder.find(11000, 12000)),
                        "start",
                        "--bucket", bucket,
                        "--sid", servantId.toString(),
                        "--token", servantToken,
                    });
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOG.error(e);
                }
            }
        };

        ServantDescription description = new ServantDescription(
            "servant-" + servantId,
            "/tmp/lzy" + servantNumber,
            task
        );
        task.start();
        servantThreads.put(servantId, description);
    }

    @Override
    protected void cleanup(ServantConnection s) {
        if (!servantThreads.containsKey(s.id())) {
            return;
        }
        servantThreads.get(s.id()).stop();
        servantThreads.remove(s.id());
    }

    @Override
    protected void terminate(ServantConnection connection) {
        if (!servantThreads.containsKey(connection.id())) {
            return;
        }
        servantThreads.get(connection.id()).stop();
        servantThreads.remove(connection.id());
    }

    private static class ServantDescription {
        private final String name;
        private final String mountPoint;
        private final Thread thread;

        public ServantDescription(String name, String mountPoint, Thread thread) {
            this.name = name;
            this.mountPoint = mountPoint;
            this.thread = thread;
        }

        private void stop() {
            try {
                thread.stop();
            } finally {
                try {
                    final Process run;
                    if (SystemUtils.IS_OS_MAC) {
                        run = Runtime.getRuntime().exec("umount -f " + mountPoint);
                    } else {
                        run = Runtime.getRuntime().exec("umount " + mountPoint);
                    }
                    String out = IOUtils.toString(run.getInputStream(), StandardCharsets.UTF_8);
                    String err = IOUtils.toString(run.getErrorStream(), StandardCharsets.UTF_8);
                    int rc = run.waitFor();
                    LOG.info("Unmounting servant fs. RC: {}\n STDOUT: {}\n STDERR: {}", rc, out, err);
                } catch (IOException | InterruptedException e) {
                    LOG.error(e);
                }
            }
        }
    }
}
