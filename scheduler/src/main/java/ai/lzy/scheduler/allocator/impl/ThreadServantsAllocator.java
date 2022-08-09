package ai.lzy.scheduler.allocator.impl;

import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.scheduler.allocator.ServantMetaStorage;
import ai.lzy.scheduler.allocator.ServantsAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import com.google.common.net.HostAndPort;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
@Requires(property = "scheduler.thread-allocator.enabled", value = "true")
public class ThreadServantsAllocator implements ServantsAllocator {
    private static final Logger LOG = LogManager.getLogger(ThreadServantsAllocator.class);

    private final Method servantMain;
    private final AtomicInteger servantCounter = new AtomicInteger(0);
    private final ServiceConfig serverConfig;
    private final ConcurrentHashMap<String, ServantDescription> servantThreads = new ConcurrentHashMap<>();
    private final ServantMetaStorage metaStorage;

    @Singleton
    public ThreadServantsAllocator(ServiceConfig serverConfig, ServantMetaStorage metaStorage) {
        this.metaStorage = metaStorage;
        this.serverConfig = serverConfig;
        try {
            Class<?> servantClass;

            if (!serverConfig.threadAllocator().servantJarFile().isEmpty()) {
                final File servantJar = new File(serverConfig.threadAllocator().servantJarFile());
                final URLClassLoader classLoader = new URLClassLoader(new URL[]{servantJar.toURI().toURL()},
                    ClassLoader.getSystemClassLoader());
                servantClass = Class.forName(serverConfig.threadAllocator().servantClassName(), true, classLoader);
            } else {
                servantClass = Class.forName("ai.lzy.servant.BashApi");
            }

            servantMain = servantClass.getDeclaredMethod("execute", String[].class);
        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private void requestAllocation(String workflowId, String servantId, String servantToken) {
        int servantNumber = servantCounter.incrementAndGet();
        LOG.info("Allocating servant {}", servantId);
        int port = FreePortFinder.find(10000, 11000);

        @SuppressWarnings("CheckStyle")
        Thread task = new Thread("servant-" + servantId) {
            @Override
            public void run() {
                try {
                    servantMain.invoke(null, (Object) new String[]{
                        "--lzy-address", "http://" + serverConfig.schedulerAddress(),
                        "--lzy-whiteboard", "http://" + serverConfig.whiteboardAddress(),
                        "--lzy-mount", "/tmp/lzy" + servantNumber,
                        "--host", HostAndPort.fromString(serverConfig.schedulerAddress()).getHost(),
                        "--port", Integer.toString(port),
                        "--fs-port", Integer.toString(FreePortFinder.find(11000, 12000)),
                        "start",
                        "--workflowName", workflowId,
                        "--sid", servantId,
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
    public void allocate(String workflowId, String servantId, Provisioning provisioning) {
        // TODO(artolord) add token
        requestAllocation(workflowId, servantId, UUID.randomUUID().toString());
    }

    @Override
    public void destroy(String workflowId, String servantId) {
        metaStorage.clear(workflowId, servantId);
        if (!servantThreads.containsKey(servantId)) {
            return;
        }
        servantThreads.get(servantId).stop();
        servantThreads.remove(servantId);
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
                //noinspection removal
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
