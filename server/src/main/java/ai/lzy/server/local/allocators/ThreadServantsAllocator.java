package ai.lzy.server.local.allocators;

import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.server.Authenticator;
import ai.lzy.server.ServantsAllocatorBase;
import ai.lzy.server.configs.ServerConfig;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
@Requires(property = "server.threadAllocator.enabled", value = "true")
public class ThreadServantsAllocator extends ServantsAllocatorBase {

    private static final Logger LOG = LogManager.getLogger(ThreadServantsAllocator.class);

    private final Method servantMain;
    private final AtomicInteger servantCounter = new AtomicInteger(0);
    private final ServerConfig serverConfig;
    private final ConcurrentHashMap<String, Thread> servantThreads = new ConcurrentHashMap<>();

    public ThreadServantsAllocator(ServerConfig serverConfig, Authenticator authenticator) {
        super(authenticator, 1, 100);
        this.serverConfig = serverConfig;
        try {
            final File servantJar = new File(serverConfig.getThreadAllocator().getFilePath());
            final URLClassLoader classLoader = new URLClassLoader(new URL[]{servantJar.toURI().toURL()},
                ClassLoader.getSystemClassLoader());
            final Class<?> servantClass = Class.forName(serverConfig.getThreadAllocator().getServantClassName(),
                true, classLoader);
            servantMain = servantClass.getDeclaredMethod("execute", String[].class);

        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void requestAllocation(String sessionId, String servantId, String servantToken,
        Provisioning provisioning, String bucket) {
        int servantNumber = servantCounter.incrementAndGet();
        LOG.info("Allocating servant {}", servantId);

        @SuppressWarnings("CheckStyle")
        Thread task = new Thread("servant-" + servantId) {
            @Override
            public void run() {
                try {
                    servantMain.invoke(null, (Object) new String[]{
                        "--lzy-address", serverConfig.getServerUri().toString(),
                        "--lzy-whiteboard", serverConfig.getWhiteboardUri().toString(),
                        "--channel-manager", serverConfig.getChannelManagerUri().toString(),
                        "--lzy-mount", "/tmp/lzy" + servantNumber,
                        "--host", serverConfig.getServerUri().getHost(),
                        "--port", Integer.toString(FreePortFinder.find(10000, 11000)),
                        "--fs-port", Integer.toString(FreePortFinder.find(11000, 12000)),
                        "start",
                        "--bucket", bucket,
                        "--sid", servantId,
                        "--token", servantToken,
                    });
                } catch (IllegalAccessException | InvocationTargetException e) {
                    LOG.error(e);
                }
            }
        };
        task.start();
        servantThreads.put(servantId, task);
    }

    @Override
    protected void cleanup(ServantConnection s) {
        terminate(s);
    }

    @Override
    protected void terminate(ServantConnection connection) {
        if (!servantThreads.containsKey(connection.id())) {
            return;
        }
        LOG.info("Terminating servant: " + connection.id());
        //noinspection removal
        servantThreads.get(connection.id()).stop();
        servantThreads.remove(connection.id());
    }
}
