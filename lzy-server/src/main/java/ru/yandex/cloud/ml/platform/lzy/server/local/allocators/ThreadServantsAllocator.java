package ru.yandex.cloud.ml.platform.lzy.server.local.allocators;

import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocatorBase;
import ru.yandex.cloud.ml.platform.lzy.server.configs.ServerConfig;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


@Singleton
@Requires(property = "server.threadAllocator.enabled", value = "true")
public class ThreadServantsAllocator extends ServantsAllocatorBase {
    private final Method servantMain;
    private final AtomicInteger servantCounter = new AtomicInteger(0);
    private final ServerConfig serverConfig;
    private final ConcurrentHashMap<UUID, Thread> servantThreads = new ConcurrentHashMap<>();

    public ThreadServantsAllocator(ServerConfig serverConfig, Authenticator authenticator) {
        super(authenticator, 10);
        this.serverConfig = serverConfig;
        try {
            final File servantJar = new File(serverConfig.getThreadAllocator().getFilePath());
            final URLClassLoader classLoader = new URLClassLoader(new URL[]{servantJar.toURI().toURL()},
                    ClassLoader.getSystemClassLoader());
            final Class<?> servantClass = Class.forName(serverConfig.getThreadAllocator().getServantClassName(),
                    true, classLoader);
            servantMain = servantClass.getDeclaredMethod("main", String[].class);

        } catch (MalformedURLException | ClassNotFoundException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void requestAllocation(UUID servantId, String servantToken,
                                     Provisioning provisioning, Env env, String bucket) {
        @SuppressWarnings("CheckStyle")
        Thread task = new Thread("servant-" + servantId.toString()){
            @Override
            public void run() {
                try {
                    servantMain.invoke(null, (Object) new String[]{
                            "--lzy-address", serverConfig.getServerUri(),
                            "--lzy-whiteboard", serverConfig.getWhiteboardUrl(),
                            "--lzy-mount", "/tmp/lzy" + servantCounter.incrementAndGet(),
                            "--host", URI.create(serverConfig.getServerUri()).getHost(),
                            "--internal-host", URI.create(serverConfig.getServerUri()).getHost(),
                            "--port", String.valueOf(10000 + servantCounter.get()),
                            "start",
                            "--bucket", bucket,
                            "--sid", servantId.toString(),
                            "--token", servantToken,
                    });
                } catch (IllegalAccessException | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        task.start();
        servantThreads.put(servantId, task);
    }

    @Override
    protected void cleanup(ServantConnection s) {
        servantThreads.get(s.id()).stop();
        servantThreads.remove(s.id());
    }

    @Override
    protected void terminate(ServantConnection connection) {
        servantThreads.get(connection.id()).stop();
        servantThreads.remove(connection.id());
    }
}
