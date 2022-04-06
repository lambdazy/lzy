package ru.yandex.cloud.ml.platform.lzy.server.local.allocators;

import io.grpc.ManagedChannel;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Singleton;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.server.Authenticator;
import ru.yandex.cloud.ml.platform.lzy.server.ServantsAllocator;
import ru.yandex.cloud.ml.platform.lzy.server.configs.ServerConfig;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;


@Singleton
@Requires(property = "server.allocator.thread.enabled", value = "true")
public class ThreadServantsAllocator implements ServantsAllocator.Ex {
    private final Method servantMain;
    private final AtomicInteger servantCounter = new AtomicInteger(0);
    private final ServerConfig serverConfig;
    private final ConcurrentHashMap<String, CompletableFuture<ServantConnection>> servantIdsToFutures = new ConcurrentHashMap<>();
    private final HashMap<String, HashSet<String>> sessions = new HashMap<>();
    private final Authenticator authenticator;

    public ThreadServantsAllocator(ServerConfig serverConfig, Authenticator authenticator) {
        this.serverConfig = serverConfig;
        this.authenticator = authenticator;
        try {
            final File servantJar = new File(serverConfig.getThreadAllocator().getJarPath());
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
    public CompletableFuture<ServantConnection> allocate(
            String sessionId,
            Operations.Provisioning provisioning,
            Operations.EnvSpec env,
            String bucket
    ) {
        final String servantId = UUID.randomUUID().toString();
        final CompletableFuture<ServantConnection> future = new CompletableFuture<>();
        servantIdsToFutures.put(servantId, future);
        sessions.compute(sessionId, (k, v) -> {
            HashSet<String> set = (v == null ? new HashSet<>() : v);
            set.add(servantId);
            return set;
        });
        final String token = authenticator.registerServant(servantId);
        ForkJoinPool.commonPool().execute(() -> {
            try {
                servantMain.invoke(null, (Object) new String[]{
                    "start",
                    "--lzy-address", serverConfig.getServerUri(),
                    "--lzy-whiteboard", serverConfig.getWhiteboardUrl(),
                    "--lzy-mount", "/tmp/lzy" + servantCounter.incrementAndGet(),
                    "--host", URI.create(serverConfig.getServerUri()).getHost(),
                    "--internal-host", URI.create(serverConfig.getServerUri()).getHost(),
                    "--port", String.valueOf(10000 + servantCounter.get()),
                    "--bucket", bucket,
                    "--sid", servantId,
                    "--token", token,
                });
            } catch (IllegalAccessException | InvocationTargetException e) {
                future.completeExceptionally(e);
                throw new RuntimeException(e);
            }
        });
        return future;
    }

    @Override
    public void shutdownSession(String sessionId) {
        sessions.getOrDefault(sessionId, new HashSet<>())
            .forEach(t -> {
                final CompletableFuture<ServantConnection> future = servantIdsToFutures.get(t);
                if (future.isDone()) {
                    try {
                        LocalServantConnection connection = (LocalServantConnection) future.get();
                        connection.close();
                    } catch (InterruptedException | ExecutionException e) {
                        // Unreachable
                    }
                } else {
                    future.completeExceptionally(new RuntimeException("Session is closed before start of servant"));
                }
            });
        sessions.remove(sessionId);
    }

    @Override
    public void register(String servantId, URI servant) {
        ServantConnection connection = new LocalServantConnection(servant, servantId);
        servantIdsToFutures.get(servantId).complete(connection);
    }

    private static class LocalServantConnection implements ServantConnection {
        private final URI servantURI;
        private final LzyServantGrpc.LzyServantBlockingStub control;
        private final HashSet<Predicate<Servant.ServantProgress>> listeners = new HashSet<>();
        private final Lock lock = new ReentrantLock();
        private final ManagedChannel channel;
        private final String servantId;

        LocalServantConnection(URI servantURI, String servantId) {
            this.servantURI = servantURI;
            this.servantId = servantId;
            channel = ChannelBuilder.forAddress(servantURI.getHost(), servantURI.getPort())
                .usePlaintext()
                .enableRetry(LzyServantGrpc.SERVICE_NAME)
                .build();
            control = LzyServantGrpc.newBlockingStub(channel);
            Iterator<Servant.ServantProgress> iter = control.start(IAM.Empty.newBuilder().build());

            ForkJoinPool.commonPool().execute(() -> iter.forEachRemaining(progress -> {
                lock.lock();
                listeners.forEach(t -> {
                    if (!t.test(progress)) {
                        listeners.remove(t);
                    }
                });
                lock.unlock();
            }));
        }

        @Override
        public String id() {
            return servantId;
        }

        @Override
        public URI uri() {
            return servantURI;
        }

        @Override
        public LzyServantGrpc.LzyServantBlockingStub control() {
            return control;
        }

        @Override
        public void onProgress(Predicate<Servant.ServantProgress> tracker) {
            lock.lock();
            listeners.add(tracker);
            lock.unlock();
        }

        private void close() {
            channel.shutdownNow();
        }
    }
}
