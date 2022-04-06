package ru.yandex.cloud.ml.platform.lzy.server;

import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Predicate;

import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Operations.EnvSpec;
import yandex.cloud.priv.datasphere.v2.lzy.Operations.Provisioning;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public abstract class ServantsAllocatorBase extends TimerTask implements ServantsAllocator.Ex {
    private static final Logger LOG = LogManager.getLogger(ServantsAllocatorBase.class);
    public static final int PERIOD = 1000;
    public static final int GRACEFUL_SHUTDOWN_PERIOD_SEC = 10;
    @SuppressWarnings("FieldCanBeLocal")
    private final Timer ttl;
    private final Map<String, List<ServantConnection>> sessionServants = Collections.synchronizedMap(new HashMap<>());
    private final Map<String, CompletableFuture<ServantConnection>> requests = new HashMap<>();
    private final Map<ServantConnection, Instant> spareServants = new HashMap<>();
    private final Map<ServantConnection, Instant> shuttingDown = new HashMap<>();

    private final Authenticator auth;
    private final int waitBeforeShutdown;

    public ServantsAllocatorBase(Authenticator auth, int waitBeforeShutdownInSec) {
        this.auth = auth;
        this.waitBeforeShutdown = waitBeforeShutdownInSec;
        ttl = new Timer("Allocator TTL", true);
        ttl.scheduleAtFixedRate(this, PERIOD, PERIOD);
    }

    protected abstract void requestAllocation(String servantId, String servantToken,
                                              Provisioning provisioning, EnvSpec env,
                                              String bucket);
    protected boolean mutate(ServantConnection connection, Provisioning provisioning, EnvSpec env) {
        return false;
    }

    protected abstract void cleanup(ServantConnection s);
    protected abstract void terminate(ServantConnection connection);

    @Override
    public synchronized CompletableFuture<ServantConnection> allocate(
        String sessionId,
        Provisioning provisioning,
        EnvSpec env,
        String bucket
    ) {
        final CompletableFuture<ServantConnection> requestResult = new CompletableFuture<>();
        final List<ServantConnection> servants = sessionServants
            .computeIfAbsent(sessionId, s -> new ArrayList<>());
        final ServantConnection spareConnection = servants.stream()
            .filter(spareServants::containsKey)
            .filter(connection -> mutate(connection, provisioning, env))
            .findFirst().orElse(null);
        if (spareConnection != null) {
            spareServants.remove(spareConnection);
            requestResult.complete(spareConnection);
        } else {
            final String servantId = UUID.randomUUID().toString();
            requests.put(servantId, requestResult);
            ForkJoinPool.commonPool().execute(() -> {
                final String servantToken = auth.registerServant(servantId);
                requestAllocation(servantId, servantToken, provisioning, env, bucket);
            });
        }
        return requestResult;
    }

    @Override
    public synchronized void register(String servantId, URI servantUri) {
        final ManagedChannel channel = ChannelBuilder.forAddress(servantUri.getHost(), servantUri.getPort())
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();
        final LzyServantGrpc.LzyServantBlockingStub blockingStub = LzyServantGrpc.newBlockingStub(channel);
        final Iterator<Servant.ServantProgress> progressIterator = blockingStub.start(IAM.Empty.newBuilder().build());

        if (!requests.containsKey(servantId)) {
            LOG.error("Astray servant found: " + servantId);
            //noinspection ResultOfMethodCallIgnored
            blockingStub.stop(IAM.Empty.newBuilder().build());
            channel.shutdownNow();
            return;
        }
        final CompletableFuture<ServantConnection> request = requests.remove(servantId);
        final ServantConnectionImpl connection = new ServantConnectionImpl(servantId, servantUri, blockingStub);
        request.complete(connection);
        progressIterator.forEachRemaining(progress -> {
            if (progress.hasExit()) { // graceful shutdown
                shuttingDown.remove(connection);
                cleanup(connection);
            } else if (progress.hasExecuteStop()) {
                spareServants.put(connection, Instant.now().plus(waitBeforeShutdown, ChronoUnit.SECONDS));
            }
            connection.progress(progress);
        });
    }

    @Override
    public synchronized void shutdownSession(String sessionId) {
        sessionServants.getOrDefault(sessionId, List.of()).forEach(c -> spareServants.put(c, Instant.now()));
    }

    public synchronized void run() {
        final Instant now = Instant.now();
        Set.copyOf(spareServants.keySet()).stream().filter(s -> spareServants.get(s).isBefore(now))
            .peek(spareServants::remove).forEach(s -> {
                shuttingDown.put(s, Instant.now().plus(GRACEFUL_SHUTDOWN_PERIOD_SEC, ChronoUnit.SECONDS));
                try {
                    //noinspection ResultOfMethodCallIgnored
                    s.control().stop(IAM.Empty.newBuilder().build());
                } catch (Exception e) {
                    terminate(s);
                    shuttingDown.remove(s);
                }
            });
        Set.copyOf(shuttingDown.keySet()).stream().filter(s -> spareServants.get(s).isBefore(now))
            .peek(shuttingDown::remove)
            .forEach(this::terminate);
    }

    private static class ServantConnectionImpl implements ServantConnection {
        private final String servantId;
        private final URI servantUri;
        private final LzyServantGrpc.LzyServantBlockingStub control;
        private final List<Predicate<Servant.ServantProgress>> trackers;

        protected ServantConnectionImpl(String servantId, URI servantUri, LzyServantGrpc.LzyServantBlockingStub c) {
            this.servantId = servantId;
            this.servantUri = servantUri;
            this.control = c;
            trackers = Collections.synchronizedList(new ArrayList<>());
        }

        public void progress(Servant.ServantProgress progress) {
            List.copyOf(trackers).stream().filter(t -> t.test(progress)).forEach(trackers::remove);
        }

        @Override
        public void onProgress(Predicate<Servant.ServantProgress> tracker) {
            trackers.add(tracker);
        }

        @Override
        public String id() {
            return servantId;
        }

        @Override
        public URI uri() {
            return servantUri;
        }

        @Override
        public LzyServantGrpc.LzyServantBlockingStub control() {
            return control;
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ServantConnectionImpl that = (ServantConnectionImpl) o;
            return servantUri.equals(that.servantUri);
        }

        @Override
        public final int hashCode() {
            return Objects.hash(servantUri);
        }
    }
}
