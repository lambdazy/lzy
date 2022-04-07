package ru.yandex.cloud.ml.platform.lzy.server;

import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Env;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public abstract class ServantsAllocatorBase extends TimerTask implements ServantsAllocator.Ex {
    private static final Logger LOG = LogManager.getLogger(ServantsAllocatorBase.class);
    public static final int PERIOD = 1000;
    public static final int GRACEFUL_SHUTDOWN_PERIOD_SEC = 10;
    public static final ThreadGroup SERVANT_CONNECTIONS_TG = new ThreadGroup("Servant connections");
    @SuppressWarnings("FieldCanBeLocal")
    private final Timer ttl;
    private final Map<UUID, CompletableFuture<ServantConnection>> requests = new HashMap<>();
    private final Map<ServantConnection, Instant> spareServants = new HashMap<>();
    private final Map<ServantConnection, Instant> shuttingDown = new HashMap<>();

    private final Map<String, Set<SessionImpl>> userToSessions = new ConcurrentHashMap<>();
    private final Map<UUID, SessionImpl> servant2sessions = new ConcurrentHashMap<>();
    private final Map<UUID, SessionImpl> sessionsById = new ConcurrentHashMap<>();

    private final Map<String, Session> userSessions = new ConcurrentHashMap<>();


    private final Authenticator auth;
    private final int waitBeforeShutdown;

    public ServantsAllocatorBase(Authenticator auth, int waitBeforeShutdownInSec) {
        this.auth = auth;
        this.waitBeforeShutdown = waitBeforeShutdownInSec;
        ttl = new Timer("Allocator TTL", true);
        ttl.scheduleAtFixedRate(this, PERIOD, PERIOD);
    }

    protected abstract void requestAllocation(UUID servantId, String servantToken,
                                              Provisioning provisioning, Env env,
                                              String bucket);
    @SuppressWarnings("unused")
    protected boolean mutate(ServantConnection connection, Provisioning provisioning, Env env) {
        return false;
    }

    protected abstract void cleanup(ServantConnection s);
    protected abstract void terminate(ServantConnection connection);


    @Override
    public synchronized CompletableFuture<ServantConnection> allocate(
        UUID sessionId, Provisioning provisioning, Env env
    ) {
        final SessionImpl session = sessionsById.get(sessionId);
        if (session == null)
            throw new IllegalArgumentException("Session " + sessionId + " not found");
        final CompletableFuture<ServantConnection> requestResult = new CompletableFuture<>();
        final ServantConnection spareConnection = session.servants.stream()
            .filter(spareServants::containsKey)
            .filter(connection -> mutate(connection, provisioning, env))
            .findFirst().orElse(null);
        if (spareConnection != null) {
            spareServants.remove(spareConnection);
            requestResult.complete(spareConnection);
        } else {
            final UUID servantId = UUID.randomUUID();
            servant2sessions.put(servantId, session);
            requests.put(servantId, requestResult);
            ForkJoinPool.commonPool().execute(() -> {
                final String servantToken = auth.registerServant(servantId);
                try {
                    requestAllocation(servantId, servantToken, provisioning, env, session.bucket());
                } catch (Exception e) {
                    requestResult.completeExceptionally(e);
                }
            });
        }
        return requestResult;
    }

    @Override
    public synchronized void register(UUID servantId, URI servantUri) {
        final ManagedChannel channel = ChannelBuilder.forAddress(servantUri.getHost(), servantUri.getPort())
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();
        final LzyServantGrpc.LzyServantBlockingStub blockingStub = LzyServantGrpc.newBlockingStub(channel);

        if (!requests.containsKey(servantId)) {
            LOG.error("Astray servant found: " + servantId);
            //noinspection ResultOfMethodCallIgnored
            blockingStub.stop(IAM.Empty.newBuilder().build());
            channel.shutdownNow();
            return;
        }
        final CompletableFuture<ServantConnection> request = requests.remove(servantId);
        final ServantConnectionImpl connection = new ServantConnectionImpl(servantId, servantUri, blockingStub);
        final Thread connectionThread = new Thread(SERVANT_CONNECTIONS_TG, () -> {
            final Iterator<Servant.ServantProgress> progressIterator = blockingStub.start(IAM.Empty.newBuilder().build());
            progressIterator.forEachRemaining(progress -> {
                if (progress.hasExit()) { // graceful shutdown
                    shuttingDown.remove(connection);
                    cleanup(connection);
                } else if (progress.hasExecuteStop()) {
                    spareServants.put(connection, Instant.now().plus(waitBeforeShutdown, ChronoUnit.SECONDS));
                }
                connection.progress(progress);
            });
        }, "connection-to-" + servantId);
        connection.connectionThread = connectionThread;
        servant2sessions.get(servantId).servants.add(connection);
        request.complete(connection);
        connectionThread.start();
        connectionThread.setDaemon(true);

    }

    @Override
    public synchronized void deleteSession(UUID sessionId) {
        final SessionImpl session = sessionsById.remove(sessionId);
        if (session != null) {
            userToSessions.getOrDefault(session.owner(), Set.of()).remove(session);
            session.servants.forEach(c -> spareServants.put(c, Instant.now()));
        }
    }


    @Override
    public Session registerSession(String userId, UUID sessionId, String bucket) {
        final SessionImpl session = new SessionImpl(sessionId, userId, bucket);
        userToSessions.computeIfAbsent(userId, u -> new HashSet<>()).add(session);
        sessionsById.put(sessionId, session);
        return session;
    }

    @Override
    public Session get(UUID sessionId) {
        return sessionsById.get(sessionId);
    }

    @Override
    public Stream<Session> sessions(String userId) {
        return userToSessions.get(userId).stream().map(s -> s);
    }

    @Override
    public Session byServant(String servantId) {
        return servant2sessions.get(servantId);
    }


    @Override
    public Session userSession(String user) {
        return userSessions.computeIfAbsent(user, u -> registerSession(u, UUID.randomUUID(), auth.bucketForUser(u)));
    }

    private final Executor executor = new ThreadPoolExecutor(1, 5, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public synchronized void run() {
        final Instant now = Instant.now();
        final List<ServantConnection> tasksToShutdown = Set.copyOf(spareServants.keySet()).stream()
            .filter(s -> spareServants.get(s).isBefore(now))
            .peek(spareServants::remove).collect(Collectors.toList());
        final List<ServantConnection> tasksToForceStop = Set.copyOf(shuttingDown.keySet()).stream()
            .filter(s -> spareServants.get(s).isBefore(now))
            .peek(shuttingDown::remove)
            .collect(Collectors.toList());
        executor.execute(() -> {
            tasksToShutdown.forEach(s -> {
                final SessionImpl session = servant2sessions.remove(s.id());
                session.servants.remove(s);
                shuttingDown.put(s, Instant.now().plus(GRACEFUL_SHUTDOWN_PERIOD_SEC, ChronoUnit.SECONDS));
                try {
                    //noinspection ResultOfMethodCallIgnored
                    s.control().stop(IAM.Empty.newBuilder().build());
                } catch (Exception e) {
                    terminate(s);
                    shuttingDown.remove(s);
                }
            });
            tasksToForceStop.forEach(this::terminate);
        });
    }

    private static class ServantConnectionImpl implements ServantConnection {
        private final UUID servantId;
        private final URI servantUri;
        private Thread connectionThread;
        private final LzyServantGrpc.LzyServantBlockingStub control;
        private final List<Predicate<Servant.ServantProgress>> trackers;

        protected ServantConnectionImpl(UUID servantId, URI servantUri, LzyServantGrpc.LzyServantBlockingStub c) {
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
        public UUID id() {
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

    private static class SessionImpl implements SessionManager.Session {
        private final UUID id;
        private final String user;
        private final List<ServantConnection> servants = new ArrayList<>();
        private final String bucket;

        private SessionImpl(UUID id, String user, String bucket) {
            this.id = id;
            this.user = user;
            this.bucket = bucket;
        }

        @Override
        public UUID id() {
            return id;
        }

        @Override
        public String owner() {
            return user;
        }

        @Override
        public UUID[] servants() {
            return servants.stream().map(ServantConnection::id).toArray(UUID[]::new);
        }

        @Override
        public String bucket() {
            return bucket;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SessionImpl session = (SessionImpl) o;
            return id.equals(session.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }
}
