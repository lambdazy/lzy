package ai.lzy.server;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.exceptions.EnvironmentInstallationException;
import ai.lzy.model.graph.Env;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.LzyFsGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.stream.Stream;

public abstract class ServantsAllocatorBase extends TimerTask implements ServantsAllocator.Ex {
    public static final int PERIOD = 1000;
    public static final int GRACEFUL_SHUTDOWN_PERIOD_SEC = 10;
    public static final ThreadGroup SERVANT_CONNECTIONS_TG = new ThreadGroup("Servant connections");
    private static final Logger LOG = LogManager.getLogger(ServantsAllocatorBase.class);
    @SuppressWarnings("FieldCanBeLocal")
    private final Timer ttl;
    private final Map<String, CompletableFuture<ServantConnection>> requests = new HashMap<>();
    private final Map<ServantConnection, Instant> spareServants = new HashMap<>();
    private final Map<ServantConnection, Instant> shuttingDown = new HashMap<>();
    private final Map<ServantConnection, Instant> waitingForAllocation = new HashMap<>();

    private final Map<String, Set<SessionImpl>> userToSessions = new HashMap<>();
    private final Map<String, SessionImpl> servant2sessions = new HashMap<>();
    private final Map<String, SessionImpl> sessionsById = new HashMap<>();

    private final Map<String, Session> userSessions = new HashMap<>();
    private final Map<String, ServantConnectionImpl> uncompletedConnections = new HashMap<>();

    private final Authenticator auth;
    private final int waitBeforeShutdown;
    private final int allocationTimeoutInSec;
    private final Executor executor = new ThreadPoolExecutor(1, 5, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>());

    public ServantsAllocatorBase(Authenticator auth, int waitBeforeShutdownInSec, int allocationTimeoutInSec) {
        this.auth = auth;
        this.waitBeforeShutdown = waitBeforeShutdownInSec;
        this.allocationTimeoutInSec = allocationTimeoutInSec;
        ttl = new Timer("Allocator TTL", true);
        ttl.scheduleAtFixedRate(this, PERIOD, PERIOD);
    }

    public ServantsAllocatorBase(Authenticator auth, int waitBeforeShutdownInSec) {
        this(auth, waitBeforeShutdownInSec, 100);
    }

    protected abstract void requestAllocation(String servantId, String servantToken,
                                              Provisioning provisioning,
                                              String bucket);

    @SuppressWarnings("unused")
    protected boolean mutate(ServantConnection connection, Provisioning provisioning, Env env) {
        return false;
    }

    protected abstract void cleanup(ServantConnection s);

    protected abstract void terminate(ServantConnection connection);

    @Override
    public synchronized CompletableFuture<ServantConnection> allocate(
        String sessionId, Provisioning provisioning, Env env
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
            final String servantId = "servant_" + UUID.randomUUID();
            servant2sessions.put(servantId, session);
            requests.put(servantId, requestResult);
            ServantConnectionImpl connection = new ServantConnectionImpl(servantId, env, provisioning);
            uncompletedConnections.put(servantId, connection);
            session.servants.add(connection);
            waitingForAllocation.put(connection, Instant.now().plus(allocationTimeoutInSec, ChronoUnit.SECONDS));
            ForkJoinPool.commonPool().execute(() -> {
                final String servantToken = auth.registerServant(servantId);
                try {
                    requestAllocation(servantId, servantToken, provisioning, session.bucket());
                } catch (Exception e) {
                    requestResult.completeExceptionally(e);
                }
            });
        }
        return requestResult;
    }

    @Override
    public synchronized void register(String servantId, URI servantUri, URI servantFsUri) {
        final ManagedChannel servantChannel = ChannelBuilder.forAddress(servantUri.getHost(), servantUri.getPort())
            .usePlaintext()
            .enableRetry(LzyServantGrpc.SERVICE_NAME)
            .build();

        final LzyServantGrpc.LzyServantBlockingStub servantStub = LzyServantGrpc.newBlockingStub(servantChannel);

        final ManagedChannel fsChannel = ChannelBuilder.forAddress(servantFsUri.getHost(), servantFsUri.getPort())
            .usePlaintext()
            .enableRetry(LzyFsGrpc.SERVICE_NAME)
            .build();
        final LzyFsGrpc.LzyFsBlockingStub servantFsStub = LzyFsGrpc.newBlockingStub(fsChannel);

        final IAM.Empty emptyRequest = IAM.Empty.newBuilder().build();
        if (!requests.containsKey(servantId)) {
            LOG.error("Astray servant found: " + servantId);
            //noinspection ResultOfMethodCallIgnored
            servantStub.stop(emptyRequest);
            servantChannel.shutdownNow();
            fsChannel.shutdownNow();
            return;
        }
        final CompletableFuture<ServantConnection> request = requests.remove(servantId);
        ServantConnectionImpl connection = uncompletedConnections.get(servantId);
        waitingForAllocation.remove(connection);
        if (connection == null) {
            throw new RuntimeException("Trying to register already connected servant");
        }
        final Thread connectionThread = new Thread(SERVANT_CONNECTIONS_TG, () -> {
            final Iterator<Servant.ServantProgress> progressIterator = servantStub.start(emptyRequest);
            Context.current().addListener(l -> connection.progress(Servant.ServantProgress.newBuilder()
                .setFailed(Servant.Failed.newBuilder().build()).build()), Runnable::run);
            try {
                progressIterator.forEachRemaining(progress -> {
                    if (progress.hasStart()) {
                        Servant.EnvResult result = servantStub.env(GrpcConverter.to(connection.env()));
                        uncompletedConnections.remove(servantId);
                        if (result.getRc() != 0) {
                            request.completeExceptionally(
                                new EnvironmentInstallationException(result.getDescription()));
                            return;
                        }
                        request.complete(connection);
                    }
                    if (progress.hasCommunicationCompleted()) {
                        synchronized (ServantsAllocatorBase.this) {
                            spareServants.put(connection, Instant.now().plus(waitBeforeShutdown, ChronoUnit.SECONDS));
                        }
                    }
                    connection.progress(progress);
                });
            } catch (Exception e) {
                LOG.error("Error while servant interconnection, servantId={}", servantId, e);
                if (!request.isDone()) {
                    request.completeExceptionally(new RuntimeException("Servant disconnected"));
                }
                connection.progress(Servant.ServantProgress.newBuilder()
                        .setFailed(Servant.Failed.newBuilder().build()).build());
            } finally {
                synchronized (ServantsAllocatorBase.this) {
                    connection.progress(Servant.ServantProgress.newBuilder()
                            .setConcluded(Servant.Concluded.newBuilder().build()).build());
                    shuttingDown.remove(connection);
                    cleanup(connection);
                }
            }
        }, "connection-to-" + servantId);
        connection.complete(connectionThread, servantUri, servantStub, servantFsUri, servantFsStub);
        servant2sessions.get(servantId).servants.add(connection);
        connectionThread.setDaemon(true);
        connectionThread.start();
    }

    @Override
    public synchronized void deleteSession(String sessionId) {
        final SessionImpl session = sessionsById.remove(sessionId);
        if (session != null) {
            userToSessions.getOrDefault(session.owner(), Set.of()).remove(session);
            session.servants.forEach(
                c -> {
                    servant2sessions.remove(c.servantId);
                    if (uncompletedConnections.containsKey(c.servantId)) {
                        final CompletableFuture<ServantConnection> remove = requests.remove(c.servantId);
                        if (remove != null) {
                            remove.completeExceptionally(
                                    new RuntimeException("Session deleted before start")
                            );
                        }
                        uncompletedConnections.remove(c.servantId);
                        if (c.connectionThread != null) {
                            c.connectionThread.interrupt();
                        }
                        terminate(c);
                        return;
                    }
                    spareServants.put(c, Instant.now());
                }
            );
        }
    }

    @Override
    public synchronized Session registerSession(String userId, String sessionId, String bucket) {
        final SessionImpl session = new SessionImpl(sessionId, userId, bucket);
        userToSessions.computeIfAbsent(userId, u -> new HashSet<>()).add(session);
        sessionsById.put(sessionId, session);
        userSessions.put(userId, session);
        return session;
    }

    @Override
    public synchronized Session get(String sessionId) {
        return sessionsById.get(sessionId);
    }

    @Override
    public synchronized Stream<Session> sessions(String userId) {
        return new ArrayList<>(userToSessions.get(userId)).stream().map(s -> s);
    }

    @Override
    public synchronized Session byServant(String servantId) {
        return servant2sessions.get(servantId);
    }

    @Override
    public synchronized Session userSession(String user) {
        Session session = userSessions.get(user);
        if (session == null) {
            session = registerSession(user, "xxx_" + UUID.randomUUID(), auth.bucketForUser(user));
        }
        return session;
    }

    public synchronized void run() {
        final Instant now = Instant.now();
        final List<ServantConnection> tasksToShutdown = Set.copyOf(spareServants.keySet()).stream()
            .filter(s -> spareServants.get(s).isBefore(now))
            .peek(spareServants::remove).toList();
        final List<ServantConnection> tasksToForceStop = Set.copyOf(shuttingDown.keySet()).stream()
            .filter(s -> shuttingDown.get(s).isBefore(now))
            .peek(shuttingDown::remove).toList();

        final List<ServantConnection> notAllocatedServants = Set.copyOf(waitingForAllocation.keySet())
            .stream()
            .filter(s -> waitingForAllocation.get(s).isBefore(now))
            .peek(s -> uncompletedConnections.remove(s.id()))
            .peek(waitingForAllocation::remove)
            .peek(s -> servant2sessions.remove(s.id()))
            .peek(s -> requests.get(s.id()).completeExceptionally(new RuntimeException("Timeout exceeded")))
            .peek(s -> requests.remove(s.id())).toList();

        executor.execute(() -> {
            tasksToShutdown.forEach(s -> {
                final SessionImpl session = servant2sessions.remove(s.id());
                if (session != null) {
                    session.servants.remove(s);
                }
                shuttingDown.put(s, Instant.now().plus(GRACEFUL_SHUTDOWN_PERIOD_SEC, ChronoUnit.SECONDS));
                try {
                    //noinspection ResultOfMethodCallIgnored
                    s.control().stop(IAM.Empty.newBuilder().build());
                } catch (Exception e) {
                    LOG.error("Failed to shutdown servant: ", e);
                    terminate(s);
                    shuttingDown.remove(s);
                }
            });
            tasksToForceStop.forEach(this::terminate);
        });
        executor.execute(() -> {
            notAllocatedServants.forEach(this::terminate);
        });
    }

    private static class ServantConnectionImpl implements ServantConnection {
        private final String servantId;
        private final List<Predicate<Servant.ServantProgress>> trackers;
        private final Env env;
        private final Provisioning provisioning;
        private final AtomicBoolean completed = new AtomicBoolean(false);
        private URI servantUri;
        private URI servantFsUri;
        private Thread connectionThread;
        private LzyServantGrpc.LzyServantBlockingStub control;
        private LzyFsGrpc.LzyFsBlockingStub fs;

        protected ServantConnectionImpl(String servantId, Env env, Provisioning provisioning) {
            this.servantId = servantId;
            trackers = Collections.synchronizedList(new ArrayList<>());
            this.env = env;
            this.provisioning = provisioning;
        }

        public void complete(Thread connectionThread,
                             URI servantUri, LzyServantGrpc.LzyServantBlockingStub control,
                             URI servantFsUri, LzyFsGrpc.LzyFsBlockingStub fs) {
            if (completed.get()) {
                throw new RuntimeException("Servant connection already completed");
            }
            this.servantUri = servantUri;
            this.control = control;
            this.servantFsUri = servantFsUri;
            this.fs = fs;
            this.connectionThread = connectionThread;
            completed.set(true);
        }

        protected void progress(Servant.ServantProgress progress) {
            LOG.info("Progress of servant connection: " + JsonUtils.printRequest(progress));
            List.copyOf(trackers).stream().filter(t -> {
                try {
                    return !t.test(progress);
                } catch (RuntimeException e) {
                    LOG.error(e);
                    return false; // this could cause memory leak
                }
            }).forEach(trackers::remove);
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
        public URI fsUri() {
            return servantFsUri;
        }

        @Override
        public LzyServantGrpc.LzyServantBlockingStub control() {
            return control;
        }

        @Override
        public LzyFsGrpc.LzyFsBlockingStub fs() {
            return fs;
        }

        @Override
        public Env env() {
            return env;
        }

        @Override
        public Provisioning provisioning() {
            return provisioning;
        }

        @Override
        public final boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ServantConnectionImpl that = (ServantConnectionImpl) o;
            return Objects.equals(this.id(), that.id());
        }

        @Override
        public final int hashCode() {
            return Objects.hash(this.id());
        }
    }

    private static class SessionImpl implements SessionManager.Session {
        private final String id;
        private final String user;
        private final List<ServantConnectionImpl> servants = new ArrayList<>();
        private final String bucket;

        private SessionImpl(String id, String user, String bucket) {
            this.id = id;
            this.user = user;
            this.bucket = bucket;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String owner() {
            return user;
        }

        @Override
        public String[] servants() {
            return servants.stream().map(ServantConnection::id).toArray(String[]::new);
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
