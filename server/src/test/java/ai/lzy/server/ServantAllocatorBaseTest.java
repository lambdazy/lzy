package ai.lzy.server;

import io.grpc.Status;
import io.micronaut.context.ApplicationContext;
import org.apache.http.client.utils.URIBuilder;
import org.junit.*;
import ai.lzy.model.GrpcConverter;
import ai.lzy.model.graph.Provisioning;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.server.mocks.AllocatedServantMock;
import ai.lzy.server.mocks.ServantAllocatorMock;
import ai.lzy.priv.v2.Operations;
import ai.lzy.priv.v2.Servant;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ServantAllocatorBaseTest {

    private static final String DEFAULT_USER = "default_user";
    private static final String DEFAULT_BUCKET = "default_bucket";

    private static Authenticator authenticator;
    private static ExecutorService executor;
    private ServantAllocatorMock allocator;

    @BeforeClass
    public static void classSetUp() {
        final ApplicationContext run = ApplicationContext.run(Map.of(
            "authenticator", "SimpleInMemAuthenticator",
            "database.enabled", false
        ));
        authenticator = run.getBean(Authenticator.class);
        executor = Executors.newFixedThreadPool(100);
    }

    @AfterClass
    public static void classTearDown() {
        executor.shutdown();
    }

    @Before
    public void setUp() {
        allocator = new ServantAllocatorMock(authenticator, 1);
    }

    @Ignore
    @Test(expected = IllegalArgumentException.class)
    public void testRegisterSessionsWithSameUUID() {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();

        //Act
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);
    }

    @Test
    public void testGetNonexistentSession() {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();

        //Act
        final SessionManager.Session session = allocator.get(sid);

        //Assert
        Assert.assertNull(session);
    }

    @Test
    public void testDeleteNonexistentSession() {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();

        //Act
        allocator.deleteSession(sid);
    }

    @Test
    public void testGetSessionForNonexistentServant() {
        //Arrange
        final String servantUUID = "servant_" + UUID.randomUUID();

        //Act
        final SessionManager.Session session = allocator.byServant(servantUUID);

        //Assert
        Assert.assertNull(session);
    }

    @Test
    public void testGetSessionForNonexistentUser() {
        //Arrange
        final String userId = UUID.randomUUID().toString();

        //Act
        final SessionManager.Session session = allocator.userSession(userId);

        //Assert
        Assert.assertNotNull(session);
    }

    @Ignore
    @Test
    public void testGetSessionsForNonexistentUser() {
        //Arrange
        final String userId = UUID.randomUUID().toString();

        //Act
        final List<SessionManager.Session> sessions = allocator.sessions(userId).toList();

        //Assert
        Assert.assertEquals(0, sessions.size());
    }

    @Test
    public void testUserSessionNotOverrideExistingOne() {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();

        //Act
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);
        final SessionManager.Session session = allocator.userSession(DEFAULT_USER);

        //Assert
        Assert.assertEquals(sid, session.id());
    }

    @Test
    public void testRegisteringMultipleSessions() {
        //Arrange
        final Map<String, List<String>> userSessions = new HashMap<>();
        IntStream.range(0, 100)
            .mapToObj(value -> "user_" + UUID.randomUUID())
            .forEach(s -> userSessions.put(s, IntStream.range(0, 100)
                .mapToObj(value -> "session_" + UUID.randomUUID()).collect(Collectors.toList())));

        //Act
        userSessions.forEach((uid, sids) -> sids.forEach(sid -> allocator.registerSession(uid, sid, DEFAULT_BUCKET)));

        //Assert
        userSessions.entrySet().stream().flatMap(stringListEntry -> stringListEntry.getValue().stream())
            .forEach(uuid -> {
                final SessionManager.Session session = allocator.get(uuid);
                Assert.assertNotNull(session);
                Assert.assertEquals(uuid, session.id());
            });
        userSessions.forEach((uid, sids) ->
            Assert.assertEquals(new HashSet<>(sids),
                allocator.sessions(uid).map(SessionManager.Session::id).collect(Collectors.toSet())));
    }

    @Test
    public void testRemovingSessionsInParallelWithGetting() throws ExecutionException, InterruptedException {
        //Arrange
        final List<String> sids = IntStream.range(0, 10000).mapToObj(value -> "session_" + UUID.randomUUID()).toList();
        sids.forEach(sid -> allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET));
        final AtomicInteger added = new AtomicInteger();

        //Act
        final Future<?> removing = executor.submit(() -> sids.forEach(sid -> allocator.deleteSession(sid)));
        final Future<?> getting = executor.submit(() -> sids.forEach(sid -> {
            final SessionManager.Session session = allocator.get(sid);
            if (session == null) { //already removed
                allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);
                added.incrementAndGet();
            }
        }));
        removing.get();
        getting.get();

        //Assert
        final List<SessionManager.Session> collect = allocator.sessions(DEFAULT_USER).toList();
        Assert.assertEquals(added.get(), collect.size());
    }

    @Test
    public void testParallelRemovingAndSessionsGettingByUser() throws ExecutionException, InterruptedException {
        //Arrange
        final List<String> sids = IntStream.range(0, 1000).mapToObj(value -> "session_" + UUID.randomUUID()).toList();
        sids.forEach(sid -> allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET));

        //Act
        final Future<?> removing = executor.submit(() -> sids.forEach(sid -> allocator.deleteSession(sid)));
        final List<SessionManager.Session> sessions = allocator.sessions(DEFAULT_USER).collect(Collectors.toList());
        removing.get();

        //Assert
        //Checking that there were no ConcurrentModificationException
        Assert.assertNotNull(sessions);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAllocationWithNonexistentSession() {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();

        //Act
        allocator.allocate(sid, new Provisioning.Any(),
            GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
    }

    @Test
    public void testSingleAllocation() throws Exception {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);

        //Act
        CompletableFuture<ServantsAllocator.ServantConnection> feature = allocator.allocate(
            sid, new Provisioning.Any(),
            GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
        final boolean allocated = allocator.waitForAllocations();
        final ServantAllocatorMock.AllocationRequest request = allocator.allocations().findFirst().orElseThrow();
        final SessionManager.Session session = allocator.byServant(request.servantId());

        int port = FreePortFinder.find(10000, 20000);

        AllocatedServantMock servantMock = new AllocatedServantMock(false, t -> {
        }, port);
        allocator.register(request.servantId(), new URIBuilder().setHost("localhost").setPort(port).build(),
            new URI("dummy://foo"));
        ServantsAllocator.ServantConnection connection = feature.get();
        final AtomicBoolean progressComplete = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        connection.onProgress(progress -> {
            if (progress.hasChanged()) {
                progressComplete.set(true);
                latch.countDown();
            }
            return true;
        });
        servantMock.progress(Servant.ServantProgress.newBuilder()
            .setChanged(Servant.StateChanged.newBuilder().build())
            .build());
        latch.await();
        servantMock.complete(null);
        servantMock.close();

        //Assert
        Assert.assertTrue(allocated);
        Assert.assertNotNull(session);
        Assert.assertEquals(sid, session.id());
        Assert.assertTrue(progressComplete.get());
    }

    @Test
    public void testDeleteWhileAllocating() {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);

        //Act
        CompletableFuture<?> feature = allocator.allocate(sid, new Provisioning.Any(),
            GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
        final boolean allocationRequested = allocator.waitForAllocations();
        final ServantAllocatorMock.AllocationRequest request = allocator.allocations().findFirst().orElseThrow();
        allocator.deleteSession(sid);
        final SessionManager.Session session = allocator.byServant(request.servantId());

        //Assert
        Assert.assertTrue(feature.isCompletedExceptionally());
        Assert.assertTrue(allocationRequested);
        Assert.assertNull(session);
    }

    @Test
    public void testDeleteWhileExecuting() throws Exception {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);

        //Act
        CompletableFuture<ServantsAllocator.ServantConnection> feature = allocator.allocate(
            sid, new Provisioning.Any(),
            GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
        allocator.waitForAllocations();
        final ServantAllocatorMock.AllocationRequest request = allocator.allocations().findFirst().orElseThrow();

        int port = FreePortFinder.find(10000, 20000);
        AtomicBoolean stopCalled = new AtomicBoolean(false);

        @SuppressWarnings("CheckStyle")
        AllocatedServantMock servantMock = new AllocatedServantMock(false, t -> {
            stopCalled.set(true);
            t.complete(null);
        }, port);
        allocator.register(request.servantId(), new URIBuilder().setHost("localhost").setPort(port).build(),
            new URI("dummy://foo"));
        ServantsAllocator.ServantConnection connection = feature.get();

        AtomicBoolean gotDisconnectedWhenDeleted = new AtomicBoolean(false);
        CompletableFuture<?> called = new CompletableFuture<>();
        connection.onProgress(p -> {
            if (p.hasConcluded()) {
                gotDisconnectedWhenDeleted.set(true);
                called.complete(null);
                return false;
            }
            return true;
        });

        allocator.deleteSession(sid);
        called.get();

        //Assert
        Assert.assertTrue(gotDisconnectedWhenDeleted.get());
        Assert.assertTrue(stopCalled.get());

        servantMock.close();

    }

    @Test
    public void testErrorOnProgress() throws IOException, ExecutionException, InterruptedException, URISyntaxException {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);

        //Act
        CompletableFuture<ServantsAllocator.ServantConnection> feature = allocator.allocate(
            sid, new Provisioning.Any(),
            GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
        allocator.waitForAllocations();
        final ServantAllocatorMock.AllocationRequest request = allocator.allocations().findFirst().orElseThrow();

        int port = FreePortFinder.find(10000, 20000);

        AllocatedServantMock servantMock = new AllocatedServantMock(false, t -> {
        }, port);
        allocator.register(request.servantId(), new URIBuilder().setHost("localhost").setPort(port).build(),
            new URI("dummy://foo"));
        ServantsAllocator.ServantConnection connection = feature.get();
        servantMock.complete(Status.UNKNOWN.asRuntimeException());

        AtomicBoolean gotExit = new AtomicBoolean(false);
        CompletableFuture<?> called = new CompletableFuture<>();
        connection.onProgress(p -> {
            if (p.hasFailed()) {
                gotExit.set(true);
                called.complete(null);
                return false;
            }
            return true;
        });

        called.get();
        servantMock.close();

        //Assert
        Assert.assertTrue(gotExit.get());
    }

    @Test
    public void testTimeoutOnAllocation() {
        //Arrange
        final ServantAllocatorMock allocator = new ServantAllocatorMock(authenticator, 10, 1, 1);
        final String sid = "session_" + UUID.randomUUID();
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);

        //Act
        CompletableFuture<ServantsAllocator.ServantConnection> feature = allocator.allocate(
            sid, new Provisioning.Any(),
            GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
        allocator.waitForAllocations();

        // Assert
        Assert.assertThrows(ExecutionException.class, () -> feature.get(5, TimeUnit.SECONDS));
        allocator.waitForTermination();
        Assert.assertEquals(1, allocator.terminations().size());

        final ServantAllocatorMock.AllocationRequest request = allocator.allocations().findFirst().orElseThrow();
        Assert.assertEquals(request.servantId(), allocator.terminations().get(0).id());
    }

    @Test
    public void testEnvFailOnAllocation() throws IOException, URISyntaxException, InterruptedException {
        //Arrange
        final String sid = "session_" + UUID.randomUUID();
        allocator.registerSession(DEFAULT_USER, sid, DEFAULT_BUCKET);
        final CountDownLatch stopLatch = new CountDownLatch(1);
        int port = FreePortFinder.find(10000, 20000);
        AllocatedServantMock servantMock = new AllocatedServantMock(true, t -> stopLatch.countDown(), port);

        //Act
        CompletableFuture<ServantsAllocator.ServantConnection> feature = allocator.allocate(
            sid, new Provisioning.Any(),
            GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
        final boolean allocated = allocator.waitForAllocations();
        final ServantAllocatorMock.AllocationRequest request = allocator.allocations().findFirst().orElseThrow();

        allocator.register(request.servantId(), new URIBuilder().setHost("localhost").setPort(port).build(),
            new URI("dummy://foo"));
        try {
            feature.get(30, TimeUnit.SECONDS);
        } catch (ExecutionException | TimeoutException ignored) {
        }
        boolean completedExceptionally = feature.isCompletedExceptionally();

        //Assert
        Assert.assertTrue(stopLatch.await(30, TimeUnit.SECONDS));
        Assert.assertTrue(allocated);
        Assert.assertTrue(completedExceptionally);

        servantMock.complete(null);
        servantMock.close();
    }
}
