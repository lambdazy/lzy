package ru.yandex.cloud.ml.platform.lzy.server;

import io.micronaut.context.ApplicationContext;
import org.junit.*;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.Provisioning;
import yandex.cloud.priv.datasphere.v2.lzy.Operations;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        final UUID sessionUUID = UUID.randomUUID();

        //Act
        allocator.registerSession(DEFAULT_USER, sessionUUID, DEFAULT_BUCKET);
        allocator.registerSession(DEFAULT_USER, sessionUUID, DEFAULT_BUCKET);
    }

    @Test
    public void testGetNonexistentSession() {
        //Arrange
        final UUID sessionUUID = UUID.randomUUID();

        //Act
        final SessionManager.Session session = allocator.get(sessionUUID);

        //Assert
        Assert.assertNull(session);
    }

    @Test
    public void testDeleteNonexistentSession() {
        //Arrange
        final UUID sessionUUID = UUID.randomUUID();

        //Act
        allocator.deleteSession(sessionUUID);
    }

    @Test
    public void testGetSessionForNonexistentServant() {
        //Arrange
        final UUID servantUUID = UUID.randomUUID();

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
        final List<SessionManager.Session> sessions = allocator.sessions(userId).collect(Collectors.toList());

        //Assert
        Assert.assertEquals(0, sessions.size());
    }

    @Ignore
    @Test
    public void testUserSessionNotOverrideExistingOne() {
        //Arrange
        final UUID sessionUUID = UUID.randomUUID();

        //Act
        allocator.registerSession(DEFAULT_USER, sessionUUID, DEFAULT_BUCKET);
        final SessionManager.Session session = allocator.userSession(DEFAULT_USER);

        //Assert
        Assert.assertEquals(sessionUUID, session.id());
    }

    @Test
    public void testRegisteringMultipleSessions() {
        //Arrange
        final Map<String, List<UUID>> userSessions = new HashMap<>();
        IntStream.range(0, 100)
                .mapToObj(value -> UUID.randomUUID().toString())
                .forEach(s -> userSessions.put(s, IntStream.range(0, 100)
                        .mapToObj(value -> UUID.randomUUID()).collect(Collectors.toList())));

        //Act
        userSessions.forEach((uid, sids) -> sids.forEach(sid -> allocator.registerSession(uid, sid, DEFAULT_BUCKET)));

        //Assert
        userSessions.entrySet().stream().flatMap(stringListEntry -> stringListEntry.getValue().stream()).forEach(uuid -> {
            final SessionManager.Session session = allocator.get(uuid);
            Assert.assertNotNull(session);
            Assert.assertEquals(uuid, session.id());
        });
        userSessions.forEach((uid, sids) ->
                Assert.assertEquals(new HashSet<>(sids),
                        allocator.sessions(uid).map(SessionManager.Session::id).collect(Collectors.toSet())));
    }

    @Ignore
    @Test
    public void testRemovingSessionsInParallelWithGetting() throws ExecutionException, InterruptedException {
        //Arrange
        final List<UUID> sids = IntStream.range(0, 10000).mapToObj(value -> UUID.randomUUID())
                .collect(Collectors.toList());
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
        final List<SessionManager.Session> collect = allocator.sessions(DEFAULT_USER).collect(Collectors.toList());
        Assert.assertEquals(added.get(), collect.size());
    }

    @Ignore
    @Test
    public void testParallelRemovingAndSessionsGettingByUser() throws ExecutionException, InterruptedException {
        //Arrange
        final List<UUID> sids = IntStream.range(0, 1000).mapToObj(value -> UUID.randomUUID())
                .collect(Collectors.toList());
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
        final UUID sessionUUID = UUID.randomUUID();

        //Act
        allocator.allocate(sessionUUID, new Provisioning.Any(),
                GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
    }

    @Test
    public void testSingleAllocation() {
        //Arrange
        final UUID sessionUUID = UUID.randomUUID();
        allocator.registerSession(DEFAULT_USER, sessionUUID, DEFAULT_BUCKET);

        //Act
        allocator.allocate(sessionUUID, new Provisioning.Any(),
                GrpcConverter.from(Operations.EnvSpec.newBuilder().build()));
        final boolean allocated = allocator.waitForAllocations();
        final ServantAllocatorMock.AllocationRequest request = allocator.allocations().findFirst().orElseThrow();
        final SessionManager.Session session = allocator.byServant(request.servantId());

        //Assert
        Assert.assertTrue(allocated);
        Assert.assertNotNull(session);
        Assert.assertEquals(sessionUUID, session.id());
    }
}
