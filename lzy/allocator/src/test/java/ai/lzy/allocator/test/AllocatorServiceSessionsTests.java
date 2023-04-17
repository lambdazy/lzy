package ai.lzy.allocator.test;

import ai.lzy.allocator.alloc.dao.SessionDao;
import ai.lzy.allocator.alloc.dao.impl.SessionDaoImpl;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.AllocatorGrpc;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.util.Durations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class AllocatorServiceSessionsTests extends AllocatorApiTestBase {

    @Before
    public void before() throws IOException {
        super.setUp();
    }

    @After
    public void after() {
        super.tearDown();
    }

    @Test
    public void testUnauthenticated() {
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.createSession(VmAllocatorApi.CreateSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            unauthorizedAllocatorBlockingStub.deleteSession(VmAllocatorApi.DeleteSessionRequest.newBuilder().build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testPermissionDenied() {
        final AllocatorGrpc.AllocatorBlockingStub invalidAuthorizedAllocatorBlockingStub =
            unauthorizedAllocatorBlockingStub.withInterceptors(
                ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION,
                    JwtUtils.invalidCredentials("user", "GITHUB")::token));
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.createSession(
                VmAllocatorApi.CreateSessionRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            //noinspection ResultOfMethodCallIgnored
            invalidAuthorizedAllocatorBlockingStub.deleteSession(
                VmAllocatorApi.DeleteSessionRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void testCreateAndDeleteSession() throws SQLException {
        var sessionId = createSession(Durations.fromSeconds(100));

        var op = deleteSession(sessionId, true);
        Assert.assertTrue(op.toString(), op.hasResponse());

        var session = allocatorCtx.getBean(SessionDao.class).get(sessionId, null);
        Assert.assertNull(session);
    }

    @Test
    public void createSessionNoOwner() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setCachePolicy(
                        VmAllocatorApi.CachePolicy.newBuilder()
                            .setIdleTimeout(Durations.fromSeconds(100))
                            .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void createSessionNoCachePolicy() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setOwner(idGenerator.generate("user-"))
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void createSessionNoCachePolicyDuration() {
        try {
            //noinspection ResultOfMethodCallIgnored
            authorizedAllocatorBlockingStub.createSession(
                VmAllocatorApi.CreateSessionRequest.newBuilder()
                    .setOwner(idGenerator.generate("user-"))
                    .setCachePolicy(
                        VmAllocatorApi.CachePolicy.newBuilder()
                            .build())
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
        }
    }

    @Test
    public void errorWhileCreatingSession() {
        allocatorCtx.getBean(SessionDaoImpl.class).injectError(new SQLException("non retryable", "xxx"));

        try {
            var sessionId = createSession(Durations.fromSeconds(100));
            Assert.fail(sessionId);
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.Code.INTERNAL, e.getStatus().getCode());
            Assert.assertEquals("non retryable", e.getStatus().getDescription());
        }
    }

    @Test
    public void retryableSqlErrorWhileCreatingSession() {
        allocatorCtx.getBean(SessionDaoImpl.class).injectError(
            new PSQLException("retry me, plz", PSQLState.CONNECTION_FAILURE));

        createSession(Durations.fromSeconds(100));
    }

    @Test
    public void idempotentCreateSessionRepeatableCalls() {
        var op1 = createSessionOp("test", Durations.ZERO, "key-1");
        var op2 = createSessionOp("test", Durations.ZERO, "key-1");

        Assert.assertEquals(op1.getId(), op2.getId());
        Assert.assertEquals(Utils.extractSessionId(op1), Utils.extractSessionId(op2));
    }

    @Test
    public void idempotentCreateSessionRepeatableCallsWithModification() {
        createSessionOp("test", Durations.ZERO, "key-1");

        try {
            createSessionOp("test1", Durations.ZERO, "key-1");
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            Assert.assertEquals("IdempotencyKey conflict", e.getStatus().getDescription());
        }

        try {
            createSessionOp("test", Durations.fromSeconds(1), "key-1");
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(Status.INVALID_ARGUMENT.getCode(), e.getStatus().getCode());
            Assert.assertEquals("IdempotencyKey conflict", e.getStatus().getDescription());
        }
    }

    @Test
    public void idempotentCreateSessionConcurrentCalls() throws Exception {
        final int N = 10;

        var readyLatch = new CountDownLatch(N);
        var doneLatch = new CountDownLatch(N);
        var sids = new String[N];

        var failed = new AtomicBoolean(false);

        var executor = Executors.newFixedThreadPool(N);
        for (int i = 0; i < N; ++i) {
            final int index = i;
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    var op = createSessionOp("test", Durations.ZERO, "key-1");
                    var sid = Utils.extractSessionId(op);
                    sids[index] = sid;
                } catch (Exception e) {
                    failed.set(true);
                    System.err.println("Exception in thread " + Thread.currentThread().getName());
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        readyLatch.await();
        doneLatch.await();
        executor.shutdown();

        Assert.assertFalse(failed.get());

        Assert.assertNotNull(sids[0]);
        var sid = sids[0];
        for (int i = 1; i < N; ++i) {
            Assert.assertEquals(sid, sids[i]);
        }
    }
}
