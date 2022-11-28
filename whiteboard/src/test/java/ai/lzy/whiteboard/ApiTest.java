package ai.lzy.whiteboard;

import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.DataScheme;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.whiteboard.LWB;
import ai.lzy.v1.whiteboard.LWBS;
import ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc;
import ai.lzy.whiteboard.grpc.ProtoConverter;
import ai.lzy.whiteboard.grpc.WhiteboardService;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.micronaut.inject.qualifiers.Qualifiers;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static ai.lzy.model.grpc.ProtoConverter.toProto;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static ai.lzy.v1.whiteboard.LWB.WhiteboardFieldInfo.StateCase.LINKEDSTATE;
import static ai.lzy.v1.whiteboard.LWB.WhiteboardFieldInfo.StateCase.NONESTATE;
import static org.junit.Assert.assertEquals;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ApiTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });

    private ApplicationContext context;
    private LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub externalUserWhiteboardClient;
    private LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub externalUser2WhiteboardClient;
    private LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub whiteboardClient;
    private Server whiteboardServer;
    private ManagedChannel channel;

    @Before
    public void setUp() throws Exception {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("whiteboard", db.getConnectionInfo()));
        final var config = context.getBean(AppConfig.class);
        var address = HostAndPort.fromString(config.getAddress());

        var iamChannel = context.getBean(ManagedChannel.class, Qualifiers.byName("WhiteboardIamGrpcChannel"));

        whiteboardServer = WhiteboardApp.createServer(address, iamChannel, context.getBean(WhiteboardService.class));
        whiteboardServer.start();

        channel = ChannelBuilder
            .forAddress(address)
            .usePlaintext()
            .build();
        var credentials = config.getIam().createRenewableToken();
        whiteboardClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> credentials.get().token()));

        User externalUser;
        User externalUser2;
        try (final var iamClient = new IamClient(config.getIam())) {
            externalUser = iamClient.createUser("wbUser");
            externalUser2 = iamClient.createUser("wbOtherUser");
        }
        externalUserWhiteboardClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, externalUser.credentials::token));

        externalUser2WhiteboardClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, externalUser2.credentials::token));

    }

    @After
    public void after() {
        whiteboardServer.shutdown();
        try {
            whiteboardServer.awaitTermination();
        } catch (InterruptedException ignored) {
            // ignored
        }

        channel.shutdown();
        try {
            channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            //ignored
        }

        DatabaseTestUtils.cleanup(context.getBean(WhiteboardDataSource.class));
        context.stop();
        super.after();
    }

    @Test
    public void testUnauthenticated() {
        final var unauthorizedClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel);
        apiAccessTest(unauthorizedClient, Status.UNAUTHENTICATED);
    }

    @Test
    public void testPermissionDenied() {
        final var invalidCredsClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(
                GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user", "GITHUB")::token
            ));
        apiAccessTest(invalidCredsClient, Status.PERMISSION_DENIED);
    }

    @Test
    public void createAndGetWhiteboard() {
        try {
            externalUserWhiteboardClient.get(LWBS.GetRequest.newBuilder().setWhiteboardId("some_wb_id").build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());
        }

        final var createdWhiteboard = externalUserWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();

        assertEquals(LWB.Whiteboard.Status.CREATED, createdWhiteboard.getStatus());
        assertEquals(4, createdWhiteboard.getFieldsCount());
        createdWhiteboard.getFieldsList().forEach(field -> {
            assertEquals(LWB.WhiteboardField.Status.CREATED, field.getStatus());
        });
        final var fields = createdWhiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getInfo().getName(), f -> f));
        assertEquals(NONESTATE, fields.get("f1").getInfo().getStateCase());
        assertEquals(NONESTATE, fields.get("f2").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, fields.get("f3").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, fields.get("f4").getInfo().getStateCase());

        final var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(createdWhiteboard.getId()).build();
        final var getResponse = whiteboardClient.get(getRequest);
        assertEquals(createdWhiteboard, getResponse.getWhiteboard());

        final var getUserResponse = externalUserWhiteboardClient.get(getRequest);
        assertEquals(createdWhiteboard, getUserResponse.getWhiteboard());

        try {
            externalUser2WhiteboardClient.get(getRequest);
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());
        }
    }

    @Test
    public void finalizeWhiteboard() {
        final var createdWhiteboard = whiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();
        final var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(createdWhiteboard.getId()).build();

        whiteboardClient.linkField(LWBS.LinkFieldRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .setFieldName("f1")
            .setStorageUri("s-uri-1")
            .setScheme(toProto(DataScheme.PLAIN))
            .build());

        whiteboardClient.linkField(LWBS.LinkFieldRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .setFieldName("f4")
            .setStorageUri("s-uri-4")
            .setScheme(toProto(DataScheme.PLAIN))
            .build());

        var whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.CREATED, whiteboard.getStatus());
        whiteboard.getFieldsList().forEach(field -> {
            if (field.getInfo().getName().equals("f1") || field.getInfo().getName().equals("f4")) {
                assertEquals(LWB.WhiteboardField.Status.FINALIZED, field.getStatus());
            } else {
                assertEquals(LWB.WhiteboardField.Status.CREATED, field.getStatus());
            }
        });

        whiteboardClient.finalizeWhiteboard(LWBS.FinalizeWhiteboardRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .build());

        whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.FINALIZED, whiteboard.getStatus());
        whiteboard.getFieldsList().forEach(field -> {
            assertEquals(LWB.WhiteboardField.Status.FINALIZED, field.getStatus());
        });

        final var finalizedFields = whiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getInfo().getName(), f -> f));
        assertEquals(LINKEDSTATE, finalizedFields.get("f1").getInfo().getStateCase());
        assertEquals(NONESTATE, finalizedFields.get("f2").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, finalizedFields.get("f3").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, finalizedFields.get("f4").getInfo().getStateCase());
        assertEquals("s-uri-4", finalizedFields.get("f4").getInfo().getLinkedState().getStorageUri());
    }

    @Test
    public void listWhiteboards() {
        final var whiteboard1 = externalUserWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest("wb1", List.of("t1"))).getWhiteboard();
        final var whiteboard2 = externalUserWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest("wb2", List.of("t2"))).getWhiteboard();
        final var whiteboard3 = externalUserWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest("wb3", List.of("t1", "t2"))).getWhiteboard();
        final var whiteboard4 = externalUser2WhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest("wb", List.of("t1", "t2"))).getWhiteboard();

        var listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder().build());
        assertEquals(3, listResult.getWhiteboardsCount());

        listResult = externalUser2WhiteboardClient.list(LWBS.ListRequest.newBuilder().build());
        assertEquals(1, listResult.getWhiteboardsCount());
        assertEquals(whiteboard4, listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder()
            .addAllTags(List.of("t1", "t2")).build());
        assertEquals(1, listResult.getWhiteboardsCount());
        assertEquals(whiteboard3, listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder().setName("wb1").build());
        assertEquals(1, listResult.getWhiteboardsCount());
        assertEquals(whiteboard1, listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder()
            .setCreatedTimeBounds(LWB.TimeBounds.newBuilder().setTo(whiteboard2.getCreatedAt()).build()).build());
        assertEquals(2, listResult.getWhiteboardsCount());
    }

    @Test
    public void createWhiteboardIdempotency() {
        var idempotencyKey = "idempotency-key";

        var createdWhiteboard1 = withIdempotencyKey(externalUserWhiteboardClient, idempotencyKey)
            .createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();
        var createdWhiteboard2 = withIdempotencyKey(externalUserWhiteboardClient, idempotencyKey)
            .createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();

        var fields1 = createdWhiteboard1.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getInfo().getName(), f -> f));
        var fields2 = createdWhiteboard2.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getInfo().getName(), f -> f));

        assertEquals(LWB.Whiteboard.Status.CREATED, createdWhiteboard1.getStatus());
        assertEquals(4, createdWhiteboard1.getFieldsCount());
        createdWhiteboard1.getFieldsList().forEach(field -> {
            assertEquals(LWB.WhiteboardField.Status.CREATED, field.getStatus());
        });
        assertEquals(NONESTATE, fields1.get("f1").getInfo().getStateCase());
        assertEquals(NONESTATE, fields1.get("f2").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, fields1.get("f3").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, fields1.get("f4").getInfo().getStateCase());

        assertEquals(createdWhiteboard1.getStatus(), createdWhiteboard2.getStatus());
        assertEquals(createdWhiteboard1.getFieldsCount(), createdWhiteboard2.getFieldsCount());
        createdWhiteboard2.getFieldsList().forEach(field -> {
            assertEquals(LWB.WhiteboardField.Status.CREATED, field.getStatus());
        });
        assertEquals(fields1.get("f1").getInfo().getStateCase(), fields2.get("f1").getInfo().getStateCase());
        assertEquals(fields1.get("f2").getInfo().getStateCase(), fields2.get("f2").getInfo().getStateCase());
        assertEquals(fields1.get("f3").getInfo().getStateCase(), fields2.get("f3").getInfo().getStateCase());
        assertEquals(fields1.get("f4").getInfo().getStateCase(), fields2.get("f4").getInfo().getStateCase());
    }

    @Test
    public void linkWbFieldIdempotency() {
        var idempotencyKey = "idempotency-key";
        var idempotentClient = withIdempotencyKey(whiteboardClient, idempotencyKey);

        var createdWhiteboard = whiteboardClient.createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();

        idempotentClient.linkField(LWBS.LinkFieldRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .setFieldName("f1")
            .setStorageUri("s-uri-1")
            .setScheme(toProto(DataScheme.PLAIN))
            .build());

        idempotentClient.linkField(LWBS.LinkFieldRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .setFieldName("f1")
            .setStorageUri("s-uri-1")
            .setScheme(toProto(DataScheme.PLAIN))
            .build());

        var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(createdWhiteboard.getId()).build();
        var whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.CREATED, whiteboard.getStatus());
        whiteboard.getFieldsList().forEach(field -> {
            if (field.getInfo().getName().equals("f1")) {
                assertEquals(LWB.WhiteboardField.Status.FINALIZED, field.getStatus());
            } else {
                assertEquals(LWB.WhiteboardField.Status.CREATED, field.getStatus());
            }
        });
    }

    @Test
    public void finalizeWhiteboardIdempotency() {
        var idempotencyKey = "idempotency-key";
        var idempotentClient = withIdempotencyKey(whiteboardClient, idempotencyKey);

        var createdWhiteboard = whiteboardClient.createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();

        idempotentClient.finalizeWhiteboard(LWBS.FinalizeWhiteboardRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .build());

        idempotentClient.finalizeWhiteboard(LWBS.FinalizeWhiteboardRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .build());

        var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(createdWhiteboard.getId()).build();
        var whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.FINALIZED, whiteboard.getStatus());
        whiteboard.getFieldsList().forEach(field -> {
            assertEquals(LWB.WhiteboardField.Status.FINALIZED, field.getStatus());
        });

        final var finalizedFields = whiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getInfo().getName(), f -> f));
        assertEquals(NONESTATE, finalizedFields.get("f1").getInfo().getStateCase());
        assertEquals(NONESTATE, finalizedFields.get("f2").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, finalizedFields.get("f3").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, finalizedFields.get("f4").getInfo().getStateCase());
        assertEquals("s-uri-4-init", finalizedFields.get("f4").getInfo().getLinkedState().getStorageUri());
    }

    @Test
    public void idempotentCreateWbConcurrent() throws InterruptedException {
        var idempotencyKey = "idempotency-key";
        var client = withIdempotencyKey(externalUserWhiteboardClient, idempotencyKey);

        final int N = 10;
        final var readyLatch = new CountDownLatch(N);
        final var doneLatch = new CountDownLatch(N);
        final var executor = Executors.newFixedThreadPool(N);
        final var whiteboards = new LWB.Whiteboard[N];
        final var failed = new AtomicBoolean(false);

        for (int i = 0; i < N; ++i) {
            final int index = i;
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    var createdWhiteboard = client.createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();
                    Assert.assertFalse(createdWhiteboard.getId().isBlank());

                    whiteboards[index] = createdWhiteboard;
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        Assert.assertFalse(failed.get());
        Assert.assertNotNull(whiteboards[0]);
        Assert.assertTrue(Arrays.stream(whiteboards).allMatch(wb ->
            wb.getStatus().equals(LWB.Whiteboard.Status.CREATED) && wb.getFieldsCount() == 4 &&
                wb.getFieldsList().stream()
                    .allMatch(field -> field.getStatus().equals(LWB.WhiteboardField.Status.CREATED))
        ));
    }

    @Test
    public void idempotentLinkWbFieldConcurrent() throws InterruptedException {
        var idempotencyKey = "idempotency-key";
        var client = withIdempotencyKey(externalUserWhiteboardClient, idempotencyKey);
        var createdWhiteboard = externalUserWhiteboardClient.createWhiteboard(genCreateWhiteboardRequest())
            .getWhiteboard();

        final int N = 10;
        final var readyLatch = new CountDownLatch(N);
        final var doneLatch = new CountDownLatch(N);
        final var executor = Executors.newFixedThreadPool(N);
        final var failed = new AtomicBoolean(false);

        for (int i = 0; i < N; ++i) {
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    client.linkField(LWBS.LinkFieldRequest.newBuilder()
                        .setWhiteboardId(createdWhiteboard.getId())
                        .setFieldName("f1")
                        .setStorageUri("s-uri-1")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build());
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        Assert.assertFalse(failed.get());

        var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(createdWhiteboard.getId()).build();
        var whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.CREATED, whiteboard.getStatus());
        whiteboard.getFieldsList().forEach(field -> {
            if (field.getInfo().getName().equals("f1")) {
                assertEquals(LWB.WhiteboardField.Status.FINALIZED, field.getStatus());
            } else {
                assertEquals(LWB.WhiteboardField.Status.CREATED, field.getStatus());
            }
        });
    }

    @Test
    public void idempotentFinalizeWbConcurrent() throws InterruptedException {
        var idempotencyKey = "idempotency-key";
        var client = withIdempotencyKey(externalUserWhiteboardClient, idempotencyKey);
        var createdWhiteboard = externalUserWhiteboardClient.createWhiteboard(genCreateWhiteboardRequest())
            .getWhiteboard();

        final int N = 10;
        final var readyLatch = new CountDownLatch(N);
        final var doneLatch = new CountDownLatch(N);
        final var executor = Executors.newFixedThreadPool(N);
        final var failed = new AtomicBoolean(false);

        for (int i = 0; i < N; ++i) {
            executor.submit(() -> {
                try {
                    readyLatch.countDown();
                    readyLatch.await();

                    client.finalizeWhiteboard(LWBS.FinalizeWhiteboardRequest.newBuilder()
                        .setWhiteboardId(createdWhiteboard.getId())
                        .build());
                } catch (Exception e) {
                    failed.set(true);
                    e.printStackTrace(System.err);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        doneLatch.await();
        executor.shutdown();

        Assert.assertFalse(failed.get());

        var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(createdWhiteboard.getId()).build();
        var whiteboard = externalUserWhiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.FINALIZED, whiteboard.getStatus());
        whiteboard.getFieldsList().forEach(field -> {
            assertEquals(LWB.WhiteboardField.Status.FINALIZED, field.getStatus());
        });

        final var finalizedFields = whiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getInfo().getName(), f -> f));
        assertEquals(NONESTATE, finalizedFields.get("f1").getInfo().getStateCase());
        assertEquals(NONESTATE, finalizedFields.get("f2").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, finalizedFields.get("f3").getInfo().getStateCase());
        assertEquals(LINKEDSTATE, finalizedFields.get("f4").getInfo().getStateCase());
        assertEquals("s-uri-4-init", finalizedFields.get("f4").getInfo().getLinkedState().getStorageUri());
    }

    private void apiAccessTest(LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub client,
                               Status expectedStatus)
    {
        try {
            client.createWhiteboard(LWBS.CreateWhiteboardRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
        try {
            client.linkField(LWBS.LinkFieldRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
        try {
            client.finalizeWhiteboard(LWBS.FinalizeWhiteboardRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
        try {
            client.get(LWBS.GetRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
        try {
            client.list(LWBS.ListRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
    }

    private LWBS.CreateWhiteboardRequest genCreateWhiteboardRequest() {
        return LWBS.CreateWhiteboardRequest.newBuilder()
            .setWhiteboardName("wb-name")
            .addAllFields(List.of(
                LWB.WhiteboardFieldInfo.newBuilder()
                    .setName("f1")
                    .setNoneState(LWB.WhiteboardFieldInfo.NoneField.getDefaultInstance())
                    .build(),
                LWB.WhiteboardFieldInfo.newBuilder()
                    .setName("f2")
                    .setNoneState(LWB.WhiteboardFieldInfo.NoneField.getDefaultInstance())
                    .build(),
                LWB.WhiteboardFieldInfo.newBuilder()
                    .setName("f3")
                    .setLinkedState(LWB.WhiteboardFieldInfo.LinkedField.newBuilder()
                        .setStorageUri("s-uri-3")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build())
                    .build(),
                LWB.WhiteboardFieldInfo.newBuilder()
                    .setName("f4")
                    .setLinkedState(LWB.WhiteboardFieldInfo.LinkedField.newBuilder()
                        .setStorageUri("s-uri-4-init")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build())
                    .build()))
            .setStorage(ProtoConverter.toProto(new Whiteboard.Storage("storage", "")))
            .addAllTags(List.of("t1, t2"))
            .setNamespace("namespace")
            .build();
    }

    private LWBS.CreateWhiteboardRequest genCreateWhiteboardRequest(String name, List<String> tags) {
        return LWBS.CreateWhiteboardRequest.newBuilder()
            .setWhiteboardName(name)
            .addAllFields(List.of(
                LWB.WhiteboardFieldInfo.newBuilder()
                    .setName("f")
                    .setLinkedState(LWB.WhiteboardFieldInfo.LinkedField.newBuilder()
                        .setStorageUri("s-uri")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build())
                    .build()))
            .setStorage(ProtoConverter.toProto(new Whiteboard.Storage("storage", "")))
            .addAllTags(tags)
            .setNamespace("namespace")
            .build();
    }

    private record User(String id, JwtCredentials credentials) {
    }

    public static class IamClient implements AutoCloseable {

        private final ManagedChannel channel;
        private final SubjectServiceClient subjectClient;

        IamClient(IamClientConfiguration config) {
            this.channel = GrpcUtils.newGrpcChannel(config.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
            var iamToken = config.createRenewableToken();
            this.subjectClient = new SubjectServiceGrpcClient("TestClient", channel, iamToken::get);
        }

        public User createUser(String name) throws Exception {
            var login = "github-" + name;
            var creds = generateCredentials(login, "GITHUB");

            var subj = subjectClient.createSubject(AuthProvider.GITHUB, login, SubjectType.USER,
                new SubjectCredentials("main", creds.publicKey(), CredentialsType.PUBLIC_KEY));

            return new User(subj.id(), creds.credentials());
        }

        @Override
        public void close() {
            channel.shutdown();
        }
    }

    public record GeneratedCredentials(
        String publicKey,
        JwtCredentials credentials
    ) {}

    public static GeneratedCredentials generateCredentials(String login, String provider)
        throws IOException, InterruptedException, NoSuchAlgorithmException, InvalidKeySpecException
    {
        final var keys = RsaUtils.generateRsaKeys();
        var from = Date.from(Instant.now());
        var till = JwtUtils.afterDays(7);
        var credentials = new JwtCredentials(JwtUtils.buildJWT(login, provider, from, till,
            CredentialsUtils.readPrivateKey(keys.privateKey())));

        final var publicKey = keys.publicKey();

        return new GeneratedCredentials(publicKey, credentials);
    }
}
