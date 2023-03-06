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
import ai.lzy.test.IdempotencyUtils.TestScenario;
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
import ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub;
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
import java.net.URI;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static ai.lzy.model.grpc.ProtoConverter.toProto;
import static ai.lzy.test.IdempotencyUtils.processIdempotentCallsConcurrently;
import static ai.lzy.test.IdempotencyUtils.processIdempotentCallsSequentially;
import static org.junit.Assert.*;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ApiTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });

    private ApplicationContext context;
    private LzyWhiteboardServiceBlockingStub externalUserWhiteboardClient;
    private LzyWhiteboardServiceBlockingStub externalUser2WhiteboardClient;
    private LzyWhiteboardServiceBlockingStub whiteboardClient;
    private Server whiteboardServer;
    private ManagedChannel channel;

    @Before
    public void setUp() throws Exception {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("whiteboard", db.getConnectionInfo()));
        var config = context.getBean(AppConfig.class);
        config.getIam().setAddress("localhost:" + super.getPort());
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

        context.getBean(WhiteboardDataSource.class).setOnClose(DatabaseTestUtils::cleanup);
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
    public void registerAndGetWhiteboard() {
        try {
            externalUserWhiteboardClient.get(LWBS.GetRequest.newBuilder().setWhiteboardId("some_wb_id").build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());
        }

        final LWBS.RegisterWhiteboardRequest request =
            genCreateWhiteboardRequest(UUID.randomUUID().toString(), Instant.now().truncatedTo(ChronoUnit.MILLIS));
        externalUserWhiteboardClient.registerWhiteboard(request);

        final var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(request.getWhiteboard().getId()).build();
        final var getResponse = whiteboardClient.get(getRequest);
        assertEquals(request.getWhiteboard(), getResponse.getWhiteboard());

        final var getUserResponse = externalUserWhiteboardClient.get(getRequest);
        assertEquals(request.getWhiteboard(), getUserResponse.getWhiteboard());

        try {
            externalUser2WhiteboardClient.get(getRequest);
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());
        }
    }

    @Test
    public void registerInvalidArgs() {
        StatusRuntimeException sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName("name")
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName("name")
                        .addAllFields(List.of(
                            LWB.WhiteboardField.newBuilder()
                                .setName("f1")
                                .setScheme(toProto(DataScheme.PLAIN))
                                .build()))
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName("name")
                        .addAllFields(List.of(
                            LWB.WhiteboardField.newBuilder()
                                .setName("f1")
                                .setScheme(toProto(DataScheme.PLAIN))
                                .build()))
                        .setNamespace("ns")
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName("name")
                        .addAllFields(List.of(
                            LWB.WhiteboardField.newBuilder()
                                .setName("f1")
                                .setScheme(toProto(DataScheme.PLAIN))
                                .build()))
                        .setNamespace("ns")
                        .setStorage(LWB.Storage.newBuilder().setName("storage").build())
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName("name")
                        .addAllFields(List.of(
                            LWB.WhiteboardField.newBuilder()
                                .setName("f1")
                                .setScheme(toProto(DataScheme.PLAIN))
                                .build()))
                        .setNamespace("ns")
                        .setStorage(LWB.Storage.newBuilder().setName("storage").setUri("s3://uri").build())
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName("name")
                        .addAllFields(List.of(
                            LWB.WhiteboardField.newBuilder()
                                .setName("f1")
                                .setScheme(toProto(DataScheme.PLAIN))
                                .build()))
                        .setNamespace("ns")
                        .setStorage(LWB.Storage.newBuilder().setName("storage").setUri("s3://uri").build())
                        .setCreatedAt(ai.lzy.util.grpc.ProtoConverter.toProto(Instant.now()))
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName("name")
                        .addAllFields(List.of(
                            LWB.WhiteboardField.newBuilder()
                                .setScheme(toProto(DataScheme.PLAIN))
                                .build()))
                        .setNamespace("ns")
                        .setStorage(LWB.Storage.newBuilder().setName("storage").setUri("s3://uri").build())
                        .setCreatedAt(ai.lzy.util.grpc.ProtoConverter.toProto(Instant.now()))
                        .setStatus(LWB.Whiteboard.Status.FINALIZED)
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);

        sre = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.registerWhiteboard(LWBS.RegisterWhiteboardRequest.newBuilder().setWhiteboard(
                    LWB.Whiteboard.newBuilder()
                        .setId(UUID.randomUUID().toString())
                        .setName("name")
                        .addAllFields(List.of(
                            LWB.WhiteboardField.newBuilder()
                                .setName("name")
                                .build()))
                        .setNamespace("ns")
                        .setStorage(LWB.Storage.newBuilder().setName("storage").setUri("s3://uri").build())
                        .setCreatedAt(ai.lzy.util.grpc.ProtoConverter.toProto(Instant.now()))
                        .setStatus(LWB.Whiteboard.Status.FINALIZED)
                        .build())
                .build()));
        assertEquals(sre.getStatus().getCode(), Status.Code.INVALID_ARGUMENT);
    }

    @Test
    public void updateWhiteboardAllFields() {
        final String id = UUID.randomUUID().toString();
        final Instant createdAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final LWBS.RegisterWhiteboardRequest whiteboardRequest = genCreateWhiteboardRequest(id, createdAt);
        whiteboardClient.registerWhiteboard(whiteboardRequest);

        var getRequest =
            LWBS.GetRequest.newBuilder().setWhiteboardId(whiteboardRequest.getWhiteboard().getId()).build();
        var whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.CREATED, whiteboard.getStatus());
        assertEquals("wb-name", whiteboard.getName());
        HashSet<String> tags = new HashSet<>(whiteboard.getTagsList());
        assertEquals(2, tags.size());
        assertTrue(tags.contains("t1"));
        assertTrue(tags.contains("t2"));
        assertEquals(createdAt.truncatedTo(ChronoUnit.MILLIS),
            ai.lzy.util.grpc.ProtoConverter.fromProto(whiteboard.getCreatedAt()).truncatedTo(ChronoUnit.MILLIS));
        assertEquals("storage", whiteboard.getStorage().getName());
        assertEquals("description", whiteboard.getStorage().getDescription());
        assertEquals("s3://uri", whiteboard.getStorage().getUri());
        assertEquals("namespace", whiteboard.getNamespace());

        var fields = whiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(LWB.WhiteboardField::getName, f -> f));
        assertEquals(4, whiteboard.getFieldsList().size());
        assertTrue(fields.containsKey("f1"));
        assertTrue(fields.containsKey("f2"));
        assertTrue(fields.containsKey("f3"));
        assertTrue(fields.containsKey("f4"));

        final Instant newCreatedAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        whiteboardClient.updateWhiteboard(LWBS.UpdateWhiteboardRequest.newBuilder()
            .setWhiteboard(LWB.Whiteboard.newBuilder()
                .setId(id)
                .setName("new_name")
                .setStatus(LWB.Whiteboard.Status.FINALIZED)
                .setCreatedAt(ai.lzy.util.grpc.ProtoConverter.toProto(newCreatedAt))
                .setNamespace("new_namespace")
                .setStorage(
                    LWB.Storage.newBuilder().setName("new_storage").setDescription("new_description").setUri("s3://new")
                        .build())
                .addAllFields(List.of(
                    LWB.WhiteboardField.newBuilder()
                        .setName("f5")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build()))
                .addAllTags(List.of("t3", "t4"))
                .build())
            .build());

        getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(whiteboardRequest.getWhiteboard().getId()).build();
        whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.FINALIZED, whiteboard.getStatus());
        assertEquals("new_name", whiteboard.getName());
        tags = new HashSet<>(whiteboard.getTagsList());
        assertEquals(2, tags.size());
        assertTrue(tags.contains("t3"));
        assertTrue(tags.contains("t4"));
        assertEquals(newCreatedAt.truncatedTo(ChronoUnit.MILLIS),
            ai.lzy.util.grpc.ProtoConverter.fromProto(whiteboard.getCreatedAt()).truncatedTo(ChronoUnit.MILLIS));
        assertEquals("new_storage", whiteboard.getStorage().getName());
        assertEquals("new_description", whiteboard.getStorage().getDescription());
        assertEquals("s3://new", whiteboard.getStorage().getUri());
        assertEquals("new_namespace", whiteboard.getNamespace());

        fields = whiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(LWB.WhiteboardField::getName, f -> f));
        assertEquals(1, whiteboard.getFieldsList().size());
        assertTrue(fields.containsKey("f5"));
    }

    @Test
    public void updateNonexistentWhiteboard() {
        final StatusRuntimeException statusRuntimeException = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.updateWhiteboard(LWBS.UpdateWhiteboardRequest.newBuilder().setWhiteboard(
                LWB.Whiteboard.newBuilder().setId(UUID.randomUUID().toString()).build()).build()));
        assertTrue(statusRuntimeException.getMessage().startsWith("NOT_FOUND"));
    }

    @Test
    public void updateNoWhiteboardId() {
        final StatusRuntimeException statusRuntimeException = Assert.assertThrows(StatusRuntimeException.class,
            () -> whiteboardClient.updateWhiteboard(LWBS.UpdateWhiteboardRequest.newBuilder().setWhiteboard(
                LWB.Whiteboard.newBuilder().build()).build()));
        assertTrue(statusRuntimeException.getMessage().startsWith("INVALID_ARGUMENT: whiteboard ID is empty"));
    }

    @Test
    public void updateWhiteboardOnlyStatus() {
        final String id = UUID.randomUUID().toString();
        final Instant createdAt = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final LWBS.RegisterWhiteboardRequest whiteboardRequest = genCreateWhiteboardRequest(id, createdAt);
        whiteboardClient.registerWhiteboard(whiteboardRequest);

        var getRequest =
            LWBS.GetRequest.newBuilder().setWhiteboardId(whiteboardRequest.getWhiteboard().getId()).build();
        var whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.CREATED, whiteboard.getStatus());
        assertEquals("wb-name", whiteboard.getName());
        HashSet<String> tags = new HashSet<>(whiteboard.getTagsList());
        assertEquals(2, tags.size());
        assertTrue(tags.contains("t1"));
        assertTrue(tags.contains("t2"));
        assertEquals(createdAt.truncatedTo(ChronoUnit.MILLIS),
            ai.lzy.util.grpc.ProtoConverter.fromProto(whiteboard.getCreatedAt()).truncatedTo(ChronoUnit.MILLIS));
        assertEquals("storage", whiteboard.getStorage().getName());
        assertEquals("description", whiteboard.getStorage().getDescription());
        assertEquals("s3://uri", whiteboard.getStorage().getUri());
        assertEquals("namespace", whiteboard.getNamespace());

        var fields = whiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(LWB.WhiteboardField::getName, f -> f));
        assertEquals(4, whiteboard.getFieldsList().size());
        assertTrue(fields.containsKey("f1"));
        assertTrue(fields.containsKey("f2"));
        assertTrue(fields.containsKey("f3"));
        assertTrue(fields.containsKey("f4"));

        whiteboardClient.updateWhiteboard(LWBS.UpdateWhiteboardRequest.newBuilder()
            .setWhiteboard(LWB.Whiteboard.newBuilder()
                .setId(id)
                .setStatus(LWB.Whiteboard.Status.FINALIZED)
                .build())
            .build());

        getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(whiteboardRequest.getWhiteboard().getId()).build();
        whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        assertEquals(LWB.Whiteboard.Status.FINALIZED, whiteboard.getStatus());
        assertEquals("wb-name", whiteboard.getName());
        tags = new HashSet<>(whiteboard.getTagsList());
        assertEquals(2, tags.size());
        assertTrue(tags.contains("t1"));
        assertTrue(tags.contains("t2"));
        assertEquals(createdAt.truncatedTo(ChronoUnit.MILLIS),
            ai.lzy.util.grpc.ProtoConverter.fromProto(whiteboard.getCreatedAt()).truncatedTo(ChronoUnit.MILLIS));
        assertEquals("storage", whiteboard.getStorage().getName());
        assertEquals("description", whiteboard.getStorage().getDescription());
        assertEquals("s3://uri", whiteboard.getStorage().getUri());
        assertEquals("namespace", whiteboard.getNamespace());

        fields = whiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(LWB.WhiteboardField::getName, f -> f));
        assertEquals(4, whiteboard.getFieldsList().size());
        assertTrue(fields.containsKey("f1"));
        assertTrue(fields.containsKey("f2"));
        assertTrue(fields.containsKey("f3"));
        assertTrue(fields.containsKey("f4"));
    }

    @Test
    public void listWhiteboards() {
        final LWBS.RegisterWhiteboardRequest whiteboardRequest1 = genCreateWhiteboardRequest("wb1", List.of("t1"));
        externalUserWhiteboardClient.registerWhiteboard(whiteboardRequest1);
        final LWBS.RegisterWhiteboardRequest whiteboardRequest2 = genCreateWhiteboardRequest("wb2", List.of("t2"));
        externalUserWhiteboardClient.registerWhiteboard(whiteboardRequest2);
        final LWBS.RegisterWhiteboardRequest whiteboardRequest3 =
            genCreateWhiteboardRequest("wb3", List.of("t1", "t2"));
        externalUserWhiteboardClient.registerWhiteboard(whiteboardRequest3);
        final LWBS.RegisterWhiteboardRequest whiteboardRequest4 = genCreateWhiteboardRequest("wb", List.of("t1", "t2"));
        externalUser2WhiteboardClient.registerWhiteboard(whiteboardRequest4);

        var listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder().build());
        assertEquals(3, listResult.getWhiteboardsCount());

        listResult = externalUser2WhiteboardClient.list(LWBS.ListRequest.newBuilder().build());
        assertEquals(1, listResult.getWhiteboardsCount());
        assertEquals(whiteboardRequest4.getWhiteboard(), listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder()
            .addAllTags(List.of("t1", "t2")).build());
        assertEquals(1, listResult.getWhiteboardsCount());
        assertEquals(whiteboardRequest3.getWhiteboard(), listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder().setName("wb1").build());
        assertEquals(1, listResult.getWhiteboardsCount());
        assertEquals(whiteboardRequest1.getWhiteboard(), listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder()
            .setCreatedTimeBounds(LWB.TimeBounds.newBuilder().setTo(whiteboardRequest2.getWhiteboard().getCreatedAt())
                .build()).build());
        assertEquals(2, listResult.getWhiteboardsCount());
    }


    @Test
    public void createWhiteboardIdempotency() {
        processIdempotentCallsSequentially(createWbScenario());
    }

    @Test
    public void updateWhiteboardIdempotency() {
        processIdempotentCallsSequentially(updateWbScenario());
    }

    @Test
    public void idempotentCreateWbConcurrent() throws InterruptedException {
        processIdempotentCallsConcurrently(createWbScenario());
    }

    @Test
    public void idempotentUpdateWbConcurrent() throws InterruptedException {
        processIdempotentCallsConcurrently(updateWbScenario());
    }

    private TestScenario<LzyWhiteboardServiceBlockingStub, Void, LWB.Whiteboard> createWbScenario() {
        final String id = UUID.randomUUID().toString();
        final Instant ts = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        return new TestScenario<>(whiteboardClient,
            stub -> null,
            (stub, nothing) -> registerWhiteboard(stub, id, ts),
            wb -> {
                assertEquals(LWB.Whiteboard.Status.CREATED, wb.getStatus());
                assertEquals(4, wb.getFieldsCount());
                wb.getFieldsList().forEach(field ->
                    assertEquals(toProto(DataScheme.PLAIN), field.getScheme()));

                var fields = wb.getFieldsList().stream()
                    .collect(Collectors.toMap(LWB.WhiteboardField::getName, f -> f));
                assertTrue(fields.containsKey("f1"));
                assertTrue(fields.containsKey("f2"));
                assertTrue(fields.containsKey("f3"));
                assertTrue(fields.containsKey("f4"));
            });
    }

    private TestScenario<LzyWhiteboardServiceBlockingStub, LWB.Whiteboard, LWB.Whiteboard> updateWbScenario() {
        final String id = UUID.randomUUID().toString();
        return new TestScenario<>(whiteboardClient,
            (client) -> registerWhiteboard(client, id, Instant.now().truncatedTo(ChronoUnit.MILLIS)),
            (stub, input) -> {
                stub.updateWhiteboard(LWBS.UpdateWhiteboardRequest.newBuilder()
                    .setWhiteboard(LWB.Whiteboard.newBuilder()
                        .setId(id)
                        .setStatus(LWB.Whiteboard.Status.FINALIZED)
                        .build())
                    .build());
                var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(input.getId()).build();
                return whiteboardClient.get(getRequest).getWhiteboard();
            },
            whiteboard -> assertEquals(LWB.Whiteboard.Status.FINALIZED, whiteboard.getStatus()));
    }

    private void apiAccessTest(LzyWhiteboardServiceBlockingStub client,
                               Status expectedStatus)
    {
        try {
            client.registerWhiteboard(LWBS.RegisterWhiteboardRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
        try {
            client.updateWhiteboard(LWBS.UpdateWhiteboardRequest.getDefaultInstance());
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

    private static LWB.Whiteboard registerWhiteboard(LzyWhiteboardServiceBlockingStub client, String id, Instant ts) {
        final LWBS.RegisterWhiteboardRequest request = genCreateWhiteboardRequest(id, ts);
        client.registerWhiteboard(request);
        return request.getWhiteboard();
    }

    private static LWBS.RegisterWhiteboardRequest genCreateWhiteboardRequest(String id, Instant ts) {
        return LWBS.RegisterWhiteboardRequest.newBuilder()
            .setWhiteboard(LWB.Whiteboard.newBuilder()
                .setName("wb-name")
                .addAllFields(List.of(
                    LWB.WhiteboardField.newBuilder()
                        .setName("f1")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build(),
                    LWB.WhiteboardField.newBuilder()
                        .setName("f2")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build(),
                    LWB.WhiteboardField.newBuilder()
                        .setName("f3")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build(),
                    LWB.WhiteboardField.newBuilder()
                        .setName("f4")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build()))
                .setStorage(
                    ProtoConverter.toProto(new Whiteboard.Storage("storage", "description", URI.create("s3://uri"))))
                .addAllTags(List.of("t1", "t2"))
                .setNamespace("namespace")
                .setCreatedAt(ai.lzy.util.grpc.ProtoConverter.toProto(ts))
                .setId(id)
                .setStatus(LWB.Whiteboard.Status.CREATED)
                .build())
            .build();
    }

    private static LWBS.RegisterWhiteboardRequest genCreateWhiteboardRequest(String name, List<String> tags) {
        return LWBS.RegisterWhiteboardRequest.newBuilder()
            .setWhiteboard(LWB.Whiteboard.newBuilder()
                .addAllFields(List.of(
                    LWB.WhiteboardField.newBuilder()
                        .setName("f")
                        .setScheme(toProto(DataScheme.PLAIN))
                        .build()))
                .setStorage(ProtoConverter.toProto(new Whiteboard.Storage("storage", "", URI.create("s3://uri"))))
                .addAllTags(tags)
                .setNamespace("namespace")
                .setName(name)
                .setId(UUID.randomUUID().toString())
                .setCreatedAt(ai.lzy.util.grpc.ProtoConverter.toProto(Instant.now().truncatedTo(ChronoUnit.MILLIS)))
                .setStatus(LWB.Whiteboard.Status.CREATED)
                .build())
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
    )
    {
    }

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
