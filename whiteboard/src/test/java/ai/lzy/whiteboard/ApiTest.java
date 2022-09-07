package ai.lzy.whiteboard;

import static ai.lzy.v1.whiteboard.LWB.WhiteboardFieldInfo.StateCase.LINKEDSTATE;
import static ai.lzy.v1.whiteboard.LWB.WhiteboardFieldInfo.StateCase.NONESTATE;

import ai.lzy.iam.clients.SubjectServiceClient;
import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.data.DataSchema;
import ai.lzy.model.data.types.SchemeType;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.whiteboard.LWB;
import ai.lzy.v1.whiteboard.LWBPS;
import ai.lzy.v1.whiteboard.LWBS;
import ai.lzy.v1.whiteboard.LzyWhiteboardPrivateServiceGrpc;
import ai.lzy.v1.whiteboard.LzyWhiteboardServiceGrpc;
import ai.lzy.whiteboard.grpc.ProtoConverter;
import ai.lzy.whiteboard.model.Whiteboard;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class ApiTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private User externalUser;
    private User externalUser2;
    private LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub externalUserWhiteboardClient;
    private LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub externalUser2WhiteboardClient;
    private LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub whiteboardClient;
    private LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceBlockingStub privateWhiteboardClient;
    private WhiteboardApp whiteboardApp;
    private ManagedChannel channel;

    @Before
    public void setUp() throws Exception {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));

        context = ApplicationContext.run(DatabaseTestUtils.preparePostgresConfig("whiteboard", db.getConnectionInfo()));

        whiteboardApp = context.getBean(WhiteboardApp.class);
        whiteboardApp.start();

        final var config = context.getBean(AppConfig.class);
        //noinspection UnstableApiUsage
        channel = ChannelBuilder
            .forAddress(HostAndPort.fromString(config.getAddress()))
            .usePlaintext()
            .build();
        var credentials = config.getIam().createCredentials();
        privateWhiteboardClient = LzyWhiteboardPrivateServiceGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
        whiteboardClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel).withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));

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
        whiteboardApp.stop();
        try {
            whiteboardApp.awaitTermination();
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
        final var unauthorizedPrivateClient = LzyWhiteboardPrivateServiceGrpc.newBlockingStub(channel);
        apiAccessTest(unauthorizedPrivateClient, Status.UNAUTHENTICATED);

        final var unauthorizedClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel);
        apiAccessTest(unauthorizedClient, Status.UNAUTHENTICATED);
    }

    @Test
    public void testPermissionDenied() {
        final var invalidCredsPrivateClient = LzyWhiteboardPrivateServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(
                GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user")::token
            ));
        apiAccessTest(invalidCredsPrivateClient, Status.PERMISSION_DENIED);

        final var userCredsPrivateClient = LzyWhiteboardPrivateServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(
                GrpcHeaders.AUTHORIZATION, externalUser.credentials::token
            ));
        apiAccessTest(userCredsPrivateClient, Status.PERMISSION_DENIED);

        final var invalidCredsClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(
                GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user")::token
            ));
        apiAccessTest(invalidCredsClient, Status.PERMISSION_DENIED);
    }

    @Test
    public void createAndGetWhiteboard() {
        try {
            externalUserWhiteboardClient.get(LWBS.GetRequest.newBuilder().setWhiteboardId("some_wb_id").build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());
        }

        final var createdWhiteboard = privateWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();
        final var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(createdWhiteboard.getId()).build();

        Assert.assertEquals(LWB.Whiteboard.Status.CREATED, createdWhiteboard.getStatus());
        Assert.assertEquals(4, createdWhiteboard.getFieldsCount());
        createdWhiteboard.getFieldsList().forEach(field -> {
            Assert.assertEquals(LWB.WhiteboardField.Status.CREATED, field.getStatus());
        });
        final var fields = createdWhiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getInfo().getName(), f -> f));
        Assert.assertEquals(NONESTATE, fields.get("f1").getInfo().getStateCase());
        Assert.assertEquals(NONESTATE, fields.get("f2").getInfo().getStateCase());
        Assert.assertEquals(LINKEDSTATE, fields.get("f3").getInfo().getStateCase());
        Assert.assertEquals(LINKEDSTATE, fields.get("f4").getInfo().getStateCase());

        final var getResponse = whiteboardClient.get(getRequest);
        Assert.assertEquals(createdWhiteboard, getResponse.getWhiteboard());

        final var getUserResponse = externalUserWhiteboardClient.get(getRequest);
        Assert.assertEquals(createdWhiteboard, getUserResponse.getWhiteboard());

        try {
            externalUser2WhiteboardClient.get(getRequest);
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.Code.NOT_FOUND, e.getStatus().getCode());
        }
    }

    @Test
    public void finalizeWhiteboard() {
        final var createdWhiteboard = privateWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest()).getWhiteboard();
        final var getRequest = LWBS.GetRequest.newBuilder().setWhiteboardId(createdWhiteboard.getId()).build();

        privateWhiteboardClient.finalizeField(LWBPS.FinalizeFieldRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .setFieldName("f1")
            .setStorageUri("s-uri-1")
            .setScheme(ai.lzy.model.GrpcConverter.to(new DataSchema(SchemeType.plain, "default")))
            .build());

        privateWhiteboardClient.finalizeField(LWBPS.FinalizeFieldRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .setFieldName("f4")
            .setStorageUri("s-uri-4")
            .setScheme(ai.lzy.model.GrpcConverter.to(new DataSchema(SchemeType.plain, "default")))
            .build());

        var whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        Assert.assertEquals(LWB.Whiteboard.Status.CREATED, whiteboard.getStatus());
        whiteboard.getFieldsList().forEach(field -> {
            if (field.getInfo().getName().equals("f1") || field.getInfo().getName().equals("f4")) {
                Assert.assertEquals(LWB.WhiteboardField.Status.FINALIZED, field.getStatus());
            } else {
                Assert.assertEquals(LWB.WhiteboardField.Status.CREATED, field.getStatus());
            }
        });

        privateWhiteboardClient.finalizeWhiteboard(LWBPS.FinalizeWhiteboardRequest.newBuilder()
            .setWhiteboardId(createdWhiteboard.getId())
            .build());

        whiteboard = whiteboardClient.get(getRequest).getWhiteboard();
        Assert.assertEquals(LWB.Whiteboard.Status.FINALIZED, whiteboard.getStatus());
        whiteboard.getFieldsList().forEach(field -> {
            Assert.assertEquals(LWB.WhiteboardField.Status.FINALIZED, field.getStatus());
        });

        final var finalizedFields = whiteboard.getFieldsList().stream()
            .collect(Collectors.toMap(f -> f.getInfo().getName(), f -> f));
        Assert.assertEquals(LINKEDSTATE, finalizedFields.get("f1").getInfo().getStateCase());
        Assert.assertEquals(NONESTATE, finalizedFields.get("f2").getInfo().getStateCase());
        Assert.assertEquals(LINKEDSTATE, finalizedFields.get("f3").getInfo().getStateCase());
        Assert.assertEquals(LINKEDSTATE, finalizedFields.get("f4").getInfo().getStateCase());
        Assert.assertEquals("s-uri-4", finalizedFields.get("f4").getInfo().getLinkedState().getStorageUri());
    }

    @Test
    public void listWhiteboards() {
        final var whiteboard1 = privateWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest(externalUser.id, "wb1", List.of("t1"))).getWhiteboard();
        final var whiteboard2 = privateWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest(externalUser.id, "wb2", List.of("t2"))).getWhiteboard();
        final var whiteboard3 = privateWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest(externalUser.id, "wb3", List.of("t1", "t2"))).getWhiteboard();
        final var whiteboard4 = privateWhiteboardClient
            .createWhiteboard(genCreateWhiteboardRequest(externalUser2.id, "wb", List.of("t1", "t2"))).getWhiteboard();

        var listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder().build());
        Assert.assertEquals(3, listResult.getWhiteboardsCount());

        listResult = externalUser2WhiteboardClient.list(LWBS.ListRequest.newBuilder().build());
        Assert.assertEquals(1, listResult.getWhiteboardsCount());
        Assert.assertEquals(whiteboard4, listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder()
            .addAllTags(List.of("t1", "t2")).build());
        Assert.assertEquals(1, listResult.getWhiteboardsCount());
        Assert.assertEquals(whiteboard3, listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder().setName("wb1").build());
        Assert.assertEquals(1, listResult.getWhiteboardsCount());
        Assert.assertEquals(whiteboard1, listResult.getWhiteboards(0));

        listResult = externalUserWhiteboardClient.list(LWBS.ListRequest.newBuilder()
            .setCreatedTimeBounds(LWB.TimeBounds.newBuilder().setTo(whiteboard2.getCreatedAt()).build()).build());
        Assert.assertEquals(2, listResult.getWhiteboardsCount());
    }

    private void apiAccessTest(LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceBlockingStub client,
                               Status expectedStatus)
    {
        try {
            client.createWhiteboard(LWBPS.CreateWhiteboardRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
        try {
            client.finalizeField(LWBPS.FinalizeFieldRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
        try {
            client.finalizeWhiteboard(LWBPS.FinalizeWhiteboardRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
    }

    private void apiAccessTest(LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub client,
                               Status expectedStatus)
    {
        try {
            client.get(LWBS.GetRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
        try {
            client.list(LWBS.ListRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), expectedStatus.getCode(), e.getStatus().getCode());
        }
    }

    private LWBPS.CreateWhiteboardRequest genCreateWhiteboardRequest() {
        return LWBPS.CreateWhiteboardRequest.newBuilder()
            .setUserId(externalUser.id)
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
                        .setScheme(ai.lzy.model.GrpcConverter.to(new DataSchema(SchemeType.plain, "default")))
                        .build())
                    .build(),
                LWB.WhiteboardFieldInfo.newBuilder()
                    .setName("f4")
                    .setLinkedState(LWB.WhiteboardFieldInfo.LinkedField.newBuilder()
                        .setStorageUri("s-uri-4-init")
                        .setScheme(ai.lzy.model.GrpcConverter.to(new DataSchema(SchemeType.plain, "default")))
                        .build())
                    .build()))
            .setStorage(ProtoConverter.toProto(new Whiteboard.Storage("storage", "")))
            .addAllTags(List.of("t1, t2"))
            .setNamespace("namespace")
            .build();
    }

    private LWBPS.CreateWhiteboardRequest genCreateWhiteboardRequest(String userId, String name, List<String> tags) {
        return LWBPS.CreateWhiteboardRequest.newBuilder()
            .setUserId(userId)
            .setWhiteboardName(name)
            .addAllFields(List.of(
                LWB.WhiteboardFieldInfo.newBuilder()
                    .setName("f")
                    .setLinkedState(LWB.WhiteboardFieldInfo.LinkedField.newBuilder()
                        .setStorageUri("s-uri")
                        .setScheme(ai.lzy.model.GrpcConverter.to(new DataSchema(SchemeType.plain, "default")))
                        .build())
                    .build()))
            .setStorage(ProtoConverter.toProto(new Whiteboard.Storage("storage", "")))
            .addAllTags(tags)
            .setNamespace("namespace")
            .build();
    }

    private record User(String id, JwtCredentials credentials) { }

    public static class IamClient implements AutoCloseable {

        private final ManagedChannel channel;
        private final SubjectServiceClient subjectClient;

        IamClient(IamClientConfiguration config) {
            this.channel = ChannelBuilder
                .forAddress(config.getAddress())
                .usePlaintext()
                .enableRetry(LzyAuthenticateServiceGrpc.SERVICE_NAME)
                .build();
            this.subjectClient = new SubjectServiceGrpcClient(channel, config::createCredentials);
        }

        public User createUser(String name) throws Exception
        {
            var subj = subjectClient.createSubject("github", "github-" + name, SubjectType.USER);
            var creds = JwtUtils.generateCredentials(subj.id());
            subjectClient.addCredentials(subj, "main", creds.publicKey(), creds.credentials().type());
            return new User(subj.id(), creds.credentials());
        }

        @Override
        public void close() {
            channel.shutdown();
        }
    }

}
