package ai.lzy.whiteboard;

import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.LWBPS;
import ai.lzy.v1.LWBS;
import ai.lzy.v1.LzyWhiteboardPrivateServiceGrpc;
import ai.lzy.v1.LzyWhiteboardServiceGrpc;
import ai.lzy.whiteboard.storage.WhiteboardDataSource;
import com.google.common.net.HostAndPort;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class ApiTest extends BaseTestWithIam {

    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {});
    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(ds -> {});

    private ApplicationContext context;
    private LzyWhiteboardServiceGrpc.LzyWhiteboardServiceBlockingStub whiteboardClient;
    private LzyWhiteboardPrivateServiceGrpc.LzyWhiteboardPrivateServiceBlockingStub privateWhiteboardClient;
    private WhiteboardApp whiteboardApp;
    private ManagedChannel channel;

    @Before
    public void setUp() throws IOException {
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
        whiteboardClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
        privateWhiteboardClient = LzyWhiteboardPrivateServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, credentials::token));
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

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testUnauthenticated() {
        final var unauthorizedWhiteboardClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel);
        try {
            unauthorizedWhiteboardClient.get(LWBS.GetRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            unauthorizedWhiteboardClient.list(LWBS.ListRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }

        final var unauthorizedWhiteboardPrivateClient = LzyWhiteboardPrivateServiceGrpc.newBlockingStub(channel);
        try {
            unauthorizedWhiteboardPrivateClient.create(LWBPS.CreateRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            unauthorizedWhiteboardPrivateClient.linkField(LWBPS.LinkFieldRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
        try {
            unauthorizedWhiteboardPrivateClient.finalize(LWBPS.FinalizeRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode());
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testPermissionDenied() {
        final var unauthorizedWhiteboardClient = LzyWhiteboardServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(
                GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user")::token
            ));
        try {
            unauthorizedWhiteboardClient.get(LWBS.GetRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            unauthorizedWhiteboardClient.list(LWBS.ListRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }

        final var unauthorizedWhiteboardPrivateClient = LzyWhiteboardPrivateServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(
                GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user")::token
            ));
        try {
            unauthorizedWhiteboardPrivateClient.create(LWBPS.CreateRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            unauthorizedWhiteboardPrivateClient.linkField(LWBPS.LinkFieldRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
        try {
            unauthorizedWhiteboardPrivateClient.finalize(LWBPS.FinalizeRequest.getDefaultInstance());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            Assert.assertEquals(e.getStatus().toString(), Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode());
        }
    }

}
