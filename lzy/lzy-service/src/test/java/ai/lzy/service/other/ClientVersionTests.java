package ai.lzy.service.other;

import ai.lzy.service.IamOnlyLzyContextTests;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Test;

import static ai.lzy.service.IamUtils.authorize;
import static org.junit.Assert.*;

public class ClientVersionTests extends IamOnlyLzyContextTests {
    private static final String USER_NAME = "test-user-1";
    private LzyWorkflowServiceBlockingStub authLzyClient;

    @Before
    public void before() throws Exception {
        authLzyClient = authorize(lzyClient, USER_NAME, iamClient);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testClientVersion() {
        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(false);

        var client = authLzyClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.CLIENT_VERSION, () -> "pylzy=1.0.0")  // unsupported pylzy
        );

        assertThrows(StatusRuntimeException.class,
            () -> client.startWorkflow(LWFS.StartWorkflowRequest.newBuilder().build()));

        var client1 = authLzyClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.CLIENT_VERSION, () -> "keklol=1.0.0")  // unsupported client
        );

        assertThrows(StatusRuntimeException.class,
            () -> client1.startWorkflow(LWFS.StartWorkflowRequest.newBuilder().build()));

        var client2 = authLzyClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.CLIENT_VERSION, () -> "pylzy=1.100")  // wrong version format
        );

        assertThrows(StatusRuntimeException.class,
            () -> client2.startWorkflow(LWFS.StartWorkflowRequest.newBuilder().build()));

        var client3 = authLzyClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.CLIENT_VERSION, () -> "askfj;o.a=kds.jv.of")  // wrong format
        );

        assertThrows(StatusRuntimeException.class,
            () -> client3.startWorkflow(LWFS.StartWorkflowRequest.newBuilder().build()));

        try {
            client.startWorkflow(LWFS.StartWorkflowRequest.newBuilder().build());
            fail();
        } catch (StatusRuntimeException e) {
            assert e.getTrailers() != null;
            var version = e.getTrailers().get(ClientVersionInterceptor.SUPPORTED_CLIENT_VERSIONS);
            assertNotNull(version);
        }

        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(true);
    }
}
