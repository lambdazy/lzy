package ai.lzy.service.other;

import ai.lzy.service.ContextAwareTests;
import ai.lzy.service.util.ClientVersionInterceptor;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.workflow.LWFS;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClientVersionTests extends ContextAwareTests {
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testClientVersion() {
        ClientVersionInterceptor.DISABLE_VERSION_CHECK.set(false);

        var client = authLzyGrpcClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.CLIENT_VERSION, () -> "pylzy=1.0.0")  // unsupported pylzy
        );

        assertThrows(StatusRuntimeException.class,
            () -> client.startWorkflow(LWFS.StartWorkflowRequest.newBuilder().build()));

        var client1 = authLzyGrpcClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.CLIENT_VERSION, () -> "keklol=1.0.0")  // unsupported client
        );

        assertThrows(StatusRuntimeException.class,
            () -> client1.startWorkflow(LWFS.StartWorkflowRequest.newBuilder().build()));

        var client2 = authLzyGrpcClient.withInterceptors(
            ClientHeaderInterceptor.header(GrpcHeaders.CLIENT_VERSION, () -> "pylzy=1.100")  // wrong version format
        );

        assertThrows(StatusRuntimeException.class,
            () -> client2.startWorkflow(LWFS.StartWorkflowRequest.newBuilder().build()));

        var client3 = authLzyGrpcClient.withInterceptors(
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
