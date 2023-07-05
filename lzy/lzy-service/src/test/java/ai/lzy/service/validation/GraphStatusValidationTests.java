package ai.lzy.service.validation;

import ai.lzy.service.IamOnlyLzyContextTests;
import ai.lzy.service.ValidationTests;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static ai.lzy.service.IamUtils.authorize;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class GraphStatusValidationTests extends IamOnlyLzyContextTests
    implements ValidationTests<LWFS.GraphStatusRequest>
{
    public static final String USER_NAME = "test-user-1";
    private static final String GRAPH_ID = "test-graph-id";
    private static final String EXEC_ID = "test-execution";

    private static LzyWorkflowServiceBlockingStub authLzyGrpcClient;

    @Before
    public void before() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InterruptedException {
        authLzyGrpcClient = authorize(lzyClient, USER_NAME, iamClient);
    }

    @Test
    public void missingExecutionId() {
        var request = LWFS.GraphStatusRequest.newBuilder()
            .setGraphId(GRAPH_ID)
            .build();
        doAssert(request);
    }

    @Test
    public void missingGraphId() {
        var request = LWFS.GraphStatusRequest.newBuilder()
            .setExecutionId(EXEC_ID)
            .build();
        doAssert(request);
    }

    @Override
    public ThrowingRunnable action(LWFS.GraphStatusRequest request) {
        //noinspection ResultOfMethodCallIgnored
        return () -> withIdempotencyKey(authLzyGrpcClient, "graph_status").graphStatus(request);
    }
}
