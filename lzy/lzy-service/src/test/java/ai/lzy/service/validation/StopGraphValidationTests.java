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

public class StopGraphValidationTests extends IamOnlyLzyContextTests
    implements ValidationTests<LWFS.StopGraphRequest>
{
    private static final String USER_NAME = "test-user-1";
    private static final String WF_NAME = "test-workflow";
    private static final String EXEC_ID = "test-execution";
    private static final String GRAPH_ID = "test-graph-id";

    private static LzyWorkflowServiceBlockingStub authLzyGrpcClient;

    @Before
    public void before() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InterruptedException {
        authLzyGrpcClient = authorize(lzyClient, USER_NAME, iamClient);
    }

    @Test
    public void missingExecutionId() {
        var request = LWFS.StopGraphRequest.newBuilder()
            .setGraphId(GRAPH_ID)
            .build();
        doAssert(request);
    }

    @Test
    public void missingGraphId() {
        var request = LWFS.StopGraphRequest.newBuilder()
            .setExecutionId(EXEC_ID)
            .build();
        doAssert(request);
    }

    @Override
    public ThrowingRunnable action(LWFS.StopGraphRequest request) {
        //noinspection ResultOfMethodCallIgnored
        return () -> withIdempotencyKey(authLzyGrpcClient, "stop_graph").stopGraph(request);
    }
}
