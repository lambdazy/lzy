package ai.lzy.service.validation;

import ai.lzy.service.IamOnlyLzyContextTests;
import ai.lzy.service.ValidationTests;
import ai.lzy.v1.common.LMST;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import static ai.lzy.service.IamUtils.authorize;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class StartWorkflowValidationTests extends IamOnlyLzyContextTests
    implements ValidationTests<LWFS.StartWorkflowRequest>
{
    private static final String USER_NAME = "test-user-1";
    private static final String WF_NAME = "test-workflow";
    private static LzyWorkflowServiceBlockingStub authLzyGrpcClient;

    @Before
    public void before() throws Exception {
        authLzyGrpcClient = authorize(lzyClient, USER_NAME, iamClient);
    }

    @Test
    public void missingWorkflowName() {
        var request = LWFS.StartWorkflowRequest.newBuilder()
            .setSnapshotStorage(LMST.StorageConfig.getDefaultInstance())
            .build();
        doAssert(request);
    }

    @Test
    public void missingStorageConfig() {
        doAssert(LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(WF_NAME).build());
    }

    @Override
    public ThrowingRunnable action(LWFS.StartWorkflowRequest request) {
        //noinspection ResultOfMethodCallIgnored
        return () -> withIdempotencyKey(authLzyGrpcClient, "start_wf").startWorkflow(request);
    }
}
