package ai.lzy.service.validation;

import ai.lzy.service.IamOnlyLzyContextTests;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Before;
import org.junit.Test;
import org.junit.function.ThrowingRunnable;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;

import static ai.lzy.service.IamUtils.authorize;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class OtherValidationTests extends IamOnlyLzyContextTests {
    private static final String USER_NAME = "test-user-1";
    private static final String WF_NAME = "test-workflow";
    private static final String EXEC_ID = "test-execution";

    private static LzyWorkflowServiceBlockingStub authLzyGrpcClient;

    @Before
    public void before() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException, InterruptedException {
        authLzyGrpcClient = authorize(lzyClient, USER_NAME, iamClient);
    }

    @Test
    public void missingWorkflowNameInReadStdRequest() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder().setExecutionId(EXEC_ID).build();
        doAssert(() -> authLzyGrpcClient.readStdSlots(request).next());
    }

    @Test
    public void missingExecutionIdInReadStdRequest() {
        var request = LWFS.ReadStdSlotsRequest.newBuilder().setWorkflowName(WF_NAME).build();
        doAssert(() -> authLzyGrpcClient.readStdSlots(request).next());
    }

    @Test
    public void missingWorkflowNameInGetVmPoolsRequest() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder().setExecutionId(EXEC_ID).build();
        //noinspection ResultOfMethodCallIgnored
        doAssert(() -> authLzyGrpcClient.getAvailablePools(request));
    }

    @Test
    public void missingExecutionIdInGetVmPoolsRequest() {
        var request = LWFS.GetAvailablePoolsRequest.newBuilder().setWorkflowName(WF_NAME).build();
        //noinspection ResultOfMethodCallIgnored
        doAssert(() -> authLzyGrpcClient.getAvailablePools(request));
    }

    private static void doAssert(ThrowingRunnable action) {
        var sre = assertThrows(StatusRuntimeException.class, action);
        assertSame(Status.INVALID_ARGUMENT.getCode(), sre.getStatus().getCode());
    }
}
