/*
package ai.lzy.service.restarts;

import ai.lzy.service.debug.InjectedFailures;
import ai.lzy.v1.workflow.LWFS;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Ignore;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

@Ignore
public class ConsistencyAfterRestartTests {
    @Before
    public void setUp() {
        InjectedFailures.reset();
    }

    @After
    public void tearDown() {
        InjectedFailures.reset();
    }
    @Ignore
    @Test
    public void startExecutionFailedJustBeforePortalStarted() {
        InjectedFailures.FAIL_LZY_SERVICE.get(9).set(() -> new InjectedFailures.TerminateException(
            "Fail just before portal started"));

        var deleteSessionFlag = new AtomicBoolean(false);
        onDeleteSession(() -> deleteSessionFlag.set(true));

        var freeVmFlag = new AtomicBoolean(false);
        onFreeVm(() -> freeVmFlag.set(true));

        var creds =
            authorizedWorkflowClient.getOrCreateDefaultStorage(
                LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        var thrown = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_1").setSnapshotStorage(creds).build()));

        var expectedErrorCode = Status.INTERNAL.getCode();

        assertEquals(expectedErrorCode, thrown.getStatus().getCode());
        assertFalse(freeVmFlag.get());
        assertFalse(deleteSessionFlag.get());
        assertEquals(0, (int) metrics.activeExecutions.labels("lzy-internal-user").get());
    }

    @Ignore
    @Test
    public void startExecutionFailedAfterSessionCreated() {
        InjectedFailures.FAIL_LZY_SERVICE.get(10).set(() -> new InjectedFailures.TerminateException(
            "Fail after session created"));

        var deleteSessionFlag = new AtomicBoolean(false);
        onDeleteSession(() -> deleteSessionFlag.set(true));

        var freeVmFlag = new AtomicBoolean(false);
        onFreeVm(() -> freeVmFlag.set(true));

        var creds =
            authorizedWorkflowClient.getOrCreateDefaultStorage(
                LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        var thrown = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_1").setSnapshotStorage(creds).build()));

        var expectedErrorCode = Status.INTERNAL.getCode();

        assertEquals(expectedErrorCode, thrown.getStatus().getCode());
        assertFalse(freeVmFlag.get());
        assertTrue(deleteSessionFlag.get());
        assertEquals(0, (int) metrics.activeExecutions.labels("lzy-internal-user").get());
    }

    @Ignore("Lzy-service shutdown before portal-vm address was stored in db")
    @Test
    public void startExecutionFailedAfterVmRequested() {
        InjectedFailures.FAIL_LZY_SERVICE.get(11).set(() -> new InjectedFailures.TerminateException(
            "Fail after portal vm requested"));

        var deleteSessionFlag = new AtomicBoolean(false);
        onDeleteSession(() -> deleteSessionFlag.set(true));

        var freeVmFlag = new AtomicBoolean(false);
        onFreeVm(() -> freeVmFlag.set(true));

        var thrown = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_1").build()));

        var expectedErrorCode = Status.INTERNAL.getCode();

        assertEquals(expectedErrorCode, thrown.getStatus().getCode());
        assertTrue(freeVmFlag.get());
        assertTrue(deleteSessionFlag.get());
    }

    @Ignore
    @Test
    public void startExecutionFailedAfterPortalStarted() {
        InjectedFailures.FAIL_LZY_SERVICE.get(12).set(() -> new InjectedFailures.TerminateException(
            "Fail after portal started"));

        var deleteSessionFlag = new AtomicBoolean(false);
        onDeleteSession(() -> deleteSessionFlag.set(true));

        var freeVmFlag = new AtomicBoolean(false);
        onFreeVm(() -> freeVmFlag.set(true));

        var creds =
            authorizedWorkflowClient.getOrCreateDefaultStorage(
                LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        var thrown = assertThrows(StatusRuntimeException.class, () ->
            authorizedWorkflowClient.startWorkflow(LWFS.StartWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_1").setSnapshotStorage(creds).build()));

        var expectedErrorCode = Status.INTERNAL.getCode();

        assertEquals(expectedErrorCode, thrown.getStatus().getCode());
        assertTrue(freeVmFlag.get());
        assertTrue(deleteSessionFlag.get());
        assertEquals(0, (int) metrics.activeExecutions.labels("lzy-internal-user").get());
    }
}
*/
