package ai.lzy.service.graph;

import ai.lzy.test.IdempotencyUtils.TestScenario;
import ai.lzy.v1.workflow.LWFS;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Test;

import static ai.lzy.test.IdempotencyUtils.processIdempotentCallsConcurrently;
import static org.junit.Assert.*;

public class ConcurrentIdempotentGraphExecutionTest extends AbstractGraphExecutionTest {
    @Test
    public void executeSimpleGraph() throws InterruptedException {
        processIdempotentCallsConcurrently(new TestScenario<>(
            authorizedWorkflowClient,
            /* preparation */
            stub -> startWorkflow("workflow_1"),
            /* action */
            (stub, wf) -> stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                    .setWorkflowName("workflow_1")
                    .setExecutionId(wf.getExecutionId())
                    .setGraph(simpleGraph())
                    .build())
                .getGraphId(),
            /* assertion */
            graphId -> assertFalse(graphId.isBlank())
        ));
    }

    @Test
    public void failedWithEmptyGraph() throws InterruptedException {
        processIdempotentCallsConcurrently(new TestScenario<>(
            authorizedWorkflowClient,
            /* preparation */
            stub -> startWorkflow("workflow_1"),
            /* action */
            (stub, wf) -> assertThrows(StatusRuntimeException.class, () -> {
                //noinspection ResultOfMethodCallIgnored
                stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                    .setWorkflowName("workflow_1")
                    .setExecutionId(wf.getExecutionId())
                    .setGraph(emptyGraph())
                    .build());
            }).getStatus().getCode(),
            /* assertion */
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode)));
    }

    @Test
    public void failedWithCyclicDataflowGraph() throws InterruptedException {
        processIdempotentCallsConcurrently(new TestScenario<>(
            authorizedWorkflowClient,
            /* preparation */
            stub -> startWorkflow("workflow_1"),
            /* action */
            (stub, wf) -> assertThrows(StatusRuntimeException.class, () -> {
                //noinspection ResultOfMethodCallIgnored
                stub.executeGraph(
                    LWFS.ExecuteGraphRequest.newBuilder()
                        .setWorkflowName("workflow_1")
                        .setExecutionId(wf.getExecutionId())
                        .setGraph(cyclicGraph())
                        .build());
            }).getStatus().getCode(),
            /* assertion */
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode)));
    }

    @Test
    public void failedWithUnknownInputSlotUri() throws InterruptedException {
        processIdempotentCallsConcurrently(new TestScenario<>(
            authorizedWorkflowClient,
            /* preparation */
            stub -> startWorkflow("workflow_1"),
            /* action */
            (stub, wf) -> assertThrows(StatusRuntimeException.class, () -> {
                //noinspection ResultOfMethodCallIgnored
                stub.executeGraph(
                    LWFS.ExecuteGraphRequest.newBuilder()
                        .setWorkflowName("workflow_1")
                        .setExecutionId(wf.getExecutionId())
                        .setGraph(unknownSlotUriGraph())
                        .build());
            }).getStatus().getCode(),
            /* assertion */
            errorCode -> assertEquals(Status.NOT_FOUND.getCode(), errorCode)));
    }

    @Test
    public void failedWithoutSuitableZone() throws InterruptedException {
        processIdempotentCallsConcurrently(new TestScenario<>(
            authorizedWorkflowClient,
            /* preparation */
            stub -> startWorkflow("workflow_1"),
            /* action */
            (stub, wf) -> assertThrows(StatusRuntimeException.class, () -> {
                //noinspection ResultOfMethodCallIgnored
                stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                    .setWorkflowName("workflow_1")
                    .setExecutionId(wf.getExecutionId())
                    .setGraph(invalidZoneGraph())
                    .build());
            }).getStatus().getCode(),
            /* assertion */
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode)));
    }

    @Test
    public void failedWithNonSuitableZone() throws InterruptedException {
        processIdempotentCallsConcurrently(new TestScenario<>(
            authorizedWorkflowClient,
            /* preparation */
            stub -> startWorkflow("workflow_1"),
            /* action */
            (stub, wf) -> assertThrows(StatusRuntimeException.class, () -> {
                //noinspection ResultOfMethodCallIgnored
                stub.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
                    .setWorkflowName("workflow_1")
                    .setExecutionId(wf.getExecutionId())
                    .setGraph(nonSuitableZoneGraph())
                    .build());
            }).getStatus().getCode(),
            /* assertion */
            errorCode -> assertEquals(Status.INVALID_ARGUMENT.getCode(), errorCode)));
    }
}
