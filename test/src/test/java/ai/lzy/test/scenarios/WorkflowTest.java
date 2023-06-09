package ai.lzy.test.scenarios;

import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.impl.v2.WhiteboardContext;
import ai.lzy.test.impl.v2.WorkflowContext;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWF.Graph;
import ai.lzy.v1.workflow.LWF.Operation;
import ai.lzy.v1.workflow.LWF.Operation.SlotDescription;
import ai.lzy.v1.workflow.LWFS;
import ai.lzy.v1.workflow.LWFS.ExecuteGraphRequest;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class WorkflowTest {
    static final Logger LOG = LogManager.getLogger(WorkflowTest.class);

    @Rule
    public final ApplicationContextRule ctx = new ApplicationContextRule();

    @Rule
    public final ContextRule<WorkflowContext> workflow = new ContextRule<>(ctx, WorkflowContext.class);

    @Rule
    public final ContextRule<WhiteboardContext> whiteboard = new ContextRule<>(ctx, WhiteboardContext.class);

    @Test
    public void simple() throws InvalidProtocolBufferException {
        var stub = workflow.context().stub();

        var workflowName = "wf";
        var creds = stub.getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();
        var wf = withIdempotencyKey(stub, "start_wf").startWorkflow(LWFS.StartWorkflowRequest
            .newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(creds)
            .build()
        );

        var graph = withIdempotencyKey(stub, "execute_graph").executeGraph(ExecuteGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(wf.getExecutionId())
            .setGraph(Graph.newBuilder()
                .setName("graph")
                .setZone("ru-central1-a")
                .addOperations(
                    Operation.newBuilder()
                        .setName("1")
                        .setPoolSpecName("s")
                        .setCommand("echo 42 > $LZY_MOUNT/o1")
                        .addOutputSlots(SlotDescription.newBuilder()
                            .setPath("/o1")
                            .setStorageUri(creds.getUri() + "/o1")
                            .build())
                        .build()
                )
                .addOperations(
                    Operation.newBuilder()
                        .setName("2")
                        .setPoolSpecName("s")
                        .setCommand("test \"$(cat $LZY_MOUNT/i2)\" == \"42\"")
                        .addInputSlots(SlotDescription.newBuilder()
                            .setPath("/i2")
                            .setStorageUri(creds.getUri() + "/o1")
                            .build())
                        .build()
                )
                .addDataDescriptions(LWF.DataDescription.newBuilder().setStorageUri(creds.getUri() + "/o1").build())
                .build()
            )
            .build());

        LWFS.GraphStatusResponse status;

        do {
            status = stub.graphStatus(LWFS.GraphStatusRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(wf.getExecutionId())
                .setGraphId(graph.getGraphId())
                .build());
        } while (!status.hasCompleted() && !status.hasFailed());

        LOG.info("Result of execution: {}", JsonFormat.printer().print(status));

        Assert.assertTrue(status.hasCompleted());

        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(stub, "finish_wf").finishWorkflow(LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setExecutionId(wf.getExecutionId()).setReason("no-matter").build());
    }
}
