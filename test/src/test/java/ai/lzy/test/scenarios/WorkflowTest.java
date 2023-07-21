package ai.lzy.test.scenarios;

import ai.lzy.test.ApplicationContextRule;
import ai.lzy.test.ContextRule;
import ai.lzy.test.TimeUtils;
import ai.lzy.test.impl.v2.AllocatorContext;
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

import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;

public class WorkflowTest {
    static final Logger LOG = LogManager.getLogger(WorkflowTest.class);

    @Rule
    public final ApplicationContextRule ctx = new ApplicationContextRule();

    @Rule
    public final ContextRule<AllocatorContext> allocator = new ContextRule<>(ctx, AllocatorContext.class);

    @Rule
    public final ContextRule<WorkflowContext> workflow = new ContextRule<>(ctx, WorkflowContext.class);

    @Rule
    public final ContextRule<WhiteboardContext> whiteboard = new ContextRule<>(ctx, WhiteboardContext.class);

    @Test
    public void simple() throws InvalidProtocolBufferException, SQLException {
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

        var wfDesc = workflow.context().wfDao()
            .loadWorkflowDescForTests(workflow.context().internalUserName(), workflowName);
        Assert.assertNotNull(wfDesc.allocatorSessionId());
        Assert.assertNull(wfDesc.allocatorSessionDeadline());
        var allocSid = wfDesc.allocatorSessionId();

        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(stub, "finish_wf").finishWorkflow(LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setExecutionId(wf.getExecutionId()).setReason("no-matter").build());


        // test session cache

        wfDesc = workflow.context().wfDao()
            .loadWorkflowDescForTests(workflow.context().internalUserName(), workflowName);
        Assert.assertEquals(allocSid, wfDesc.allocatorSessionId());
        Assert.assertNotNull(wfDesc.allocatorSessionDeadline());

        var session = allocator.context().sessionDao().get(allocSid, null);
        Assert.assertNotNull(session);

        System.out.println("--> deadline: " + wfDesc.allocatorSessionDeadline());
        System.out.println("-->      now: " + Instant.now());

        workflow.context().startGc();
        var ok = TimeUtils.waitFlagUp(() -> {
            try {
                var desc = workflow.context().wfDao()
                    .loadWorkflowDescForTests(workflow.context().internalUserName(), workflowName);
                Assert.assertNotNull(desc);
                return desc.allocatorSessionId() == null;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }, 10, TimeUnit.SECONDS);
        Assert.assertTrue(ok);

        session = allocator.context().sessionDao().get(allocSid, null);
        Assert.assertNull(session);
    }
}
