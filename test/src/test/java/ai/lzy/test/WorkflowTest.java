package ai.lzy.test;

import ai.lzy.common.RandomIdGenerator;
import ai.lzy.service.dao.WorkflowDao;
import ai.lzy.test.context.LzyContextTests;
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
import org.junit.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static ai.lzy.longrunning.OperationUtils.awaitOperationDone;
import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static java.util.Objects.requireNonNull;

public class WorkflowTest extends LzyContextTests {
    static final Logger LOG = LogManager.getLogger(WorkflowTest.class);

    @Test
    public void simple() throws InvalidProtocolBufferException, SQLException {
        var workflowName = "wf";
        var creds = lzyGrpcClient.getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();
        var wf = withIdempotencyKey(lzyGrpcClient, "start_wf").startWorkflow(LWFS.StartWorkflowRequest
            .newBuilder()
            .setWorkflowName(workflowName)
            .setSnapshotStorage(creds)
            .build()
        );

        var graph = withIdempotencyKey(lzyGrpcClient, "execute_graph").executeGraph(ExecuteGraphRequest.newBuilder()
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
            status = lzyGrpcClient.graphStatus(LWFS.GraphStatusRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(wf.getExecutionId())
                .setGraphId(graph.getGraphId())
                .build());
        } while (!status.hasCompleted() && !status.hasFailed());

        LOG.info("Result of execution: {}", JsonFormat.printer().print(status));

        Assert.assertTrue(status.hasCompleted());

        var wfDesc = getWorkflowDesc(workflowName);
        Assert.assertNotNull(wfDesc.allocatorSessionId());
        Assert.assertNull(wfDesc.allocatorSessionDeadline());
        var allocSid = wfDesc.allocatorSessionId();

        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(lzyGrpcClient, "finish_wf").finishWorkflow(LWFS.FinishWorkflowRequest.newBuilder()
            .setWorkflowName(workflowName).setExecutionId(wf.getExecutionId()).setReason("no-matter").build());


        // test delayed session removal
        {
            wfDesc = getWorkflowDesc(workflowName);
            Assert.assertEquals(allocSid, wfDesc.allocatorSessionId());
            Assert.assertNotNull(wfDesc.allocatorSessionDeadline());

            var session = sessionDao().get(allocSid, null);
            Assert.assertNotNull(session);

            System.out.println("--> deadline: " + wfDesc.allocatorSessionDeadline());
            System.out.println("-->      now: " + Instant.now());

            var gc = garbageCollector();
            var deleteAllocSessionOpId = new AtomicReference<String>(null);
            gc.setInterceptor(deleteAllocSessionOpId::set);
            gc.start();

            var ok = TimeUtils.waitFlagUp(() -> {
                try {
                    var desc = getWorkflowDesc(workflowName);
                    return desc.allocatorSessionId() == null;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }, 10, TimeUnit.SECONDS);
            Assert.assertTrue(ok);

            Assert.assertNotNull(deleteAllocSessionOpId.get());

            System.out.println("--> opId: " + deleteAllocSessionOpId.get());

            var op = awaitOperationDone(operationDao(), deleteAllocSessionOpId.get(),
                Duration.ofMillis(100), Duration.ofSeconds(10), LOG);
            Assert.assertNotNull(op);
            Assert.assertTrue(op.done());
            Assert.assertNotNull(op.response());

            session = sessionDao().get(allocSid, null);
            Assert.assertNull(session);
        }
    }

    @Test
    public void reuseAllocatorSession() throws SQLException {
        var workflowName = new RandomIdGenerator().generate("wf-");
        var creds = lzyGrpcClient.getOrCreateDefaultStorage(LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build())
            .getStorage();
        var wf = withIdempotencyKey(lzyGrpcClient, "start_wf")
            .startWorkflow(
                LWFS.StartWorkflowRequest.newBuilder()
                    .setWorkflowName(workflowName)
                    .setSnapshotStorage(creds)
                    .build());

        var wfDesc = getWorkflowDesc(workflowName);
        Assert.assertNotNull(wfDesc.allocatorSessionId());
        Assert.assertNull(wfDesc.allocatorSessionDeadline());

        var sid = wfDesc.allocatorSessionId();

        //noinspection ResultOfMethodCallIgnored
        withIdempotencyKey(lzyGrpcClient, "finish_wf")
            .finishWorkflow(
                LWFS.FinishWorkflowRequest.newBuilder()
                    .setWorkflowName(workflowName)
                    .setExecutionId(wf.getExecutionId())
                    .setReason("no-matter")
                    .build());

        wfDesc = getWorkflowDesc(workflowName);
        Assert.assertEquals(sid, wfDesc.allocatorSessionId());
        Assert.assertNotNull(wfDesc.allocatorSessionDeadline());

        // start new workflow with the same name
        {
            var wf2 = withIdempotencyKey(lzyGrpcClient, "start_wf2")
                .startWorkflow(
                    LWFS.StartWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setSnapshotStorage(creds)
                        .build());

            Assert.assertNotEquals(wf.getExecutionId(), wf2.getExecutionId());

            wfDesc = getWorkflowDesc(workflowName);
            Assert.assertEquals(sid, wfDesc.allocatorSessionId());
            Assert.assertNull(wfDesc.allocatorSessionDeadline());

            //noinspection ResultOfMethodCallIgnored
            withIdempotencyKey(lzyGrpcClient, "finish_wf2")
                .finishWorkflow(
                    LWFS.FinishWorkflowRequest.newBuilder()
                        .setWorkflowName(workflowName)
                        .setExecutionId(wf2.getExecutionId())
                        .setReason("no-matter")
                        .build());

            wfDesc = getWorkflowDesc(workflowName);
            Assert.assertEquals(sid, wfDesc.allocatorSessionId());
            Assert.assertNotNull(wfDesc.allocatorSessionDeadline());
        }
    }

    private WorkflowDao.WorkflowDesc getWorkflowDesc(String workflowName) throws SQLException {
        return requireNonNull(wfDao().loadWorkflowDescForTests(testUser.id(), workflowName));
    }
}
