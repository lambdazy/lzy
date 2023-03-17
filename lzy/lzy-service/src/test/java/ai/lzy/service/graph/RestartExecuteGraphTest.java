package ai.lzy.service.graph;

import ai.lzy.model.db.exceptions.DaoException;
import ai.lzy.service.LzyService;
import ai.lzy.service.debug.InjectedFailures;
import ai.lzy.service.debug.InjectedFailures.TerminateException;
import ai.lzy.service.debug.OperationDaoDecorator;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static ai.lzy.util.grpc.GrpcUtils.withIdempotencyKey;
import static io.micronaut.inject.qualifiers.Qualifiers.byName;
import static org.junit.Assert.*;

public class RestartExecuteGraphTest extends AbstractGraphExecutionTest {
    private OperationDaoDecorator operationDaoDecorator;

    @Override
    @Before
    public void setUp() throws IOException, InterruptedException {
        super.setUp();
        operationDaoDecorator = context.getBean(OperationDaoDecorator.class, byName("LzyServiceOperationDao"));
        operationDaoDecorator.onCreateCounter();
        InjectedFailures.reset();
    }

    @Override
    @After
    public void tearDown() throws java.sql.SQLException, InterruptedException, DaoException {
        super.tearDown();
        operationDaoDecorator = null;
        InjectedFailures.reset();
    }

    @Test
    public void executeGraphFail0() {
        InjectedFailures.FAIL_LZY_SERVICE.get(0).set(() -> new TerminateException("Fail before create operation"));
        executeGraphWithRestartImpl();
    }

    @Test
    public void executeGraphFail1() {
        InjectedFailures.FAIL_LZY_SERVICE.get(1).set(
            () -> new TerminateException("Fail before processing operation"));
        executeGraphWithRestartImpl();
    }

    @Test
    public void executeGraphFail2() {
        InjectedFailures.FAIL_LZY_SERVICE.get(2).set(() ->
            new TerminateException("Fail just after validation but before saving results"));
        executeGraphWithRestartImpl();
    }

    @Test
    public void executeGraphFail3() {
        InjectedFailures.FAIL_LZY_SERVICE.get(3).set(() -> new TerminateException("Fail before building graph"));
        executeGraphWithRestartImpl();
    }

    // todo: enable when CLOUD-122323 will be done
    @Ignore("Required idempotency in channel manager")
    @Test
    public void executeGraphFail4() {
        InjectedFailures.FAIL_LZY_SERVICE.get(4).set(
            () -> new TerminateException("Fail just after building graph but before saving results"));
        executeGraphWithRestartImpl();
    }

    @Test
    public void executeGraphFail5() {
        InjectedFailures.FAIL_LZY_SERVICE.get(5).set(
            () -> new TerminateException("Fail before graph executor was requested"));
        executeGraphWithRestartImpl();
    }

    @Test
    public void executeGraphFail6() {
        InjectedFailures.FAIL_LZY_SERVICE.get(6).set(
            () -> new TerminateException("Fail just after requested but before saving results"));
        executeGraphWithRestartImpl();
    }

    @Test
    public void executeGraphFail7() {
        InjectedFailures.FAIL_LZY_SERVICE.get(7).set(
            () -> new TerminateException("Fail before saving if of executed graph"));
        executeGraphWithRestartImpl();
    }

    @Test
    public void executeGraphFail8() {
        InjectedFailures.FAIL_LZY_SERVICE.get(8).set(
            () -> new TerminateException("Fail before operation result saving"));
        executeGraphWithRestartImpl();
    }

    private void executeGraphWithRestartImpl() {
        var workflowName = "workflow_1";
        var storage =
            authorizedWorkflowClient.getOrCreateDefaultStorage(
                    LWFS.GetOrCreateDefaultStorageRequest.newBuilder().build()).getStorage();
        LWFS.StartWorkflowResponse workflow = authorizedWorkflowClient.startWorkflow(
            LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).setSnapshotStorage(storage).build());
        LWF.Graph graph = simpleGraph(storage);
        var executionId = workflow.getExecutionId();

        var idempotencyKey = "idempotency-key";
        var idempotentCallsClient = withIdempotencyKey(authorizedWorkflowClient, idempotencyKey);

        assertThrows(StatusRuntimeException.class, () -> idempotentCallsClient.executeGraph(
            LWFS.ExecuteGraphRequest.newBuilder()
                .setWorkflowName(workflowName)
                .setExecutionId(executionId)
                .setGraph(graph)
                .build()).getGraphId());

        // restart LzyService

        context.getBean(LzyService.class).testRestart();

        var graphId = idempotentCallsClient.executeGraph(LWFS.ExecuteGraphRequest.newBuilder()
            .setWorkflowName(workflowName)
            .setExecutionId(executionId)
            .setGraph(graph)
            .build()).getGraphId();

        assertFalse(graphId.isBlank());
        assertSame(1, operationDaoDecorator.createCallsCount());
    }
}
