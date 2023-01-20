package ai.lzy.service;

import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.workflow.LWF;
import ai.lzy.v1.workflow.LWFS;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;

public class LzyServiceAuthTest extends BaseTest {
    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testUnauthenticated() {
        var workflowName = "workflow_1";
        var executionId = "some-valid-execution-id";
        var graphName = "simple-graph";
        var graphId = "some-valid-graph-id";

        var thrown = new ArrayList<StatusRuntimeException>() {
            {
                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.startWorkflow(
                    LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.finishWorkflow(
                    LWFS.FinishWorkflowRequest.newBuilder().setExecutionId(executionId).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.executeGraph(
                    LWFS.ExecuteGraphRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraph(LWF.Graph.newBuilder()
                            .setName(graphName)
                            .build())
                        .build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.graphStatus(
                    LWFS.GraphStatusRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraphId(graphId)
                        .build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> unauthorizedWorkflowClient.stopGraph(
                    LWFS.StopGraphRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraphId(graphId)
                        .build()
                )));
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.UNAUTHENTICATED.getCode(), e.getStatus().getCode()));
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testPermissionDenied() {
        var workflowName = "workflow_1";
        var executionId = "some-valid-execution-id";
        var graphName = "simple-graph";
        var graphId = "some-valid-graph-id";
        var client = unauthorizedWorkflowClient.withInterceptors(ClientHeaderInterceptor.header(
            GrpcHeaders.AUTHORIZATION, JwtUtils.invalidCredentials("user", "GITHUB")::token));

        var thrown = new ArrayList<StatusRuntimeException>() {
            {
                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.startWorkflow(
                    LWFS.StartWorkflowRequest.newBuilder().setWorkflowName(workflowName).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.finishWorkflow(
                    LWFS.FinishWorkflowRequest.newBuilder().setExecutionId(executionId).build())));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.executeGraph(
                    LWFS.ExecuteGraphRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraph(LWF.Graph.newBuilder()
                            .setName(graphName)
                            .build())
                        .build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.graphStatus(
                    LWFS.GraphStatusRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraphId(graphId)
                        .build()
                )));

                add(Assert.assertThrows(StatusRuntimeException.class, () -> client.stopGraph(
                    LWFS.StopGraphRequest.newBuilder()
                        .setExecutionId(executionId)
                        .setGraphId(graphId)
                        .build()
                )));
            }
        };

        thrown.forEach(e -> Assert.assertEquals(Status.PERMISSION_DENIED.getCode(), e.getStatus().getCode()));
    }
}
