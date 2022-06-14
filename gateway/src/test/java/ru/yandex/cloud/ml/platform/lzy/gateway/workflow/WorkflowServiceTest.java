package ru.yandex.cloud.ml.platform.lzy.gateway.workflow;

import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.gateway.workflow.storage.Storage;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.CreateWorkflowRequest;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.CreateWorkflowResponse;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.FinishWorkflowRequest;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.FinishWorkflowResponse;

import static org.junit.Assert.assertEquals;

public class WorkflowServiceTest {
    private ApplicationContext ctx;
    private Storage storage;
    private WorkflowService workflowService;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        storage = ctx.getBean(Storage.class);
        workflowService = ctx.getBean(WorkflowService.class);
    }

    @Test
    public void createWorkflow() {
        doCreateWorkflow("workflow_1");

        workflowService.createWorkflow(
                CreateWorkflowRequest.newBuilder()
                        .setWorkflowName("workflow_1")
                        .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(CreateWorkflowResponse value) {
                        assertEquals(CreateWorkflowResponse.KindCase.ERROR, value.getKindCase());
                        assertEquals(CreateWorkflowResponse.ErrorCode.ALREADY_EXISTS.getNumber(), value.getError().getCode());
                    }

                    @Override
                    public void onError(Throwable t) {
                        Assert.fail(t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }

    @Test
    public void finishWorkflow() {
        String executionId = doCreateWorkflow("workflow_2");
        doFinishWorkflow("workflow_2", executionId);

        workflowService.finishWorkflow(
                FinishWorkflowRequest.newBuilder()
                        .setWorkflowName("workflow_2")
                        .setExecutionId(executionId)
                        .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(FinishWorkflowResponse value) {
                        assertEquals(FinishWorkflowResponse.Status.NOT_FOUND.getNumber(), value.getStatus());
                    }

                    @Override
                    public void onError(Throwable t) {
                        Assert.fail(t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }

    private String doCreateWorkflow(String name) {
        var executionId = new String[]{null};

        workflowService.createWorkflow(
                CreateWorkflowRequest.newBuilder()
                        .setWorkflowName(name)
                        .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(CreateWorkflowResponse value) {
                        assertEquals(CreateWorkflowResponse.KindCase.SUCCESS, value.getKindCase());
                        executionId[0] = value.getSuccess().getExecutionId();
                    }

                    @Override
                    public void onError(Throwable t) {
                        Assert.fail(t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                        Assert.assertNotNull(executionId[0]);
                    }
                });
        Assert.assertNotNull(executionId[0]);
        return executionId[0];
    }

    private void doFinishWorkflow(String name, String id) {
        workflowService.finishWorkflow(
                FinishWorkflowRequest.newBuilder()
                        .setWorkflowName(name)
                        .setExecutionId(id)
                        .build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(FinishWorkflowResponse value) {
                        assertEquals(FinishWorkflowResponse.Status.SUCCESS.getNumber(), value.getStatus());
                    }

                    @Override
                    public void onError(Throwable t) {
                        Assert.fail(t.getMessage());
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }

}
