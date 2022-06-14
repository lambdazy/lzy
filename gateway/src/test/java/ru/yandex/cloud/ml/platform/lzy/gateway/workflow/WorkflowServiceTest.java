package ru.yandex.cloud.ml.platform.lzy.gateway.workflow;

import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.JwtCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.context.AuthenticationContext;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.User;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.TokenParser;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.CreateWorkflowRequest;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.CreateWorkflowResponse;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.FinishWorkflowRequest;
import yandex.cloud.priv.datasphere.v2.lzy.LzyApiGateway.FinishWorkflowResponse;

import static org.junit.Assert.assertEquals;

public class WorkflowServiceTest {
    private ApplicationContext ctx;
    private WorkflowService workflowService;
    private Context grpcCtx;
    private Context prevGrpcCtx;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        workflowService = ctx.getBean(WorkflowService.class);

        grpcCtx = Context.current().withValue(
            AuthenticationContext.KEY,
            new AuthenticationContext(
                new TokenParser.Token(TokenParser.Token.Kind.JWT_TOKEN, "i-am-a-hacker"),
                new JwtCredentials("i-am-a-hacker"),
                new User("test-user")));
        prevGrpcCtx = grpcCtx.attach();
    }

    @After
    public void tearDown() {
        grpcCtx.detach(prevGrpcCtx);
        ctx.stop();
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
