package ai.lzy.kharon.workflow;

import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import ai.lzy.iam.authorization.credentials.JwtCredentials;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.utils.TokenParser;
import ai.lzy.priv.v2.LzyWorkflowApi.CreateWorkflowRequest;
import ai.lzy.priv.v2.LzyWorkflowApi.CreateWorkflowResponse;
import ai.lzy.priv.v2.LzyWorkflowApi.FinishWorkflowRequest;
import ai.lzy.priv.v2.LzyWorkflowApi.FinishWorkflowResponse;

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
                    Assert.fail("Unexpected");
                }

                @Override
                public void onError(Throwable t) {
                    if (t instanceof StatusException se) {
                        assertEquals(Status.ALREADY_EXISTS.getCode(), se.getStatus().getCode());
                    } else {
                        Assert.fail(t.getMessage());
                    }
                }

                @Override
                public void onCompleted() {
                    Assert.fail("Unexpected");
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
                    Assert.fail("Unexpected");
                }

                @Override
                public void onError(Throwable t) {
                    if (t instanceof StatusException se) {
                        assertEquals(Status.NOT_FOUND.getCode(), se.getStatus().getCode());
                    } else {
                        Assert.fail(t.getMessage());
                    }
                }

                @Override
                public void onCompleted() {
                    Assert.fail("Unexpected");
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
                    executionId[0] = value.getExecutionId();
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
