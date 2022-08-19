package ai.lzy.kharon.workflow;

import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.resources.subjects.User;
import ai.lzy.iam.utils.CredentialsHelper;
import ai.lzy.kharon.KharonConfig;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.model.grpc.ClientHeaderInterceptor;
import ai.lzy.model.grpc.GrpcHeaders;
import ai.lzy.model.utils.FreePortFinder;
import ai.lzy.storage.impl.MockS3Storage;
import ai.lzy.util.auth.exceptions.AuthUnauthenticatedException;
import ai.lzy.v1.workflow.LWS.CreateWorkflowRequest;
import ai.lzy.v1.workflow.LWS.FinishWorkflowRequest;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc;
import ai.lzy.v1.workflow.LzyWorkflowServiceGrpc.LzyWorkflowServiceBlockingStub;
import io.grpc.Server;
import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class WorkflowServiceTest {
    private ApplicationContext ctx;
    private Server storageServer;
    private Server workflowServer;
    private LzyWorkflowServiceBlockingStub workflowClient;

    @Before
    public void setUp() throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        var storagePort = FreePortFinder.find(10000, 11000);
        var workflowPort = FreePortFinder.find(storagePort + 1, 11000);

        var props = new HashMap<String, Object>();
        props.put("kharon.address", "localhost:" + workflowPort);
        props.put("kharon.storage.address", "localhost:" + storagePort);

        ctx = ApplicationContext.run(PropertySource.of(props));
        var iam = ctx.getBean(KharonConfig.class).getIam();

        var internalUser = iam.createCredentials();

        var authInterceptor = new AuthServerInterceptor(credentials -> {
            var issuer = CredentialsHelper.issuerFromJWT(credentials.token());
            if (iam.getInternalUserName().equals(issuer)) {
                return new User(issuer);
            }
            throw new AuthUnauthenticatedException("heck");
        });

        storageServer = NettyServerBuilder.forPort(storagePort)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(new MockS3Storage(), authInterceptor))
            .build();
        storageServer.start();

        workflowServer = NettyServerBuilder.forPort(workflowPort)
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(ServerInterceptors.intercept(ctx.getBean(WorkflowService.class), authInterceptor))
            .build();
        workflowServer.start();

        var channel = ChannelBuilder.forAddress("localhost:" + workflowPort)
            .usePlaintext()
            .build();

        workflowClient = LzyWorkflowServiceGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, internalUser::token));
    }

    @After
    public void tearDown() {
        storageServer.shutdown();
        workflowServer.shutdown();
        ctx.stop();
    }

    @Test
    public void createWorkflow() {
        workflowClient.createWorkflow(CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());

        try {
            workflowClient.createWorkflow(CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_1").build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            if (!Status.ALREADY_EXISTS.getCode().equals(e.getStatus().getCode())) {
                e.printStackTrace(System.err);
                Assert.fail(e.getMessage());
            }
        }
    }

    @Test
    public void finishWorkflow() {
        var executionId = workflowClient.createWorkflow(
            CreateWorkflowRequest.newBuilder().setWorkflowName("workflow_2").build()
        ).getExecutionId();

        workflowClient.finishWorkflow(
            FinishWorkflowRequest.newBuilder()
                .setWorkflowName("workflow_2")
                .setExecutionId(executionId)
                .build());

        try {
            workflowClient.finishWorkflow(
                FinishWorkflowRequest.newBuilder()
                    .setWorkflowName("workflow_2")
                    .setExecutionId(executionId)
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            if (Status.INVALID_ARGUMENT.getCode().equals(e.getStatus().getCode())
                && "Already finished.".equals(e.getStatus().getDescription())) {
                Assert.assertTrue(true);
            } else {
                e.printStackTrace(System.err);
                Assert.fail(e.getMessage());
            }
        }

        try {
            workflowClient.finishWorkflow(
                FinishWorkflowRequest.newBuilder()
                    .setWorkflowName("workflow_3")
                    .setExecutionId("execution-id")
                    .build());
            Assert.fail();
        } catch (StatusRuntimeException e) {
            if (!Status.NOT_FOUND.getCode().equals(e.getStatus().getCode())) {
                e.printStackTrace(System.err);
                Assert.fail(e.getMessage());
            }
        }
    }
}
