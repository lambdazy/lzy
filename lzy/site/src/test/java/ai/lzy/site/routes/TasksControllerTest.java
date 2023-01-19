package ai.lzy.site.routes;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.interceptors.AuthServerInterceptor;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.site.ServiceConfig;
import ai.lzy.v1.scheduler.Scheduler;
import ai.lzy.v1.scheduler.SchedulerApi;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import com.google.common.net.HostAndPort;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpStatus;
import io.micronaut.runtime.server.EmbeddedServer;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

public class TasksControllerTest extends BaseTestWithIam {
    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });

    private ApplicationContext context;
    private EmbeddedServer server = ApplicationContext.run(EmbeddedServer.class);

    private Auth auth;
    private Tasks tasks;
    private Server schedulerServer;

    @Before
    public void before() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));
        context = server.getApplicationContext();
        tasks = context.getBean(Tasks.class);
        auth = context.getBean(Auth.class);
        server = context.getBean(EmbeddedServer.class);
        ServiceConfig serviceConfig = context.getBean(ServiceConfig.class);
        final HostAndPort schedulerAddress = HostAndPort.fromString(serviceConfig.getSchedulerAddress());
        schedulerServer = NettyServerBuilder.forAddress(
                new InetSocketAddress(schedulerAddress.getHost(), schedulerAddress.getPort()))
            .intercept(new AuthServerInterceptor(
                new AuthenticateServiceGrpcClient("LzySite", serviceConfig.getIam().getAddress())))
            .addService(new SchedulerTasksMock())
            .build();
        schedulerServer.start();
    }

    @After
    public void after() {
        super.after();
        server.stop();
        if (schedulerServer != null) {
            schedulerServer.shutdownNow();
        }
        context.stop();
    }

    @Test
    public void getTasksTest() {
        final String signInUrl = "https://host/signIn";
        final var response = auth.acceptGithubCode("code", signInUrl);
        Assert.assertEquals(HttpStatus.MOVED_PERMANENTLY.getCode(), response.code());
        final Utils.ParsedCookies cookies = Utils.parseCookiesFromHeaders(response);
        final String workflowId = "workflowId";
        {
            final var tasksListResponse = tasks.get(
                cookies.userSubjectId(),
                cookies.sessionId(),
                new Tasks.GetTasksRequest(workflowId)
            );
            final Tasks.GetTasksResponse body = tasksListResponse.body();
            Assert.assertNotNull(body);
            final List<Tasks.TaskStatus> taskStatusList = body.taskStatusList();
            Assert.assertNotNull(taskStatusList);

            Assert.assertEquals(getTestTasks(workflowId), taskStatusList);
        }
    }

    private List<Tasks.TaskStatus> getTestTasks(String workflowId) {
        return List.of(
            new Tasks.TaskStatus(workflowId, "foo", "task1", "SUCCESS", "Return code: 0"),
            new Tasks.TaskStatus(workflowId, "bar", "task2", "EXECUTING", "-"),
            new Tasks.TaskStatus(workflowId, "baz", "task3", "ERROR", "some error occurred")
        );
    }

    private final class SchedulerTasksMock extends SchedulerGrpc.SchedulerImplBase {
        @Override
        public void list(SchedulerApi.TaskListRequest request,
                         StreamObserver<SchedulerApi.TaskListResponse> responseObserver)
        {
            final String workflowId = request.getWorkflowId();
            final var responseBuilder = SchedulerApi.TaskListResponse.newBuilder();
            getTestTasks(workflowId)
                .forEach(taskStatus -> {
                    final Scheduler.TaskStatus.Builder taskBuilder = Scheduler.TaskStatus.newBuilder()
                        .setWorkflowId(taskStatus.workflowId())
                        .setTaskId(taskStatus.taskId())
                        .setOperationName(taskStatus.operationName());
                    switch (taskStatus.status()) {
                        case "SUCCESS" -> taskBuilder.setSuccess(
                            Scheduler.TaskStatus.Success.newBuilder().setRc(0).build());
                        case "EXECUTING" -> taskBuilder.setExecuting(
                            Scheduler.TaskStatus.Executing.getDefaultInstance());
                        case "ERROR" -> taskBuilder.setError(Scheduler.TaskStatus.Error.newBuilder()
                            .setDescription(taskStatus.description())
                            .buildPartial());
                    }
                    responseBuilder.addStatus(taskBuilder.build());
                });

            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        }
    }
}
