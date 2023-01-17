package ai.lzy.scheduler.jobs;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.*;
import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.db.JobsOperationDao;
import ai.lzy.jobsutils.providers.WorkflowJobProvider;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.exceptions.AuthUniqueViolationException;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class AfterAllocation extends WorkflowJobProvider<TaskState> {

    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;
    private final RenewableJwt credentials;

    protected AfterAllocation(JobService jobService, TaskStateSerializer serializer,
                              JobsOperationDao opDao, ApplicationContext context, ServiceConfig config)
    {
        super(jobService, serializer, opDao, AwaitAllocation.class, AwaitExecutionCompleted.class, context);

        credentials = config.getIam().createRenewableToken();

        IamClientConfiguration authConfig = config.getIam();
        ManagedChannel iamChannel = newGrpcChannel(authConfig.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
        subjectClient = new SubjectServiceGrpcClient(SchedulerApi.APP, iamChannel, credentials::get);
        abClient = new AccessBindingServiceGrpcClient(SchedulerApi.APP, iamChannel, credentials::get);
    }

    @Override
    protected TaskState exec(TaskState task, String operationId) throws JobProviderException {

        Subject subj;

        try {
            subj = subjectClient.createSubject(AuthProvider.INTERNAL, task.vmId(), SubjectType.WORKER,
                new SubjectCredentials("main", task.workerPublicKey(), CredentialsType.PUBLIC_KEY));
        } catch (StatusRuntimeException e) {
            // Skipping already exists, it can be from cache
            if (e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                subj = new Worker(task.vmId());
            } else {
                throw e;  // Will be handled in super
            }
        } catch (AuthUniqueViolationException e) {
            subj = new Worker(task.vmId());  // Skipping already exists, it can be from cache
        }


        try {
            abClient.setAccessBindings(new Workflow(task.userId() + "/" + task.workflowName()),
                    List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (StatusRuntimeException e) {
            if (!e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                throw e;
            }
        } catch (AuthUniqueViolationException e) {
            // Skipping already exists, it can be from cache
        }

        var address = task.workerAddress();

        var workerChannel = GrpcUtils.newGrpcChannel(address, WorkerApiGrpc.SERVICE_NAME);
        var client = GrpcUtils.newBlockingClient(
                WorkerApiGrpc.newBlockingStub(workerChannel),
                "worker", () -> credentials.get().token());

        client = GrpcUtils.withIdempotencyKey(client, task.id());

        var operation = client.execute(LWS.ExecuteRequest.newBuilder()
            .setTaskId(task.id())
            .setExecutionId(task.executionId())
            .setTaskDesc(task.description().toProto())
            .build());

        try {
            workerChannel.shutdownNow();
            workerChannel.awaitTermination(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            // ignored
        }

        return task.copy()
            .workerOperationId(operation.getId())
            .build();
    }

    @Override
    protected TaskState clear(TaskState state, String operationId) {
        if (state.workerOperationId() != null) {
            var address = state.workerAddress();

            var workerChannel = GrpcUtils.newGrpcChannel(address, LongRunningServiceGrpc.SERVICE_NAME);
            var client = GrpcUtils.newBlockingClient(
                LongRunningServiceGrpc.newBlockingStub(workerChannel),
                "worker", () -> credentials.get().token());

            client = GrpcUtils.withIdempotencyKey(client, state.id());
            try {
                client.cancel(LongRunning.CancelOperationRequest.newBuilder()
                    .setOperationId(state.workerOperationId())
                    .build());
            } catch (Exception e) {
                logger.error("Error while canceling operation on worker {}", state.vmId(), e);
                // ignored
            }
            return state.copy()
                .workerOperationId(null)
                .build();
        }

        return state;
    }
}
