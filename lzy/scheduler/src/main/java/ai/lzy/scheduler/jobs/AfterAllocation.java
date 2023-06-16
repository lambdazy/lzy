package ai.lzy.scheduler.jobs;

import ai.lzy.iam.config.IamClientConfiguration;
import ai.lzy.iam.grpc.client.AccessBindingServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.scheduler.JobService;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.JobsOperationDao;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.providers.WorkflowJobProvider;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthUniqueViolationException;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import com.google.common.net.HostAndPort;
import com.google.rpc.Code;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.*;

@Singleton
public class AfterAllocation extends WorkflowJobProvider<TaskState> {

    private final RenewableJwt credentials;
    private final IamClientConfiguration authConfig;

    protected AfterAllocation(JobService jobService, TaskStateSerializer serializer,
                              JobsOperationDao opDao, ApplicationContext context, ServiceConfig config)
    {
        super(jobService, serializer, opDao, AwaitAllocation.class, AwaitExecutionCompleted.class, context);

        credentials = config.getIam().createRenewableToken();
        authConfig = config.getIam();
    }

    @Override
    protected TaskState exec(TaskState task, String operationId) throws JobProviderException {
        var iamChannel = newGrpcChannel(authConfig.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
        var subjectClient = new SubjectServiceGrpcClient(SchedulerApi.APP, iamChannel, credentials::get);
        var abClient = new AccessBindingServiceGrpcClient(SchedulerApi.APP, iamChannel, credentials::get);

        RsaUtils.RsaKeys iamKeys;
        try {
            iamKeys = RsaUtils.generateRsaKeys();
            logger.error("Generate RSA keys for worker subject: { publicKey: {}, privateKey: {} }", iamKeys.publicKey(),
                iamKeys.privateKey());
        } catch (Exception e) {
            logger.error("Cannot generate RSA keys: {}", e.getMessage());

            // TODO: delete VM

            fail(com.google.rpc.Status.newBuilder()
                .setCode(Code.INTERNAL.getNumber())
                .setMessage("Cannot generate keys")
                .build());
            return null;
        }

        try {
            Subject subj;
            String[] credName = {null};

            try {
                credName[0] = "main";
                subj = subjectClient.createSubject(AuthProvider.INTERNAL, task.vmId(), SubjectType.WORKER,
                    new SubjectCredentials(credName[0], iamKeys.publicKey(), CredentialsType.PUBLIC_KEY));
            } catch (AuthUniqueViolationException e) {
                subj = subjectClient.findSubject(AuthProvider.INTERNAL, task.vmId(), SubjectType.WORKER);

                for (int i = 0; i < 1000 /* 1000 retries is enough for all ;) */; ++i) {
                    try {
                        credName[0] = "main-" + i;
                        subjectClient.addCredentials(
                            subj.id(), SubjectCredentials.publicKey(credName[0], iamKeys.publicKey()));
                        break;
                    } catch (AuthUniqueViolationException ex) {
                        // (uid, key-name) already exists, try another key name
                    }
                }

            } catch (AuthException e) {
                logger.error("Error while finding subject for vm {}:", task.vmId(), e);
                fail(com.google.rpc.Status.newBuilder()
                    .setCode(Status.Code.INTERNAL.value())
                    .setMessage("Error in iam")
                    .build());
                return null;
            }

            logger.error("Now worker has subject and creds: { vmId: {}, credName: {}, publicKey: {} }", task.vmId(),
                credName[0], iamKeys.publicKey());

            try {
                abClient.setAccessBindings(new Workflow(task.userId() + "/" + task.workflowName()),
                    List.of(new AccessBinding(Role.LZY_WORKER, subj)));
            } catch (StatusRuntimeException e) {
                if (!e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                    throw e;
                }
            } catch (AuthUniqueViolationException e) {
                // Skipping already exists, it can be from cache
            }

            var address = Objects.requireNonNull(task.workerHost());
            var port = Objects.requireNonNull(task.workerPort());

            var workerChannel = newGrpcChannel(HostAndPort.fromParts(address, port), WorkerApiGrpc.SERVICE_NAME);
            var client = newBlockingClient(
                WorkerApiGrpc.newBlockingStub(workerChannel), "Scheduler", () -> credentials.get().token());

            client.init(LWS.InitRequest.newBuilder()
                .setUserId(task.userId())
                .setWorkflowName(task.workflowName())
                .setWorkerSubjectName(task.vmId())
                .setWorkerPrivateKey(iamKeys.privateKey())
                .build());

            var operation = withIdempotencyKey(client, task.id())
                .execute(
                    LWS.ExecuteRequest.newBuilder()
                        .setTaskId(task.id())
                        .setExecutionId(task.executionId())
                        .setWorkflowName(task.workflowName())
                        .setUserId(task.userId())
                        .setTaskDesc(task.description())
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
        } finally {
            iamChannel.shutdown();
        }
    }

    @Override
    protected TaskState clear(TaskState state, String operationId) {
        if (state.workerOperationId() != null) {
            var host = Objects.requireNonNull(state.workerHost());
            var port = Objects.requireNonNull(state.workerPort());

            var address = HostAndPort.fromParts(host, port);

            var workerChannel = newGrpcChannel(address, LongRunningServiceGrpc.SERVICE_NAME);
            var client = newBlockingClient(
                LongRunningServiceGrpc.newBlockingStub(workerChannel), "Scheduler", () -> credentials.get().token());

            try {
                withIdempotencyKey(client, state.id())
                    .cancel(
                        LongRunning.CancelOperationRequest.newBuilder()
                            .setOperationId(state.workerOperationId())
                            .build());
            } catch (Exception e) {
                logger.error("Error while canceling operation on worker {}", state.vmId(), e);
                // ignored
            }

            workerChannel.shutdown();

            return state.copy()
                .workerOperationId(null)
                .build();
        }

        return state;
    }
}
