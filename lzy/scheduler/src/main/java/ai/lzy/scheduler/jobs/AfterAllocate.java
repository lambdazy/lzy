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
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.SchedulerApi;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.impl.SchedulerDataSource;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.VmAllocatorApi;
import ai.lzy.v1.VmAllocatorApi.AllocateResponse.VmEndpoint.VmEndpointType;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.worker.LWS;
import ai.lzy.v1.worker.WorkerApiGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.util.List;
import javax.annotation.Nullable;

import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;

@Singleton
public class AfterAllocate extends SchedulerOperationConsumer {

    private final SubjectServiceGrpcClient subjectClient;
    private final AccessBindingServiceGrpcClient abClient;
    private final RenewableJwt credentials;

    protected AfterAllocate(JobService jobService, WaitForOperation opProvider,
                            TaskDao dao, SchedulerDataSource storage, ServiceConfig config)
    {
        super(jobService, opProvider, dao, storage);

        credentials = config.getIam().createRenewableToken();

        IamClientConfiguration authConfig = config.getIam();
        ManagedChannel iamChannel = newGrpcChannel(authConfig.getAddress(), LzyAuthenticateServiceGrpc.SERVICE_NAME);
        subjectClient = new SubjectServiceGrpcClient(SchedulerApi.APP, iamChannel, credentials::get);
        abClient = new AccessBindingServiceGrpcClient(SchedulerApi.APP, iamChannel, credentials::get);
    }

    @Override
    protected void execute(String operationId, @Nullable Status.Code code, @Nullable LongRunning.Operation op,
                           TaskState task, @Nullable TransactionHandle tx) throws SQLException
    {
        if (code != null && !code.equals(Status.Code.OK)) {
            logger.error("Error while polling allocating operation {}, code: {}, task: {}",
                operationId, code, task.id());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        if (op == null) {
            logger.error("Operation is null, but no error code for operation {} for task {}", operationId, task.id());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        if (!task.status().equals(TaskState.Status.ALLOCATING)) {
            logger.error("Status of task {} is {}, but expected ALLOCATING", task.id(), task.status());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        if (op.hasError()) {
            logger.error("Error while allocating worker for task {}: {}", op.getError(), task.id());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        final VmAllocatorApi.AllocateResponse vmDesc;
        try {
            vmDesc = op.getResponse().unpack(VmAllocatorApi.AllocateResponse.class);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Error while allocating worker for task {}: cannot get response from op", task.id(), e);

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        var meta = vmDesc.getMetadataMap();
        var vmId = vmDesc.getVmId();

        var publicKey = meta.get("PUBLIC_KEY");

        if (publicKey == null) {
            logger.error("Error while allocating worker for task {}: public key in metadata is null", task.id());

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        Subject subj;

        try {
            subj = subjectClient.createSubject(AuthProvider.INTERNAL, vmId, SubjectType.WORKER,
                new SubjectCredentials("main", publicKey, CredentialsType.PUBLIC_KEY));
        } catch (StatusRuntimeException e) {

            // Skipping already exists, it can be from cache
            if (!e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                logger.error("Error while creating subject for vm {}: ", vmId, e);

                failInternal(task.id(), task.executionId(), tx);
                return;
            }

            subj = new Worker(vmId);
        }


        try {
            abClient.setAccessBindings(new Workflow(task.userId() + "/" + task.workflowName()),
                List.of(new AccessBinding(Role.LZY_WORKFLOW_OWNER, subj)));
        } catch (StatusRuntimeException e) {
            if (!e.getStatus().getCode().equals(Status.Code.ALREADY_EXISTS)) {
                logger.error("Error while creating subject for vm {}: ", vmId, e);

                failInternal(task.id(), task.executionId(), tx);
                return;
            }
        }

        var address = vmDesc.getEndpointsList().stream()
            .filter(e -> e.getType().equals(VmEndpointType.INTERNAL_IP))
            .findFirst()
            .orElse(null);

        if (address == null) {
            logger.error("Error while allocating worker for task {}: no internal ip on vm {}", task.id(), vmId);

            failInternal(task.id(), task.executionId(), tx);
            return;
        }

        var workerChannel = GrpcUtils.newGrpcChannel(address.getValue(), WorkerApiGrpc.SERVICE_NAME);
        var client = GrpcUtils.newBlockingClient(
            WorkerApiGrpc.newBlockingStub(workerChannel),
            "worker", () -> credentials.get().token());

        client = GrpcUtils.withIdempotencyKey(client, task.id());

        var operation = client.execute(LWS.ExecuteRequest.newBuilder()
            .setTaskId(task.id())
            .setExecutionId(task.executionId())
            .setTaskDesc(task.description().toProto())
            .build());

        dao.updateWorkerData(task.id(), task.executionId(), address.getValue(), operation.getId(), tx);
    }
}
