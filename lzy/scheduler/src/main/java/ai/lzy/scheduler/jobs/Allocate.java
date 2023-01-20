package ai.lzy.scheduler.jobs;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.db.JobsOperationDao;
import ai.lzy.jobsutils.providers.WorkflowJobProvider;
import ai.lzy.model.db.DbHelper;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.db.SchedulerDataSource;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import io.grpc.Status.Code;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;

@Singleton
public class Allocate extends WorkflowJobProvider<TaskState> {
    private final WorkersAllocator allocator;
    private final TaskDao dao;
    private final SchedulerDataSource storage;

    protected Allocate(
        JobService jobService, TaskStateSerializer serializer, JobsOperationDao opDao, ApplicationContext context,
        TaskDao dao, SchedulerDataSource storage, WorkersAllocator allocator)
    {
        super(jobService, serializer, opDao, null, AwaitAllocation.class, context);
        this.allocator = allocator;
        this.dao = dao;
        this.storage = storage;
    }

    @Override
    protected TaskState exec(TaskState task, String operationId) throws JobProviderException {

        final String session;
        try {
            session = DbHelper.withRetries(logger, () -> {
                try (TransactionHandle tx = TransactionHandle.create(storage)) {
                    var s = dao.getAllocatorSession(task.workflowName(), task.userId(), tx);

                    if (s == null) {
                        s = allocator.createSession(task.userId(), task.workflowName());
                        dao.insertAllocatorSession(task.workflowName(), task.userId(), s, tx);
                    }

                    return s;
                }
            });
        } catch (Exception e) {
            logger.error("Error while generating session in op {}", operationId, e);
            fail(Status.newBuilder()
                .setCode(Code.INTERNAL.value())
                .setMessage("Internal exception")
                .build());
            return null;
        }

        var allocateDesc = allocator.allocate(task.userId(), task.workflowName(), session,
            task.description().operation().requirements());

        final String vmId;
        try {
            vmId = allocateDesc.allocationOp().getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();
        } catch (InvalidProtocolBufferException e) {
            logger.error("Error while getting vmId from meta {} for task {}",
                allocateDesc.allocationOp().getMetadata(),
                task.id(), e);
            fail(Status.newBuilder()
                .setCode(Code.INTERNAL.value())
                .setMessage("Internal exception")
                .build());
            return null;
        }

        return task.copy()
            .allocatorOperationId(allocateDesc.allocationOp().getId())
            .vmId(vmId)
            .workerPort(allocateDesc.workerPort())
            .build();
    }

    @Override
    protected TaskState clear(TaskState state, String operationId) {
        if (state.vmId() != null) {
            allocator.free(state.vmId());
        }

        return state.copy()
            .vmId(null)
            .build();
    }
}
