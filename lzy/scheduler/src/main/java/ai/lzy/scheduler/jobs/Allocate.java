package ai.lzy.scheduler.jobs;

import ai.lzy.scheduler.JobService;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.db.JobsOperationDao;
import ai.lzy.scheduler.db.SchedulerDataSource;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.providers.WorkflowJobProvider;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
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

        var allocationOp = allocator.allocate(task.userId(), task.workflowName(), task.allocatorSessionId(),
            task.description().getOperation().getRequirements());

        final String vmId;
        try {
            vmId = allocationOp.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();
        } catch (InvalidProtocolBufferException e) {
            logger.error("Error while getting vmId from meta {} for task {}",
                allocationOp.getMetadata(),
                task.id(), e);
            fail(Status.newBuilder()
                .setCode(Code.INTERNAL.value())
                .setMessage("Internal exception")
                .build());
            return null;
        }

        return task.copy()
            .allocatorOperationId(allocationOp.getId())
            .vmId(vmId)
            .build();
    }

    @Override
    protected TaskState clear(TaskState state, String operationId) {
        if (state.vmId() != null) {
            try {
                allocator.free(state.vmId());
            } catch (StatusRuntimeException e) {
                if (!RETRYABLE_CODES.contains(e.getStatus().getCode())) {
                    logger.error("Error while free vm {}", state.vmId(), e);
                } else {
                    throw e;
                }
            }
        }

        return state.copy()
            .vmId(null)
            .build();
    }
}
