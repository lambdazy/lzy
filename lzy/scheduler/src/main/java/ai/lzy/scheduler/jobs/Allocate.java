package ai.lzy.scheduler.jobs;

import ai.lzy.jobsutils.JobService;
import ai.lzy.jobsutils.db.JobsOperationDao;
import ai.lzy.jobsutils.providers.JobSerializerBase;
import ai.lzy.jobsutils.providers.WorkflowJobProvider;
import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.impl.SchedulerDataSource;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.VmAllocatorApi;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Status;
import io.grpc.Status.Code;
import io.micronaut.context.ApplicationContext;
import jakarta.inject.Singleton;

import javax.annotation.Nullable;
import java.sql.SQLException;
import java.time.Duration;

@Singleton
public class Allocate extends WorkflowJobProvider<TaskState> {
    private final WorkersAllocator allocator;
    private final ServiceConfig config;
    private final RenewableJwt token;
    private final TaskDao dao;
    private final SchedulerDataSource storage;

    protected Allocate(
        JobService jobService, TaskStateSerializer serializer, JobsOperationDao opDao, ApplicationContext context,
        TaskDao dao, SchedulerDataSource storage, WorkersAllocator allocator, ServiceConfig config)
    {
        super(jobService, serializer, opDao, null, null, context);
        this.allocator = allocator;
        this.config = config;
        this.token = config.getIam().createRenewableToken();
        this.dao = dao;
        this.storage = storage;
    }

    @Override
    protected TaskState exec(TaskState task, String operationId) throws JobProviderException {

        String session;

        try (TransactionHandle tx = TransactionHandle.create(storage)) {
            session = dao.getAllocatorSession(task.workflowName(), task.userId(), tx);

            if (session == null) {
                session = allocator.createSession(task.userId(), task.workflowName());
                dao.insertAllocatorSession(task.workflowName(), task.userId(), session, tx);
            }
        }

        var op = allocator.allocate(task.userId(), task.workflowName(), session,
            task.description().operation().requirements());

        final String vmId;
        try {
            vmId = op.getMetadata().unpack(VmAllocatorApi.AllocateMetadata.class).getVmId();
        } catch (InvalidProtocolBufferException e) {
            logger.error("Error while getting vmId from op for task {}", task.id(), e);
            fail(Status.newBuilder()
                .setCode(Code.INTERNAL.value())
                .setMessage("Internal exception")
                .build());
        }



        dao.updateAllocatorData(task.id(), task.executionId(), op.getId(), vmId, tx);


        return null;
    }

    @Override
    protected TaskState clear(TaskState state, String operationId) {
        return null;
    }
}
