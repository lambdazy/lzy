package ai.lzy.scheduler;

import ai.lzy.jobsutils.db.JobsOperationDao;
import ai.lzy.jobsutils.providers.JobSerializer;
import ai.lzy.longrunning.IdempotencyUtils;
import ai.lzy.longrunning.Operation;
import ai.lzy.model.ReturnCodes;
import ai.lzy.model.TaskDesc;
import ai.lzy.model.db.DbHelper;
import ai.lzy.scheduler.db.SchedulerDataSource;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.jobs.Allocate;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.scheduler.Scheduler.TaskStatus;
import ai.lzy.v1.scheduler.SchedulerApi.*;
import ai.lzy.v1.scheduler.SchedulerGrpc;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class SchedulerApiImpl extends SchedulerGrpc.SchedulerImplBase {
    private static final Logger LOG = LogManager.getLogger(SchedulerApiImpl.class);
    private final TaskDao dao;
    private final JobsOperationDao opDao;
    private final SchedulerDataSource storage;
    private final Allocate allocateJob;

    @Inject
    public SchedulerApiImpl(TaskDao dao, JobsOperationDao opDao, SchedulerDataSource storage, Allocate allocateJob) {
        this.dao = dao;
        this.opDao = opDao;
        this.storage = storage;
        this.allocateJob = allocateJob;
    }

    @Override
    public void schedule(TaskScheduleRequest request, StreamObserver<TaskScheduleResponse> responseObserver) {

        var idempotencyKey = IdempotencyUtils.getIdempotencyKey(request);

        if (idempotencyKey != null) {
            final Operation op;
            try {
                op = DbHelper.withRetries(LOG, () -> opDao.getByIdempotencyKey(idempotencyKey.token(), null));
            } catch (Exception e) {
                LOG.error("Error while finding op by idempotency key", e);
                throw Status.INTERNAL.asRuntimeException();
            }

            if (op != null) {
                final TaskDao.TaskDesc desc;
                try {
                    desc = DbHelper.withRetries(LOG, () -> dao.getTaskDesc(op.id(), null));
                } catch (Exception e) {
                    LOG.error("Error while getting task description", e);
                    throw Status.INTERNAL.asRuntimeException();
                }

                if (desc != null) {
                    responseObserver.onNext(TaskScheduleResponse.newBuilder()
                        .setStatus(buildTaskStatus(desc, op))
                        .build());
                    responseObserver.onCompleted();
                    return;
                }

                throw Status.INVALID_ARGUMENT
                    .withDescription("Operation is not completed, but description not found").asRuntimeException();
            }
        }

        var taskState = new TaskState(
            UUID.randomUUID().toString(),
            request.getWorkflowId(),
            request.getWorkflowName(),
            request.getUserId(),
            TaskDesc.fromProto(request.getTask()),
            null,
            null,
            null,
            null,
            null,
            null
        );

        var op = Operation.create(
            request.getUserId(),
            "Executing task " + taskState.id(),
            idempotencyKey,
            null
        );

        var taskDesc = new TaskDao.TaskDesc(
            taskState.id(),
            taskState.executionId(),
            taskState.workflowName(),
            taskState.userId(),
            op.id(),
            request.getTask().getOperation().getName()
        );

        try {
            DbHelper.withRetries(LOG, () -> opDao.create(op, null));
            DbHelper.withRetries(LOG, () -> dao.insertTaskDesc(taskDesc, null));
        } catch (Exception e) {
            LOG.error("Error while scheduling task", e);
            responseObserver.onError(Status.INTERNAL.asException());
            responseObserver.onCompleted();
            return;
        }

        try {
            allocateJob.schedule(op.id(), taskState, null, null);
        } catch (JobSerializer.SerializationException e) {
            LOG.error("Error while scheduling task {}", taskState.id());

            try {
                opDao.failOperation(op.id(), com.google.rpc.Status.newBuilder()
                    .setCode(Status.Code.INTERNAL.value())
                    .setMessage("Internal")
                    .build(), null, LOG);
            } catch (SQLException ex) {
                LOG.error("Error while failing operation {}: ", op.id(), ex);
            }

            responseObserver.onError(Status.INTERNAL.asException());
            responseObserver.onCompleted();
            return;
        }

        responseObserver.onNext(TaskScheduleResponse.newBuilder()
                .setStatus(buildTaskStatus(taskDesc, op))
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void status(TaskStatusRequest request, StreamObserver<TaskStatusResponse> responseObserver) {
        final TaskDao.TaskDesc desc;
        final Operation op;

        try {
            desc = DbHelper.withRetries(LOG, () -> dao.getTaskDesc(request.getTaskId(), request.getWorkflowId(), null));
            op = DbHelper.withRetries(LOG, () -> opDao.get(desc.operationId(), null));

        } catch (Exception e) {
            throw Status.INTERNAL.withDescription("Error while getting data").asRuntimeException();
        }

        responseObserver.onNext(TaskStatusResponse.newBuilder()
                .setStatus(buildTaskStatus(desc, op))
                .buildPartial());
        responseObserver.onCompleted();
    }

    @Override
    public void list(TaskListRequest request, StreamObserver<TaskListResponse> responseObserver) {

        final List<TaskDao.TaskDesc> tasks;

        try {
            tasks = DbHelper.withRetries(LOG, () -> dao.listTasks(request.getWorkflowId(), null));
        } catch (Exception e) {
            LOG.error("Cannot list tasks", e);
            throw Status.INTERNAL.withDescription("Cannot list tasks").asRuntimeException();
        }

        final ArrayList<TaskStatus> statuses = new ArrayList<>();

        for (var task: tasks) {
            final Operation op;
            try {
                op = DbHelper.withRetries(LOG, () -> opDao.get(task.operationId(), null));
            } catch (Exception e) {
                LOG.error("Cannot get op: ", e);
                throw Status.INTERNAL.asRuntimeException();
            }
            statuses.add(buildTaskStatus(task, op));
        }

        responseObserver.onNext(TaskListResponse.newBuilder().addAllStatus(statuses).build());
        responseObserver.onCompleted();
    }

    @Override
    public void stop(TaskStopRequest request, StreamObserver<TaskStopResponse> responseObserver) {
        final TaskDao.TaskDesc desc;
        try {
            desc = DbHelper.withRetries(LOG,
                () -> dao.getTaskDesc(request.getTaskId(), request.getWorkflowId(), null));
        } catch (Exception e) {
            LOG.error("Error while getting task", e);
            throw Status.INTERNAL.asRuntimeException();
        }

        final Operation op;
        try {
            op = DbHelper.withRetries(LOG, () -> opDao.get(desc.operationId(), null));
        } catch (Exception e) {
            LOG.error("Error while getting operation", e);
            throw Status.INTERNAL.asRuntimeException();
        }

        try {
            opDao.failOperation(op.id(), com.google.rpc.Status.newBuilder()
                .setCode(Status.Code.CANCELLED.value())
                .setMessage(request.getIssue())
                .build(), null, LOG);
        } catch (SQLException e) {
            LOG.error("Error while failing op {}", op.id(), e);
            throw Status.INTERNAL.asRuntimeException();
        }


        responseObserver.onNext(TaskStopResponse.newBuilder().setStatus(buildTaskStatus(desc, op)).build());
        responseObserver.onCompleted();
    }

    @Override
    public void killAll(KillAllRequest request, StreamObserver<KillAllResponse> responseObserver) {

        final List<TaskDao.TaskDesc> descList;

        try {
            descList = DbHelper.withRetries(LOG,
                () -> dao.listByWfName(request.getWorkflowName(), request.getUserId(), null));
        } catch (Exception e) {
            LOG.error("Error while listing tasks by wf name", e);
            throw Status.INTERNAL.asRuntimeException();
        }

        for (var task: descList) {
            try {
                opDao.failOperation(task.operationId(), com.google.rpc.Status.newBuilder()
                    .setCode(Status.Code.INTERNAL.value())
                    .setMessage(request.getIssue())
                    .build(), null, LOG);
            } catch (SQLException e) {
                LOG.error("Error while failing op {}", task.operationId(), e);
            }
        }

        responseObserver.onNext(KillAllResponse.newBuilder().build());
        responseObserver.onCompleted();
    }

    private static TaskStatus buildTaskStatus(TaskDao.TaskDesc desc, Operation op) {
        var builder = TaskStatus.newBuilder()
            .setTaskId(desc.taskId())
            .setWorkflowId(desc.executionId())
            .setOperationName(desc.operationName());

        if (!op.done()) {
            return builder.setExecuting(TaskStatus.Executing.getDefaultInstance()).build();
        }

        if (op.error() != null) {
            return builder
                .setError(TaskStatus.Error.newBuilder()
                    .setRc(ReturnCodes.INTERNAL_ERROR.getRc())
                    .setDescription(op.error().getDescription())
                    .build())
                .build();
        }

        final TaskStatus resp;
        try {
            resp = op.response().unpack(TaskStatus.class);
        } catch (InvalidProtocolBufferException e) {
            LOG.error("Error while unpacking op {}: ", JsonUtils.printRequest(op.toProto()), e);
            throw Status.INTERNAL.asRuntimeException();
        }


        return resp;
    }
}
