package ai.lzy.scheduler.jobs;

import ai.lzy.longrunning.dao.OperationDao;
import ai.lzy.model.db.DbHelper;
import ai.lzy.scheduler.JobService;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.JobsOperationDao;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.scheduler.providers.WorkflowJobProvider;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc.LongRunningServiceBlockingStub;
import ai.lzy.v1.scheduler.Scheduler.TaskStatus;
import ai.lzy.v1.worker.LWS;
import com.google.common.net.HostAndPort;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import com.google.rpc.Status;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.micronaut.context.ApplicationContext;
import jakarta.annotation.PreDestroy;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Singleton
public class AwaitExecutionCompleted extends WorkflowJobProvider<TaskState> {
    private final RenewableJwt credentials;
    private final ConcurrentHashMap<HostAndPort, LongRunningServiceBlockingStub> clients = new ConcurrentHashMap<>();
    private final BlockingQueue<ManagedChannel> channels = new LinkedBlockingQueue<>();
    private final OperationDao operationDao;
    private final WorkersAllocator allocator;

    protected AwaitExecutionCompleted(JobService jobService, TaskStateSerializer serializer, JobsOperationDao opDao,
                                      ApplicationContext context, ServiceConfig config,
                                      WorkersAllocator allocator)
    {
        super(jobService, serializer, opDao, AfterAllocation.class, null, context);
        credentials = config.getIam().createRenewableToken();
        this.operationDao = opDao;
        this.allocator = allocator;
    }

    @Override
    protected TaskState exec(TaskState state, String operationId) throws JobProviderException {
        LongRunningServiceBlockingStub client;

        var address = state.workerHost();
        var port = state.workerPort();

        var addr = HostAndPort.fromParts(address, port);

        if (clients.containsKey(addr)) {
            client = clients.get(addr);
        } else {

            var workerChannel = GrpcUtils.newGrpcChannel(addr, LongRunningServiceGrpc.SERVICE_NAME);
            client = GrpcUtils.newBlockingClient(
                    LongRunningServiceGrpc.newBlockingStub(workerChannel),
                    "worker", () -> credentials.get().token());

            clients.put(addr, client);
            try {
                channels.put(workerChannel);
            } catch (InterruptedException e) {
                // ignored
            }
        }

        client = GrpcUtils.withIdempotencyKey(client, state.id());

        final LongRunning.Operation op;
        try {
            op = client.get(LongRunning.GetOperationRequest.newBuilder()
                    .setOperationId(state.workerOperationId())
                    .build());
        } catch (StatusRuntimeException e) {
            logger.error("Error while getting execution status for task {}: ", state.id(), e);
            fail(Status.newBuilder()
                .setCode(io.grpc.Status.Code.INTERNAL.value())
                .setMessage("Some internal exception while waiting for execution")
                .build());
            return null;
        }

        if (!op.getDone()) {
            reschedule(Duration.ofSeconds(1));
            return null;
        }

        if (op.hasError()) {
            logger.error("Task {} execution failed: [{}] {}",
                state.id(), op.getError().getCode(), op.getError().getMessage());

            fail(Status.newBuilder()
                .setCode(io.grpc.Status.Code.INTERNAL.value())
                .setMessage("Some internal error while waiting for execution")
                .build());

            return null;
        }

        final LWS.ExecuteResponse resp;
        try {
            resp = op.getResponse().unpack(LWS.ExecuteResponse.class);
        } catch (InvalidProtocolBufferException e) {
            logger.error("Error while unpacking response {} for task {}: ", JsonUtils.printRequest(op.getResponse()),
                state.id(), e);

            fail(Status.newBuilder()
                .setCode(io.grpc.Status.Code.INTERNAL.value())
                .setMessage("Some internal exception while waiting for execution")
                .build());

            return null;
        }

        final TaskStatus.Builder builder = TaskStatus.newBuilder()
            .setTaskId(state.id())
            .setWorkflowId(state.executionId());

        final TaskStatus status;

        if (resp.getRc() == 0) {
            status = builder
                .setSuccess(TaskStatus.Success.newBuilder()
                    .setRc(0)
                    .setDescription(resp.getDescription())
                    .build()
                )
                .build();
        } else {
            status = builder
                .setError(TaskStatus.Error.newBuilder()
                    .setRc(resp.getRc())
                    .setDescription(resp.getDescription())
                    .build()
                )
                .build();
        }

        try {
            DbHelper.withRetries(logger, () -> {
                operationDao.complete(operationId, Any.pack(status), null);
            });
        } catch (SQLException e) {
            logger.error("Sql exception while updating response for task {}", state.id(), e);
            fail(Status.newBuilder()
                .setCode(Code.INTERNAL.getNumber())
                .setMessage("Error while waiting for operation")
                .build()
            );
            return null;
        } catch (Exception e) {
            logger.error("Some unexpected exception while executing task {}", state.id(), e);
            fail(Status.newBuilder()
                .setCode(Code.INTERNAL.getNumber())
                .setMessage("Error while waiting for operation")
                .build()
            );
            return null;
        }

        allocator.free(state.vmId());

        return state;
    }

    @Override
    protected TaskState clear(TaskState state, String operationId) {
        LongRunningServiceBlockingStub client;

        var address = state.workerHost();
        var port = state.workerPort();

        var addr = HostAndPort.fromParts(address, port);

        if (clients.containsKey(addr)) {
            client = clients.get(addr);
        } else {

            var workerChannel = GrpcUtils.newGrpcChannel(addr, LongRunningServiceGrpc.SERVICE_NAME);
            client = GrpcUtils.newBlockingClient(
                LongRunningServiceGrpc.newBlockingStub(workerChannel),
                "worker", () -> credentials.get().token());

            clients.put(addr, client);
            try {
                channels.put(workerChannel);
            } catch (InterruptedException e) {
                // ignored
            }
        }

        try {
            client.cancel(LongRunning.CancelOperationRequest.newBuilder()
                .setOperationId(state.workerOperationId())
                .build());
        } catch (Exception e) {
            logger.error("Cannot cancel operation on worker: ", e);
        }

        return state.copy()
            .workerOperationId(null)
            .build();
    }

    @PreDestroy
    public void close() {
        for (var channel: channels) {
            channel.shutdown();
        }

        for (var channel: channels) {
            try {
                channel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignored
            }
        }
    }
}
