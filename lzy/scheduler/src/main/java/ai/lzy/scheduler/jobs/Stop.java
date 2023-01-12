package ai.lzy.scheduler.jobs;

import ai.lzy.model.db.TransactionHandle;
import ai.lzy.scheduler.allocator.WorkersAllocator;
import ai.lzy.scheduler.configs.ServiceConfig;
import ai.lzy.scheduler.db.TaskDao;
import ai.lzy.scheduler.db.impl.SchedulerDataSource;
import ai.lzy.scheduler.models.TaskState;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.util.grpc.GrpcUtils;
import ai.lzy.v1.longrunning.LongRunning;
import ai.lzy.v1.longrunning.LongRunningServiceGrpc;
import jakarta.inject.Singleton;

import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@Singleton
public class Stop extends SchedulerJob {

    private final RenewableJwt credentials;
    private final WorkersAllocator allocator;

    protected Stop(TaskDao dao, SchedulerDataSource storage, ServiceConfig config, WorkersAllocator allocator) {
        super(dao, storage);
        credentials = config.getIam().createRenewableToken();
        this.allocator = allocator;
    }

    @Override
    protected void execute(TaskState task, TransactionHandle tx) throws SQLException {
        if (task.workerOperationId() != null) {
            var workerChannel = GrpcUtils.newGrpcChannel(task.workerAddress(), LongRunningServiceGrpc.SERVICE_NAME);
            var client = GrpcUtils.newBlockingClient(
                    LongRunningServiceGrpc.newBlockingStub(workerChannel),
                    "worker", () -> credentials.get().token());

            client = GrpcUtils.withIdempotencyKey(client, task.id());
            client.cancel(LongRunning.CancelOperationRequest.newBuilder()
                .setOperationId(task.workerOperationId())
                .build());

            workerChannel.shutdownNow();
            try {
                workerChannel.awaitTermination(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("Interrupted while awaiting termination", e);
            }
        }

        if (task.vmId() != null) {
            allocator.free(task.vmId());
        }
    }
}
