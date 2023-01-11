package ai.lzy.scheduler;

import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.scheduler.SchedulerPrivateApi;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Executing;
import ai.lzy.v1.scheduler.SchedulerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;


public class SchedulerAgent implements AutoCloseable {
    private static final Logger LOG = LogManager.getLogger(SchedulerAgent.class);

    private final String taskId;
    private final String executionId;
    private final ManagedChannel channel;

    private final SchedulerPrivateGrpc.SchedulerPrivateBlockingStub stub;
    private final Timer timer = new Timer();
    private final TimerTask task;

    public SchedulerAgent(String schedulerAddress, String taskId, String executionId,
                          Duration heartbeatPeriod, String iamPrivateKey)
    {
        this.taskId = taskId;
        this.executionId = executionId;

        RenewableJwt jwt;
        try {
            jwt = new RenewableJwt(this.taskId, "INTERNAL", Duration.ofMinutes(15),
                CredentialsUtils.readPrivateKey(iamPrivateKey));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        channel = newGrpcChannel(schedulerAddress, SchedulerPrivateGrpc.SERVICE_NAME);
        stub = newBlockingClient(SchedulerPrivateGrpc.newBlockingStub(channel), "SA", () -> jwt.get().token());

        task = new TimerTask() {
            @Override
            public void run() {
                reportProgress(WorkerProgress.newBuilder()
                    .setExecuting(Executing.newBuilder().build())
                    .build());
            }
        };
        timer.scheduleAtFixedRate(task, heartbeatPeriod.toMillis(), heartbeatPeriod.toMillis());
    }

    private synchronized void reportProgress(WorkerProgress progress) {
        try {
            stub.workerProgress(
                SchedulerPrivateApi.WorkerProgressRequest.newBuilder()
                    .setTaskId(taskId)
                    .setExecutionId(executionId)
                    .setProgress(progress)
                    .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while reporting progress", e);
            throw e;
        }
    }

    public void reportExecutionCompleted(int rc, String description) {
        reportProgress(WorkerProgress.newBuilder()
            .setExecutionCompleted(WorkerProgress.ExecutionCompleted.newBuilder()
                .setRc(rc)
                .setDescription(description)
            )
            .build());
    }

    public synchronized void close() {
        task.cancel();
        timer.cancel();
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error while stopping scheduler agent", e);
        }
    }

}
