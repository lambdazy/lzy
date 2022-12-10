package ai.lzy.scheduler;

import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RenewableJwt;
import ai.lzy.v1.scheduler.SchedulerPrivateApi;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Executing;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.WorkerProgress.Idle;
import ai.lzy.v1.scheduler.SchedulerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static ai.lzy.util.grpc.GrpcUtils.newBlockingClient;
import static ai.lzy.util.grpc.GrpcUtils.newGrpcChannel;


public class SchedulerAgent extends Thread {
    private static final Logger LOG = LogManager.getLogger(SchedulerAgent.class);

    private final String workerId;
    private final String workflowName;
    private final Duration heartbeatPeriod;
    private final int apiPort;
    private final ManagedChannel channel;

    private final SchedulerPrivateGrpc.SchedulerPrivateBlockingStub stub;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final BlockingQueue<WorkerProgress> progressQueue = new LinkedBlockingQueue<>();
    private final Timer timer = new Timer();
    private final AtomicReference<TimerTask> task = new AtomicReference<>();

    public SchedulerAgent(String schedulerAddress, String workerId, String workflowName,
                          Duration heartbeatPeriod, int apiPort, String iamPrivateKey)
    {
        super("scheduler-agent-" + workerId);
        this.workerId = workerId;
        this.workflowName = workflowName;
        this.heartbeatPeriod = heartbeatPeriod;
        this.apiPort = apiPort;

        RenewableJwt jwt;
        try {
            jwt = new RenewableJwt(workerId, "INTERNAL", Duration.ofDays(1),
                CredentialsUtils.readPrivateKey(iamPrivateKey));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        channel = newGrpcChannel(schedulerAddress, SchedulerPrivateGrpc.SERVICE_NAME);
        stub = newBlockingClient(SchedulerPrivateGrpc.newBlockingStub(channel), "SA", () -> jwt.get().token());
    }

    public void start() {
        super.start();

        try {
            //noinspection ResultOfMethodCallIgnored
            stub.registerWorker(SchedulerPrivateApi.RegisterWorkerRequest.newBuilder()
                .setWorkerId(workerId)
                .setWorkflowName(workflowName)
                .setApiPort(apiPort)
                .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while registering agent");
            shutdown();
            throw new RuntimeException(e);
        }
    }

    public synchronized void reportProgress(WorkerProgress progress) {
        try {
            progressQueue.put(progress);
        } catch (InterruptedException e) {
            throw new RuntimeException("Must be unreachable");
        }
    }

    @Override
    public void run() {
        while (!stopping.get()) {
            final WorkerProgress progress;
            try {
                progress = progressQueue.take();
            } catch (InterruptedException e) {
                continue;
            }

            var retryCount = 0;
            while (++retryCount < 5) {
                try {
                    //noinspection ResultOfMethodCallIgnored
                    stub.workerProgress(
                        SchedulerPrivateApi.WorkerProgressRequest.newBuilder()
                            .setWorkerId(workerId)
                            .setWorkflowName(workflowName)
                            .setProgress(progress)
                            .build());
                    break;
                } catch (StatusRuntimeException e) {
                    if (stopping.get()) {
                        return;
                    }
                    LOG.error("Cannot send progress to scheduler: {}. Retrying...", e.getStatus());
                    LockSupport.parkNanos(Duration.ofMillis(100L * retryCount).toNanos());
                }
            }

            if (retryCount == 5 && !stopping.get()) {
                LOG.error("Cannot send progress to scheduler. Stopping thread");
                throw new RuntimeException("Cannot send progress to scheduler");
            }
        }
    }

    public synchronized void reportIdle() {
        if (task.get() != null) {
            task.get().cancel();
            timer.purge();
        }
        task.set(
            new TimerTask() {
                @Override
                public void run() {
                    reportProgress(WorkerProgress.newBuilder()
                        .setIdling(Idle.newBuilder().build())
                        .build());
                }
            });
        timer.scheduleAtFixedRate(task.get(), heartbeatPeriod.toMillis(), heartbeatPeriod.toMillis());
    }

    public synchronized void reportExecuting() {
        if (task.get() != null) {
            task.get().cancel();
            timer.purge();
        }
        task.set(
            new TimerTask() {
                @Override
                public void run() {
                    reportProgress(WorkerProgress.newBuilder()
                        .setExecuting(Executing.newBuilder().build())
                        .build());
                }
            });
        timer.scheduleAtFixedRate(task.get(), heartbeatPeriod.toMillis(), heartbeatPeriod.toMillis());
    }

    public synchronized void reportStop() {
        if (task.get() != null) {
            task.get().cancel();
            timer.purge();
        }
        stopping.set(true);
    }

    public void shutdown() {
        reportStop();
        this.interrupt();
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error while stopping scheduler agent", e);
        }
    }

}
