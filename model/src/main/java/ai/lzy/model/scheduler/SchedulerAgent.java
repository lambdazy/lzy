package ai.lzy.model.scheduler;

import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.lzy.SchedulerPrivateApi;
import ai.lzy.v1.lzy.SchedulerPrivateApi.ServantProgress;
import ai.lzy.v1.lzy.SchedulerPrivateApi.ServantProgress.Executing;
import ai.lzy.v1.lzy.SchedulerPrivateApi.ServantProgress.Idle;
import ai.lzy.v1.lzy.SchedulerPrivateGrpc;
import ai.lzy.v1.lzy.SchedulerPrivateGrpc.SchedulerPrivateBlockingStub;
import io.grpc.ManagedChannel;
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

public class SchedulerAgent extends Thread {
    private static final Logger LOG = LogManager.getLogger(SchedulerAgent.class);

    private final String servantId;
    private final String workflowName;
    private final Duration heartbeatPeriod;
    private final ManagedChannel channel;

    private final SchedulerPrivateBlockingStub stub;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final BlockingQueue<ServantProgress> progressQueue = new LinkedBlockingQueue<>();
    private final Timer timer = new Timer();
    private final AtomicReference<TimerTask> task = new AtomicReference<>();

    public SchedulerAgent(String schedulerAddress, String servantId, String workflowName,
                          Duration heartbeatPeriod, int apiPort, String iamToken) {
        super("scheduler-agent-" + servantId);
        this.servantId = servantId;
        this.workflowName = workflowName;
        this.heartbeatPeriod = heartbeatPeriod;
        channel = ChannelBuilder.forAddress(schedulerAddress)
            .usePlaintext()
            .enableRetry(SchedulerPrivateGrpc.SERVICE_NAME)
            .build();
        stub = SchedulerPrivateGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> iamToken));

        this.start();

        stub.registerServant(SchedulerPrivateApi.RegisterServantRequest.newBuilder()
            .setServantId(servantId)
            .setWorkflowName(workflowName)
            .setApiPort(apiPort)
            .build());
    }

    public synchronized void progress(ServantProgress progress) {
        try {
            progressQueue.put(progress);
        } catch (InterruptedException e) {
            throw new RuntimeException("Must be unreachable");
        }
    }

    @Override
    public void run() {
        while (!stopping.get()) {
            final ServantProgress progress;
            try {
                progress = progressQueue.take();
            } catch (InterruptedException e) {
                continue;
            }

            stub.servantProgress(SchedulerPrivateApi.ServantProgressRequest.newBuilder()
                .setServantId(servantId)
                .setWorkflowName(workflowName)
                .setProgress(progress)
                .build());
        }
    }

    public synchronized void idling() {
        if (task.get() != null) {
            task.get().cancel();
            timer.purge();
        }
        task.set(
            new TimerTask() {
                @Override
                public void run() {
                    progress(ServantProgress.newBuilder()
                        .setIdling(Idle.newBuilder().build())
                        .build());
                }
            });
        timer.scheduleAtFixedRate(task.get(), heartbeatPeriod.toMillis(), heartbeatPeriod.toMillis());
    }

    public synchronized void executing() {
        if (task.get() != null) {
            task.get().cancel();
            timer.purge();
        }
        task.set(
            new TimerTask() {
                @Override
                public void run() {
                    progress(ServantProgress.newBuilder()
                        .setExecuting(Executing.newBuilder().build())
                        .build());
                }
            });
        timer.scheduleAtFixedRate(task.get(), heartbeatPeriod.toMillis(), heartbeatPeriod.toMillis());
    }

    public synchronized void stopping() {
        if (task.get() != null) {
            task.get().cancel();
            timer.purge();
        }
        stopping.set(true);
    }

    public void shutdown() {
        stopping();
        this.interrupt();
        channel.shutdown();
        try {
            channel.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.error("Error while stopping scheduler agent", e);
        }
    }

}
