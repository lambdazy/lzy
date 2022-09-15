package ai.lzy.model.scheduler;

import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.ClientHeaderInterceptor;
import ai.lzy.util.grpc.GrpcHeaders;
import ai.lzy.v1.scheduler.SchedulerPrivateApi;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.Executing;
import ai.lzy.v1.scheduler.SchedulerPrivateApi.ServantProgress.Idle;
import ai.lzy.v1.scheduler.SchedulerPrivateGrpc;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.StringReader;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
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
    private final int apiPort;
    private final ManagedChannel channel;

    private final SchedulerPrivateGrpc.SchedulerPrivateBlockingStub stub;
    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private final BlockingQueue<ServantProgress> progressQueue = new LinkedBlockingQueue<>();
    private final Timer timer = new Timer();
    private final AtomicReference<TimerTask> task = new AtomicReference<>();

    public SchedulerAgent(String schedulerAddress, String servantId, String workflowName,
                          Duration heartbeatPeriod, int apiPort, String iamPrivateKey)
    {
        super("scheduler-agent-" + servantId);
        this.servantId = servantId;
        this.workflowName = workflowName;
        this.heartbeatPeriod = heartbeatPeriod;
        this.apiPort = apiPort;

        String jwt;
        try {
            jwt = JwtUtils.buildJWT(servantId, "INTERNAL", Date.from(Instant.now()), JwtUtils.afterDays(7),
                new StringReader(iamPrivateKey));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        channel = ChannelBuilder.forAddress(schedulerAddress)
            .usePlaintext()
            .enableRetry(SchedulerPrivateGrpc.SERVICE_NAME)
            .build();
        stub = SchedulerPrivateGrpc.newBlockingStub(channel)
            .withInterceptors(ClientHeaderInterceptor.header(GrpcHeaders.AUTHORIZATION, () -> jwt));
    }

    public void start() {
        super.start();

        try {
            stub.registerServant(SchedulerPrivateApi.RegisterServantRequest.newBuilder()
                .setServantId(servantId)
                .setWorkflowName(workflowName)
                .setApiPort(apiPort)
                .build());
        } catch (StatusRuntimeException e) {
            LOG.error("Error while registering agent");
            shutdown();
            throw new RuntimeException(e);
        }
    }

    public synchronized void reportProgress(ServantProgress progress) {
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

            var retryCount = 0;
            while (++retryCount < 5) {
                try {
                    stub.servantProgress(SchedulerPrivateApi.ServantProgressRequest.newBuilder()
                        .setServantId(servantId)
                        .setWorkflowName(workflowName)
                        .setProgress(progress)
                        .build());
                    break;
                } catch (StatusRuntimeException e) {
                    LOG.error("Cannot send progress to scheduler: {}. Retrying...", e.getStatus());
                }
            }

            if (retryCount == 5) {
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
                    reportProgress(ServantProgress.newBuilder()
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
                    reportProgress(ServantProgress.newBuilder()
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
