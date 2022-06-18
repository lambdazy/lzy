package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.model.Context;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.ReturnCodes;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEventLogger;
import ru.yandex.cloud.ml.platform.lzy.storage.StorageClient;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.LzyServant;

public class LzyServant extends LzyAgent {
    private static final Logger LOG = LogManager.getLogger(LzyServant.class);

    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final URI agentAddress;
    private final Server agentServer;
    private final CompletableFuture<Boolean> started = new CompletableFuture<>();
    private LzyContext context;

    public LzyServant(LzyAgentConfig config) throws URISyntaxException, IOException {
        super(config);
        final long start = System.currentTimeMillis();

        final ManagedChannel channel = ChannelBuilder
            .forAddress(config.getServerAddress().getHost(), config.getServerAddress().getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        server = LzyServerGrpc.newBlockingStub(channel);

        agentAddress =
            new URI(LzyServant.scheme(), null, config.getAgentHost(), config.getAgentPort(), null, null, null);

        agentServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(agentAddress.getHost(), agentAddress.getPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.SECONDS)
            .keepAliveTimeout(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.SECONDS)
            .keepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS, TimeUnit.MINUTES)
            .addService(new Impl())
            .build();

        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "time from agent construct finish to LzyServant construct finish",
                Map.of(
                    "context_id", config.getServantId(),
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    @Override
    protected LzyContext context() {
        return context;
    }

    @Override
    protected URI serverUri() {
        return agentAddress;
    }

    @Override
    protected Server server() {
        return agentServer;
    }

    protected void started() {
        started.complete(true);
    }

    private void waitForStart() {
        try {
            started.get();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error(e);
        }
    }

    @Override
    protected void onStartUp() {
        final long start = System.currentTimeMillis();
        UserEventLogger.log(new UserEvent(
            "Servant startup",
            Map.of(
                "context_id", config.getServantId(),
                "address", agentAddress.toString()
            ),
            UserEvent.UserEventType.TaskStartUp
        ));
        status.set(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(auth);
        commandBuilder.setServantURI(agentAddress.toString());
        commandBuilder.setFsURI(lzyFs.getUri().toString());
        commandBuilder.setServantId(config.getServantId());
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());
        status.set(AgentStatus.REGISTERED);

        context = new LzyContext(config.getServantId(), lzyFs.getSlotsManager(),
            lzyFs.getMountPoint().toString());

        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "LzyServant startUp time",
                Map.of(
                    "context_id", config.getServantId(),
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    @Override
    protected LzyServerGrpc.LzyServerBlockingStub serverApi() {
        return server;
    }

    private void forceStop(String reason, Throwable th) {
        LOG.error("Force terminate servant {}: {}", config.getServantId(), reason, th);
        agentServer.shutdownNow();
        lzyFs.forceStop();
    }

    private class Impl extends LzyServantGrpc.LzyServantImplBase {
        private LzyExecution currentExecution;

        @Override
        public void env(Operations.EnvSpec request, StreamObserver<Servant.EnvResult> responseObserver) {
            LOG.info("LzyServant::prepare " + JsonUtils.printRequest(request));
            MetricEventLogger.timeIt(
                "time of context preparing",
                Map.of("metric_type", "system_metric"),
                () -> {
                    final Servant.EnvResult.Builder result = Servant.EnvResult.newBuilder();
                    try {
                        final String bucket = server.getBucket(Lzy.GetBucketRequest
                            .newBuilder().setAuth(auth).build()).getBucket();
                        final Lzy.GetS3CredentialsResponse credentials = server.getS3Credentials(
                            Lzy.GetS3CredentialsRequest.newBuilder()
                                .setBucket(bucket)
                                .setAuth(auth)
                                .build()
                        );
                        context().prepare(GrpcConverter.from(request), StorageClient.create(credentials));
                    } catch (EnvironmentInstallationException e) {
                        LOG.error("Unable to install environment", e);
                        result.setRc(ReturnCodes.ENVIRONMENT_INSTALLATION_ERROR.getRc());
                        result.setDescription(e.getMessage());
                    }
                    responseObserver.onNext(result.build());
                    responseObserver.onCompleted();
                });
        }

        @Override
        public void start(IAM.Empty request, StreamObserver<Servant.ServantProgress> responseObserver) {
            waitForStart();
            LzyServant.this.context.onProgress(responseObserver::onNext);
            LzyServant.this.context.start();
        }

        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            LOG.info("LzyServant::execute " + JsonUtils.printRequest(request));
            if (status.get() == AgentStatus.EXECUTING) {
                responseObserver.onError(Status.RESOURCE_EXHAUSTED.withDescription("Already executing").asException());
                return;
            }

            status.set(AgentStatus.PREPARING_EXECUTION);
            final AtomicZygote zygote = (AtomicZygote) GrpcConverter.from(request.getZygote());
            final Stream<Context.SlotAssignment> assignments = GrpcConverter.from(
                request.getAssignmentsList().stream()
            );
            final String tid = request.getTid();
            UserEventLogger.log(new UserEvent(
                "Servant execution preparing",
                Map.of(
                    "task_id", tid,
                    "zygote_description", zygote.description()
                ),
                UserEvent.UserEventType.ExecutionPreparing
            ));
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();

            try {
                assignments.map(
                        entry -> {
                            LzySlot slot = context.configureSlot(tid, entry.slot(), entry.binding());
                            // TODO: It will be removed after creating Portal
                            final String channelName;
                            if (entry.binding().startsWith("channel:")) {
                                channelName = entry.binding().substring("channel:".length());
                            } else {
                                channelName = entry.binding();
                            }
                            final URI channelUri = URI.create(channelName);
                            if (Objects.equals(channelUri.getScheme(), "snapshot") && slot instanceof LzyOutputSlot) {
                                String snapshotId = "snapshot://" + channelUri.getHost();
                                lzyFs.getSlotConnectionManager().snapshooter()
                                        .registerSlot(slot, snapshotId, channelName);
                            }
                            return slot;
                        }
                ).forEach(slot -> {
                    if (slot instanceof LzyFileSlot) {
                        lzyFs.addSlot((LzyFileSlot) slot);
                    }
                });
                currentExecution = context.execute(tid, zygote, progress -> {
                    LOG.info("LzyServant::progress {} {}", agentAddress, JsonUtils.printRequest(progress));
                    UserEventLogger.log(new UserEvent(
                        "Servant execution progress",
                        Map.of(
                            "task_id", tid,
                            "zygote_description", zygote.description(),
                            "progress", JsonUtils.printRequest(progress)
                        ),
                        UserEvent.UserEventType.ExecutionProgress
                    ));
                    if (progress.hasExecuteStop()) {
                        UserEventLogger.log(new UserEvent(
                            "Servant execution exit",
                            Map.of(
                                "task_id", tid,
                                "zygote_description", zygote.description(),
                                "exit_code", String.valueOf(progress.getExecuteStop().getRc())
                            ),
                            UserEvent.UserEventType.ExecutionComplete
                        ));
                        LOG.info("LzyServant::exit {}", agentAddress);
                        status.set(AgentStatus.REGISTERED);
                    }
                });
            } catch (Exception e) {
                forceStop("Error while execution", e);
                return;
            }
            status.set(AgentStatus.EXECUTING);
        }

        @Override
        public void stop(IAM.Empty request, StreamObserver<IAM.Empty> responseObserver) {
            LOG.info("Servant::stop {}", agentAddress);
            context.close();

            responseObserver.onNext(IAM.Empty.newBuilder().build());
            responseObserver.onCompleted();
            UserEventLogger.log(new UserEvent(
                "Servant task exit",
                Map.of(
                    "task_id", config.getServantId(),
                    "address", agentAddress.toString(),
                    "exit_code", String.valueOf(0)
                ),
                UserEvent.UserEventType.TaskStop
            ));
            try {
                agentServer.shutdown();
                lzyFs.stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void signal(Tasks.TaskSignal request, StreamObserver<IAM.Empty> responseObserver) {
            if (status.get().getValue() < AgentStatus.EXECUTING.getValue()) {
                responseObserver.onError(Status.ABORTED.asException());
                return;
            }
            if (status.get() != AgentStatus.EXECUTING) {
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Cannot send signal when servant not executing").asException());
                return;
            }
            currentExecution.signal(request.getSigValue());
            responseObserver.onNext(IAM.Empty.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void update(IAM.Auth request, StreamObserver<IAM.Empty> responseObserver) {
            LzyServant.this.update(request, responseObserver);
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            LzyServant.this.status(request, responseObserver);
        }
    }
}
