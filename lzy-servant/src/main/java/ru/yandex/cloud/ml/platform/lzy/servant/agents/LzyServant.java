package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConverter;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.EnvironmentInstallationException;
import ru.yandex.cloud.ml.platform.lzy.model.exceptions.LzyExecutionException;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.SlotConnectionManager;
import yandex.cloud.priv.datasphere.v2.lzy.*;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.SlotCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.SlotCommandStatus;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class LzyServant extends LzyAgent {
    private static final Logger LOG = LogManager.getLogger(LzyServant.class);

    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final Server agentServer;
    private final String contextId;
    private final LzyAgentConfig config;
    private final CompletableFuture<Boolean> started = new CompletableFuture<>();
    private LzyContext context;

    public LzyServant(LzyAgentConfig config) throws URISyntaxException {
        super(config);
        final long start = System.currentTimeMillis();
        contextId = config.getServantId();
        final Impl impl = new Impl();
        final ManagedChannel channel = ChannelBuilder.forAddress(serverAddress.getHost(), serverAddress.getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        server = LzyServerGrpc.newBlockingStub(channel);
        agentServer = NettyServerBuilder.forPort(config.getAgentPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(impl).build();
        this.config = config;

        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "time from agent construct finish to LzyServant construct finish",
                Map.of(
                    "context_id", contextId,
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
                "context_id", contextId,
                "address", agentAddress.toString()
            ),
            UserEvent.UserEventType.TaskStartUp
        ));
        status.set(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(auth);
        commandBuilder.setServantURI(agentAddress.toString());
        commandBuilder.setServantId(contextId);
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());
        status.set(AgentStatus.REGISTERED);

        // [TODO] this trash must be removed somehow, the only usage of this field is to determine cloud
        // environment type, which is completely incorrect, since storage location could differ from the compute
        final SlotConnectionManager slotsManager =
            new SlotConnectionManager(server, auth, config.getWhiteboardAddress(), config.getBucket(), contextId);

        context = new LzyContext(contextId, slotsManager, agentInternalAddress);

        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "LzyServant startUp time",
                Map.of(
                    "context_id", contextId,
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
                        context().prepare(GrpcConverter.from(request));
                    } catch (EnvironmentInstallationException e) {
                        LOG.error("Unable to install environment", e);
                        result.setRc(-1);
                        result.setDescription(e.getMessage());
                    }
                    responseObserver.onNext(result.build());
                    responseObserver.onCompleted();
                });
        }

        @Override
        public void start(IAM.Empty request, StreamObserver<Servant.ServantProgress> responseObserver) {
            waitForStart();
            LzyServant.this.context.onProgress(progress -> {
                responseObserver.onNext(progress);
                if (progress.getStatusCase() == Servant.ServantProgress.StatusCase.EXIT) {
                    responseObserver.onCompleted();
                }
            });
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
            final String tid = request.getTid();
            UserEventLogger.log(new UserEvent(
                "Servant execution preparing",
                Map.of(
                    "task_id", tid,
                    "zygote_description", zygote.description()
                ),
                UserEvent.UserEventType.ExecutionPreparing
            ));

            try {
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

            } catch (LzyExecutionException | InterruptedException e) {
                responseObserver.onError(
                    Status.INTERNAL
                        .withDescription(e.getMessage()).withCause(e.getCause()).asException()
                );
                return;
            }
            status.set(AgentStatus.EXECUTING);
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();
        }

        private void stop() {
            lzyFS.umount();
            agentServer.shutdown();
        }

        @Override
        public void stop(IAM.Empty request, StreamObserver<IAM.Empty> responseObserver) {
            LOG.info("Servant::stop {}", agentAddress);
            try {
                context.close();
            } catch (InterruptedException e) {
                LOG.error("Failed to wait until all output slots successfully read");
            }

            responseObserver.onNext(IAM.Empty.newBuilder().build());
            responseObserver.onCompleted();
            UserEventLogger.log(new UserEvent(
                "Servant task exit",
                Map.of(
                    "task_id", contextId,
                    "address", agentAddress.toString(),
                    "exit_code", String.valueOf(0)
                ),
                UserEvent.UserEventType.TaskStop
            ));
            stop();
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request,
                                   StreamObserver<Servant.Message> responseObserver) {
            final long start = System.currentTimeMillis();
            LOG.info("LzyServant::openOutputSlot " + JsonUtils.printRequest(request));
            final Path path = Paths.get(URI.create(request.getSlotUri()).getPath());
            final String tid;
            final String slotName;
            if (path.getNameCount() < 2) {
                responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Wrong slot format, must be [task]/[slot]").asException());
                return;
            } else {
                tid = path.getName(0).toString();
                slotName = path.getName(0).relativize(path).toString();
                System.out.println("tid: " + tid + " slot: " + slotName);
            }

            final LzySlot slot = context.slot(tid, slotName);
            if (!(slot instanceof LzyOutputSlot)) {
                LOG.info("Trying to read from input slot " + path);
                responseObserver
                    .onError(Status.NOT_FOUND.withDescription("Reading from input slot: " + path).asException());
                return;
            }
            final LzyOutputSlot outputSlot = (LzyOutputSlot) slot;
            try {
                outputSlot.readFromPosition(request.getOffset())
                    .forEach(chunk -> responseObserver.onNext(Servant.Message.newBuilder().setChunk(chunk).build()));
                responseObserver.onNext(Servant.Message.newBuilder().setControl(Servant.Message.Controls.EOS).build());
                responseObserver.onCompleted();
            } catch (IOException iae) {
                responseObserver.onError(iae);
            }
            final long finish = System.currentTimeMillis();
            MetricEventLogger.log(
                new MetricEvent(
                    "LzyServant openOutputSlot time",
                    Map.of(
                        "task_id", contextId,
                        "metric_type", "system_metric"
                    ),
                    finish - start
                )
            );
        }

        @Override
        public void configureSlot(SlotCommand request, StreamObserver<SlotCommandStatus> responseObserver) {
            LzyServant.this.configureSlot(request, responseObserver);
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
