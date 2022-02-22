package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SnapshotterImpl;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage.SnapshotStorage;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.GetS3CredentialsResponse;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ContextProgress;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.ContextProgress.StatusCase;
import yandex.cloud.priv.datasphere.v2.lzy.SnapshotApiGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks.ContextSpec;

public class LzyServant extends LzyAgent {

    private static final Logger LOG = LogManager.getLogger(LzyServant.class);
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot;
    private final Server agentServer;
    private final String taskId;
    private final String bucket;
    private final SnapshotStorage storage;
    private final GetS3CredentialsResponse credentials;

    public LzyServant(LzyAgentConfig config) throws URISyntaxException {
        super(config);
        final long start = System.currentTimeMillis();
        taskId = config.getTask();
        URI whiteboardAddress = config.getWhiteboardAddress();
        final Impl impl = new Impl();
        final ManagedChannel channel = ChannelBuilder
            .forAddress(serverAddress.getHost(), serverAddress.getPort())
            .usePlaintext()
            .enableRetry(LzyServerGrpc.SERVICE_NAME)
            .build();
        server = LzyServerGrpc.newBlockingStub(channel);
        final ManagedChannel channelWb = ChannelBuilder
            .forAddress(whiteboardAddress.getHost(), whiteboardAddress.getPort())
            .usePlaintext()
            .enableRetry(SnapshotApiGrpc.SERVICE_NAME)
            .build();
        snapshot = SnapshotApiGrpc.newBlockingStub(channelWb);
        agentServer = NettyServerBuilder.forPort(config.getAgentPort())
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(impl).build();
        bucket = config.getBucket();
        Lzy.GetS3CredentialsResponse resp = server
            .getS3Credentials(
                Lzy.GetS3CredentialsRequest.newBuilder()
                    .setAuth(auth)
                    .setBucket(bucket)
                    .build()
            );

        credentials = resp;

        storage = SnapshotStorage.create(resp);
        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "time from agent construct finish to LzyServant construct finish",
                Map.of(
                    "task_id", taskId,
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    @Override
    protected Server server() {
        return agentServer;
    }

    @Override
    protected void onStartUp() {
        final long start = System.currentTimeMillis();
        UserEventLogger.log(new UserEvent(
            "Servant startup",
            Map.of(
                "task_id", taskId,
                "address", agentAddress.toString()
            ),
            UserEvent.UserEventType.TaskStartUp
        ));
        status.set(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(auth);
        commandBuilder.setServantURI(agentAddress.toString());
        commandBuilder.setSessionId(taskId);
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());
        status.set(AgentStatus.REGISTERED);
        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "LzyServant startUp time",
                Map.of(
                    "task_id", taskId,
                    "metric_type", "system_metric"
                ),
                finish - start
            )
        );
    }

    @Override
    protected LzyServerApi lzyServerApi() {
        return server::zygotes;
    }

    private class Impl extends LzyServantGrpc.LzyServantImplBase {

        private LzyExecution currentExecution;
        private final AtomicBoolean executing = new AtomicBoolean(false);

        @Override
        public void prepare(ContextSpec request, StreamObserver<ContextProgress> responseObserver) {
            LOG.info("LzyServant::prepare " + JsonUtils.printRequest(request));
            ru.yandex.cloud.ml.platform.lzy.model.Context context = GrpcConverter.from(request);
            if (inContext.get()) {
                responseObserver.onError(Status.ALREADY_EXISTS.withDescription("Context already prepared")
                    .asException());
                return;
            }
            final Snapshotter snapshotter = new SnapshotterImpl(auth.getTask(), bucket,
                snapshot, storage, context.assignments(), context.meta());
            LzyServant.this.context = new LzyContext(taskId, snapshotter, agentInternalAddress,
                credentials);
            inContext.set(true);
            LzyServant.this.context.onProgress(progress -> {
                responseObserver.onNext(progress);
                if (progress.getStatusCase() == StatusCase.EXIT) {
                    responseObserver.onCompleted();
                    inContext.set(false);
                }
            });
            MetricEventLogger.timeIt(
                "time of context preparing",
                Map.of("metric_type", "system_metric"),
                () -> {
                    try {
                        LzyServant.this.context.prepare(lzyFS, context);
                    } catch (EnvironmentInstallationException e) {
                        LOG.info(e);
                        responseObserver.onCompleted();
                    }
                });
        }

        @Override
        public void execute(Tasks.TaskSpec request,
                            StreamObserver<Servant.ExecutionProgress> responseObserver) {
            status.set(AgentStatus.PREPARING_EXECUTION);
            LOG.info("LzyServant::execute " + JsonUtils.printRequest(request));
            if (executing.get()) {
                responseObserver.onError(Status.RESOURCE_EXHAUSTED.withDescription("Already executing").asException());
                return;
            }

            if (!inContext.get()) {
                responseObserver.onError(
                    Status.NOT_FOUND.withDescription("Running execute without context").asException());
                return;
            }

            final AtomicZygote zygote = (AtomicZygote) GrpcConverter.from(request.getZygote());

            UserEventLogger.log(new UserEvent(
                "Servant execution preparing",
                Map.of(
                    "task_id", request.getAuth().getTask().getTaskId(),
                    "zygote_description", zygote.description()
                ),
                UserEvent.UserEventType.ExecutionPreparing
            ));

            try {
                executing.set(true);
                currentExecution = context.execute(zygote, progress -> {
                    LOG.info("LzyServant::progress {} {}", agentAddress,
                        JsonUtils.printRequest(progress));
                    UserEventLogger.log(new UserEvent(
                        "Servant execution progress",
                        Map.of(
                            "task_id", request.getAuth().getTask().getTaskId(),
                            "zygote_description", zygote.description(),
                            "progress", JsonUtils.printRequest(progress)
                        ),
                        UserEvent.UserEventType.ExecutionProgress
                    ));
                    responseObserver.onNext(progress);
                    if (progress.hasExit()) {
                        UserEventLogger.log(new UserEvent(
                            "Servant execution exit",
                            Map.of(
                                "task_id", request.getAuth().getTask().getTaskId(),
                                "zygote_description", zygote.description(),
                                "exit_code", String.valueOf(progress.getExit().getRc())
                            ),
                            UserEvent.UserEventType.ExecutionComplete
                        ));
                        LOG.info("LzyServant::exit {}", agentAddress);
                        executing.set(false);
                        responseObserver.onCompleted();
                    }
                });

            } catch (LzyExecutionException | InterruptedException e) {
                responseObserver.onError(
                    Status.INTERNAL
                        .withDescription(e.getMessage()).withCause(e.getCause()).asException()
                );
                return;
            }
            Context.current().addListener(context -> {
                if (executing.get()) {
                    LOG.info("Execution terminated from server ");
                    UserEventLogger.log(new UserEvent(
                        "Servant task exit",
                        Map.of(
                            "task_id", taskId,
                            "address", agentAddress.toString(),
                            "exit_code", String.valueOf(1)
                        ),
                        UserEvent.UserEventType.TaskStop
                    ));
                    System.exit(1);
                }
            }, Runnable::run);

            status.set(AgentStatus.EXECUTING);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request,
                                   StreamObserver<Servant.Message> responseObserver) {
            final long start = System.currentTimeMillis();
            LOG.info("LzyServant::openOutputSlot " + JsonUtils.printRequest(request));
            if (!inContext.get() || context.slot(request.getSlot()) == null) {
                LOG.info("Not found slot: " + request.getSlot());
                responseObserver
                    .onError(Status.NOT_FOUND
                        .withDescription("Not found slot: " + request.getSlot()).asException());
                return;
            }
            final LzyOutputSlot slot = (LzyOutputSlot) context.slot(request.getSlot());
            try {
                slot.readFromPosition(request.getOffset())
                    .forEach(chunk -> responseObserver
                        .onNext(Servant.Message.newBuilder().setChunk(chunk).build()));
                responseObserver.onNext(
                    Servant.Message.newBuilder().setControl(Servant.Message.Controls.EOS).build());
                responseObserver.onCompleted();
            } catch (IOException iae) {
                responseObserver.onError(iae);
            }
            final long finish = System.currentTimeMillis();
            MetricEventLogger.log(
                new MetricEvent(
                    "LzyServant openOutputSlot time",
                    Map.of(
                        "task_id", taskId,
                        "metric_type", "system_metric"
                    ),
                    finish - start
                )
            );
        }

        @Override
        public void configureSlot(
            Servant.SlotCommand request,
            StreamObserver<Servant.SlotCommandStatus> responseObserver
        ) {
            LzyServant.this.configureSlot(request, responseObserver);
        }

        @Override
        public void signal(Tasks.TaskSignal request,
                           StreamObserver<Servant.ExecutionStarted> responseObserver) {
            if (status.get().getValue() < AgentStatus.EXECUTING.getValue()) {
                responseObserver.onError(Status.ABORTED.asException());
                return;
            }
            if (!executing.get()) {
                responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Cannot send signal when servant not executing").asException());
                return;
            }
            currentExecution.signal(request.getSigValue());
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void update(IAM.Auth request,
                           StreamObserver<Servant.ExecutionStarted> responseObserver) {
            LzyServant.this.update(request, responseObserver);
        }

        @Override
        public void status(IAM.Empty request,
                           StreamObserver<Servant.ServantStatus> responseObserver) {
            LzyServant.this.status(request, responseObserver);
        }

        @Override
        public void stop(IAM.Empty request, StreamObserver<IAM.Empty> responseObserver) {
            LOG.info("Servant::stop {}", agentAddress);
            responseObserver.onNext(IAM.Empty.newBuilder().build());
            responseObserver.onCompleted();
            UserEventLogger.log(new UserEvent(
                "Servant task exit",
                Map.of(
                    "task_id", taskId,
                    "address", agentAddress.toString(),
                    "exit_code", String.valueOf(0)
                ),
                UserEvent.UserEventType.TaskStop
            ));
            System.exit(0);
        }
    }
}
