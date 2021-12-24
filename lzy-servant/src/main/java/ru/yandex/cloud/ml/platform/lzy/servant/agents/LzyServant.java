package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.MetricEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEvent;
import ru.yandex.cloud.ml.platform.lzy.model.logs.UserEventLogger;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.SnapshotterImpl;
import ru.yandex.cloud.ml.platform.lzy.model.snapshot.SnapshotMeta;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.storage.SnapshotStorage;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class LzyServant extends LzyAgent {
    private static final Logger LOG = LogManager.getLogger(LzyServant.class);
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot;
    private final Server agentServer;
    private final String taskId;
    private final String bucket;
    private final SnapshotStorage storage;

    public LzyServant(LzyAgentConfig config) throws URISyntaxException {
        super(config);
        final long start = System.currentTimeMillis();
        taskId = config.getTask();
        bucket = config.getBucket();
        URI whiteboardAddress = config.getWhiteboardAddress();
        final Impl impl = new Impl();
        final ManagedChannel channel = ManagedChannelBuilder
                .forAddress(serverAddress.getHost(), serverAddress.getPort())
                .usePlaintext()
                .build();
        server = LzyServerGrpc.newBlockingStub(channel);
        final ManagedChannel channelWb = ManagedChannelBuilder
                .forAddress(whiteboardAddress.getHost(), whiteboardAddress.getPort())
                .usePlaintext()
                .build();
        snapshot = SnapshotApiGrpc.newBlockingStub(channelWb);
        agentServer = ServerBuilder.forPort(config.getAgentPort()).addService(impl).build();
        storage = initStorage();
        final long finish = System.currentTimeMillis();
        MetricEventLogger.log(
            new MetricEvent(
                "time from agent construct finish to LzyServant construct finish",
                Map.of(),
                finish - start
            )
        );
       }

    private SnapshotStorage initStorage(){
        Lzy.GetS3CredentialsResponse resp = server.getS3Credentials(Lzy.GetS3CredentialsRequest.newBuilder().setAuth(auth).build());
        return SnapshotStorage.create(resp);
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
                Map.of(),
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

        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            final long executeMillis = System.currentTimeMillis();
            status.set(AgentStatus.PREPARING_EXECUTION);
            LOG.info("LzyServant::execute " + JsonUtils.printRequest(request));
            if (currentExecution != null) {
                responseObserver.onError(Status.RESOURCE_EXHAUSTED.asException());
                return;
            }
            final String tid = request.getAuth().getTask().getTaskId();
            final SnapshotMeta meta = request.hasSnapshotMeta() ? SnapshotMeta.from(request.getSnapshotMeta()) : SnapshotMeta.empty();
            final AtomicZygote zygote = (AtomicZygote) gRPCConverter.from(request.getZygote());
            final Snapshotter snapshotter = new SnapshotterImpl(auth.getTask(), bucket, zygote, snapshot, meta, storage);

            UserEventLogger.log(new UserEvent(
                "Servant execution preparing",
                Map.of(
                    "task_id", request.getAuth().getTask().getTaskId(),
                    "zygote_description", zygote.description()
                ),
                UserEvent.UserEventType.ExecutionPreparing
            ));

            currentExecution = new LzyExecution(tid, zygote, agentInternalAddress, snapshotter);
            currentExecution.onProgress(progress -> {
                LOG.info("LzyServant::progress {} {}", agentAddress, JsonUtils.printRequest(progress));
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
                    currentExecution = null;
                    responseObserver.onCompleted();
                }
            });
            Context.current().addListener(context -> {
                if (currentExecution != null) {
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

            for (Tasks.SlotAssignment spec : request.getAssignmentsList()) {
                final LzySlot lzySlot = currentExecution.configureSlot(
                        gRPCConverter.from(spec.getSlot()),
                        spec.getBinding()
                );
                if (lzySlot instanceof LzyFileSlot) {
                    LOG.info("lzyFS::addSlot " + lzySlot.name());
                    lzyFS.addSlot((LzyFileSlot) lzySlot);
                    LOG.info("lzyFS::slot added " + lzySlot.name());
                }
            }

            final long startExecutionMillis = System.currentTimeMillis();
            MetricEventLogger.log(
                new MetricEvent(
                    "time from task LzyServant::execution to LzyExecution::start",
                    Map.of(),
                    startExecutionMillis - executeMillis
                )
            );
            currentExecution.start();
            final long finishExecutionMillis = System.currentTimeMillis();
            MetricEventLogger.log(
                new MetricEvent(
                    "execution time",
                    Map.of(),
                    finishExecutionMillis - startExecutionMillis
                )
            );
            status.set(AgentStatus.EXECUTING);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
            final long start = System.currentTimeMillis();
            LOG.info("LzyServant::openOutputSlot " + JsonUtils.printRequest(request));
            if (currentExecution == null || currentExecution.slot(request.getSlot()) == null) {
                LOG.info("Not found slot: " + request.getSlot());
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            final LzyOutputSlot slot = (LzyOutputSlot) currentExecution.slot(request.getSlot());
            try {
                slot.readFromPosition(request.getOffset())
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
                    Map.of(),
                    finish - start
                )
            );
        }

        @Override
        public void configureSlot(
                Servant.SlotCommand request,
                StreamObserver<Servant.SlotCommandStatus> responseObserver
        ) {
            LzyServant.this.configureSlot(currentExecution, request, responseObserver);
        }

        @Override
        public void signal(Tasks.TaskSignal request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            if (currentExecution == null) {
                responseObserver.onError(Status.NOT_FOUND.asException());
                return;
            }
            currentExecution.signal(request.getSigValue());
            responseObserver.onNext(Servant.ExecutionStarted.newBuilder().build());
            responseObserver.onCompleted();
        }

        @Override
        public void update(IAM.Auth request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            LzyServant.this.update(request, responseObserver);
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            LzyServant.this.status(currentExecution, request, responseObserver);
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
                        "exit_code", String.valueOf(1)
                    ),
                    UserEvent.UserEventType.TaskStop
            ));
            System.exit(0);
        }
    }
}
