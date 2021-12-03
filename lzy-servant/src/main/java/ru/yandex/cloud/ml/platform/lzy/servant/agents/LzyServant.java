package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.whiteboard.SnapshotMeta;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class LzyServant extends LzyAgent {
    private static final Logger LOG = LogManager.getLogger(LzyServant.class);
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final SnapshotApiGrpc.SnapshotApiBlockingStub snapshot;
    private final Server agentServer;
    private final String taskId;

    public LzyServant(LzyAgentConfig config) throws URISyntaxException {
        super(config);
        taskId = config.getTask();
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
    }

    @Override
    protected Server server() {
        return agentServer;
    }

    @Override
    protected void onStartUp() {
        status.set(AgentStatus.REGISTERING);
        final Lzy.AttachServant.Builder commandBuilder = Lzy.AttachServant.newBuilder();
        commandBuilder.setAuth(auth);
        commandBuilder.setServantURI(agentAddress.toString());
        commandBuilder.setSessionId(taskId);
        //noinspection ResultOfMethodCallIgnored
        server.registerServant(commandBuilder.build());
        status.set(AgentStatus.REGISTERED);
    }

    @Override
    protected LzyServerApi lzyServerApi() {
        return server::zygotes;
    }

    private class Impl extends LzyServantGrpc.LzyServantImplBase {
        private LzyExecution currentExecution;

        @Override
        public void execute(Tasks.TaskSpec request, StreamObserver<Servant.ExecutionProgress> responseObserver) {
            status.set(AgentStatus.PREPARING_EXECUTION);
            LOG.info("LzyServant::execute " + JsonUtils.printRequest(request));
            if (currentExecution != null) {
                responseObserver.onError(Status.RESOURCE_EXHAUSTED.asException());
                return;
            }
            final String tid = request.getAuth().getTask().getTaskId();
            final SnapshotMeta meta = request.hasSnapshotMeta() ? SnapshotMeta.from(request.getSnapshotMeta()) : null;
            currentExecution = new LzyExecution(
                tid,
                (AtomicZygote) gRPCConverter.from(request.getZygote()),
                agentInternalAddress,
                snapshot,
                meta
            );

            currentExecution.onProgress(progress -> {
                LOG.info("LzyServant::progress {} {}", agentAddress, JsonUtils.printRequest(progress));
                responseObserver.onNext(progress);
                if (progress.hasExit()) {
                    LOG.info("LzyServant::exit {}", agentAddress);
                    currentExecution = null;
                    responseObserver.onCompleted();
                }
            });
            Context.current().addListener(context -> {
                if (currentExecution != null) {
                    LOG.info("Execution terminated from server ");
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

            currentExecution.start();
            status.set(AgentStatus.EXECUTING);
        }

        @Override
        public void openOutputSlot(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
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
            System.exit(0);
        }
    }
}
