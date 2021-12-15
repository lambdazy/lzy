package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.*;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.gaul.s3proxy.S3Proxy;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.S3SlotSnapshot;
import ru.yandex.cloud.ml.platform.lzy.servant.snapshot.Snapshotter;
import yandex.cloud.priv.datasphere.v2.lzy.*;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalState;

import java.io.Closeable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ForkJoinPool;

public class LzyTerminal extends LzyAgent implements Closeable {
    private static final Logger LOG = LogManager.getLogger(LzyTerminal.class);
    private final Server agentServer;
    private final ManagedChannel channel;
    private S3Proxy proxy = null;
    private final LzyKharonGrpc.LzyKharonStub kharon;
    private final LzyKharonGrpc.LzyKharonBlockingStub kharonBlockingStub;
    private CommandHandler commandHandler;
    private LzyExecution currentExecution;

    public LzyTerminal(LzyAgentConfig config) throws URISyntaxException {
        super(config);
        final LzyTerminal.Impl impl = new Impl();
        agentServer = ServerBuilder.forPort(config.getAgentPort()).addService(impl).build();
        channel = ManagedChannelBuilder
            .forAddress(serverAddress.getHost(), serverAddress.getPort())
            .usePlaintext()
            .build();
        kharon = LzyKharonGrpc.newStub(channel);
        kharonBlockingStub = LzyKharonGrpc.newBlockingStub(channel);
    }

    @Override
    protected Server server() {
        return agentServer;
    }

    private class CommandHandler {
        private final StreamObserver<TerminalState> responseObserver;
        private final TerminalSlotSender slotSender = new TerminalSlotSender(kharon);

        CommandHandler() {
            StreamObserver<TerminalCommand> supplier = new StreamObserver<>() {
                @Override
                public void onNext(TerminalCommand terminalCommand) {
                    LOG.info("TerminalCommand::onNext " + JsonUtils.printRequest(terminalCommand));

                    final String commandId = terminalCommand.getCommandId();
                    if (terminalCommand.getCommandCase() != TerminalCommand.CommandCase.SLOTCOMMAND) {
                        CommandHandler.this.onError(Status.INVALID_ARGUMENT.asException());
                    }

                    final Servant.SlotCommand slotCommand = terminalCommand.getSlotCommand();
                    try {
                        final LzySlot slot = currentExecution.slot(slotCommand.getSlot());
                        if (slotCommand.hasConnect()) {
                            final URI slotUri = URI.create(slotCommand.getConnect().getSlotUri());
                            ForkJoinPool.commonPool().execute(() -> {
                                if (slot instanceof LzyOutputSlot) {
                                    slotSender.connect((LzyOutputSlot) slot, slotUri);
                                } else if (slot instanceof LzyInputSlot) {
                                    ((LzyInputSlot) slot).connect(slotUri, kharonBlockingStub::openOutputSlot);
                                }
                            });

                            CommandHandler.this.onNext(TerminalState.newBuilder()
                                .setCommandId(commandId)
                                .setSlotStatus(Servant.SlotCommandStatus.newBuilder()
                                    .setRc(Servant.SlotCommandStatus.RC.newBuilder()
                                        .setCodeValue(0)
                                        .build())
                                    .build())
                                .build());
                            return;
                        }

                        final Servant.SlotCommandStatus slotCommandStatus = configureSlot(
                            currentExecution,
                            slotCommand
                        );
                        final TerminalState terminalState = TerminalState.newBuilder()
                            .setCommandId(commandId)
                            .setSlotStatus(slotCommandStatus)
                            .build();
                        LOG.info("CommandHandler::onNext " + JsonUtils.printRequest(terminalState));
                        CommandHandler.this.onNext(terminalState);
                    } catch (StatusException e) {
                        LOG.info("CommandHandler::onError " + e);
                        CommandHandler.this.onError(e);
                    }
                }

                @Override
                public void onError(Throwable throwable) {
                    LOG.error("Exception during terminal <-> server: " + throwable);
                    System.exit(-1);
                }

                @Override
                public void onCompleted() {
                    LOG.warn("Terminal was detached from server");
                    System.exit(0);
                }
            };

            responseObserver = kharon.attachTerminal(supplier);

            status.set(AgentStatus.REGISTERING);
            responseObserver.onNext(TerminalState.newBuilder()
                .setAttachTerminal(AttachTerminal.newBuilder()
                    .setAuth(auth.getUser())
                    .build())
                .build());
            status.set(AgentStatus.REGISTERED);
        }

        public synchronized void onNext(TerminalState terminalState) {
            responseObserver.onNext(terminalState);
        }

        public void onError(Throwable th) {
            responseObserver.onError(th);
        }

        public void onCompleted() {
            responseObserver.onCompleted();
        }
    }

    @Override
    protected void onStartUp() {
        commandHandler = new CommandHandler();
        status.set(AgentStatus.PREPARING_EXECUTION);
        currentExecution = new LzyExecution(null, null, agentInternalAddress, new Snapshotter.DevNullSnapshotter());
        status.set(AgentStatus.EXECUTING);

        Context.current().addListener(context -> {
            if (currentExecution != null) {
                LOG.info("Execution terminated from server ");
                System.exit(1);
            }
        }, Runnable::run);

        Lzy.GetS3CredentialsResponse response = kharonBlockingStub.getS3Credentials(Lzy.GetS3CredentialsRequest.newBuilder().setAuth(auth).build());
        if (response.getUseS3Proxy()){
            proxy = S3SlotSnapshot.createProxy(
                response.getS3ProxyProvider(),
                response.getS3ProxyIdentity(),
                response.getS3ProxyCredentials()
            );
        }

        currentExecution.onProgress(progress -> {
            LOG.info("LzyTerminal::progress {} {}", agentAddress, JsonUtils.printRequest(progress));
            if (progress.hasAttach()) {
                final TerminalState terminalState = TerminalState.newBuilder()
                    .setAttach(progress.getAttach())
                    .build();
                commandHandler.onNext(terminalState);
            } else if (progress.hasDetach()) {
                final TerminalState terminalState = TerminalState.newBuilder()
                    .setDetach(progress.getDetach())
                    .build();
                commandHandler.onNext(terminalState);
            } else {
                LOG.info("Skipping to send progress from terminal to server :" + JsonUtils.printRequest(progress));
            }

            if (progress.hasExit()) {
                LOG.info("LzyTerminal::exit {}", agentAddress);
                currentExecution = null;
                commandHandler.onCompleted();
            }
        });
    }

    @Override
    protected LzyServerApi lzyServerApi() {
        return kharonBlockingStub::zygotes;
    }

    @Override
    public void close() {
        super.close();
        commandHandler.onCompleted();
        channel.shutdown();
        if (proxy != null){
            try {
                proxy.stop();
            } catch (Exception e) {
                LOG.error(e);
            }
        }
    }

    private class Impl extends LzyServantGrpc.LzyServantImplBase {
        @Override
        public void configureSlot(
            Servant.SlotCommand request,
            StreamObserver<Servant.SlotCommandStatus> responseObserver
        ) {
            LOG.info("LzyTerminal configureSlot " + JsonUtils.printRequest(request));
            LzyTerminal.this.configureSlot(currentExecution, request, responseObserver);
        }

        @Override
        public void update(IAM.Auth request, StreamObserver<Servant.ExecutionStarted> responseObserver) {
            LzyTerminal.this.update(request, responseObserver);
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            LzyTerminal.this.status(currentExecution, request, responseObserver);
        }
    }
}
