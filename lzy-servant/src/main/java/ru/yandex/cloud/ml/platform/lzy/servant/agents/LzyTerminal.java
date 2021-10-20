package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.gRPCConverter;
import ru.yandex.cloud.ml.platform.lzy.model.graph.AtomicZygote;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzyFileSlot;
import ru.yandex.cloud.ml.platform.lzy.servant.fs.LzySlot;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyTerminalGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.io.Closeable;
import java.net.URISyntaxException;

public class LzyTerminal extends LzyAgent implements Closeable {
    private static final Logger LOG = LogManager.getLogger(LzyTerminal.class);
    private final Server agentServer;
    private final ManagedChannel channel;
    private final LzyServerGrpc.LzyServerStub asyncServer;
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
        asyncServer = LzyServerGrpc.newStub(channel);
    }

    @Override
    protected Server server() {
        return agentServer;
    }

    private class CommandHandler {
        private final StreamObserver<Lzy.TerminalCommand> supplier;
        private StreamObserver<Lzy.TerminalState> responseObserver;

        CommandHandler() {
            supplier = new StreamObserver<>() {
                @Override
                public void onNext(Lzy.TerminalCommand terminalCommand) {
                    LOG.info("TerminalCommand::onNext " + JsonUtils.printRequest(terminalCommand));

                    final String commandId = terminalCommand.getCommandId();
                    if (terminalCommand.getCommandCase() != Lzy.TerminalCommand.CommandCase.SLOTCOMMAND) {
                        CommandHandler.this.onError(Status.INVALID_ARGUMENT.asException());
                    }

                    final Servant.SlotCommand slotCommand = terminalCommand.getSlotCommand();
                    try {
                        final Servant.SlotCommandStatus slotCommandStatus = configureSlot(
                            currentExecution,
                            slotCommand
                        );
                        final Lzy.TerminalState terminalState = Lzy.TerminalState.newBuilder()
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

            responseObserver = asyncServer.attachTerminal(supplier);
            responseObserver.onNext(Lzy.TerminalState.newBuilder()
                .setAttachTerminal(Lzy.AttachTerminal.newBuilder()
                    .setAuth(auth.getUser())
                    .build())
                .build());
        }

        public synchronized void onNext(Lzy.TerminalState terminalState) {
            responseObserver.onNext(terminalState);
        }

        public synchronized void onError(Throwable th) {
            responseObserver.onError(th);
        }

        public synchronized void onCompleted() {
            responseObserver.onCompleted();
        }
    }

    @Override
    protected void onStartUp() {
        status.set(AgentStatus.REGISTERING);
        commandHandler = new CommandHandler();
        status.set(AgentStatus.REGISTERED);

        status.set(AgentStatus.PREPARING_EXECUTION);
        currentExecution = new LzyExecution(null, null, agentInternalAddress);
        status.set(AgentStatus.EXECUTING);

        Context.current().addListener(context -> {
            if (currentExecution != null) {
                LOG.info("Execution terminated from server ");
                System.exit(1);
            }
        }, Runnable::run);

        currentExecution.onProgress(progress -> {
            LOG.info("LzyTerminal::progress {} {}", agentAddress, JsonUtils.printRequest(progress));
            if (progress.hasAttach()) {
                final Lzy.TerminalState terminalState = Lzy.TerminalState.newBuilder()
                    .setAttach(progress.getAttach())
                    .build();
                commandHandler.onNext(terminalState);
            } else if (progress.hasDetach()) {
                final Lzy.TerminalState terminalState = Lzy.TerminalState.newBuilder()
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
    public void close() {
        super.close();
        commandHandler.onCompleted();
        channel.shutdown();
    }

    private class Impl extends LzyTerminalGrpc.LzyTerminalImplBase {
        @Override
        public void configureSlot(
            Servant.SlotCommand request,
            StreamObserver<Servant.SlotCommandStatus> responseObserver
        ) {
            LOG.info("LzyTerminal configureSlot " + JsonUtils.printRequest(request));
            LzyTerminal.this.configureSlot(currentExecution, request, responseObserver);
        }

        @Override
        public void update(
            IAM.Auth request, StreamObserver<Servant.ExecutionStarted> responseObserver
        ) {
            LzyTerminal.this.update(request, responseObserver);
        }

        @Override
        public void status(
            IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver
        ) {
            LzyTerminal.this.status(currentExecution, request, responseObserver);
        }
    }
}
