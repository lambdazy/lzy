package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.*;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalState;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.LzyTerminal;

public class LzyTerminal extends LzyAgent implements Closeable {
    private static final Logger LOG = LogManager.getLogger(LzyTerminal.class);

    private final URI agentUri;
    private final Server agentServer;
    private final ManagedChannel channel;
    private final LzyKharonGrpc.LzyKharonStub kharon;
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private CommandHandler commandHandler;
    private LzyContext context;

    public LzyTerminal(LzyAgentConfig config) throws URISyntaxException, IOException {
        super(LzyAgentConfig.updateServantId(config, "term_" + UUID.randomUUID()));

        agentUri = new URI(LzyTerminal.scheme(), null, config.getAgentName(), config.getAgentPort(), null, null, null);

        agentServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(config.getAgentName(), config.getAgentPort()))
            .permitKeepAliveWithoutCalls(true)
            .permitKeepAliveTime(ChannelBuilder.KEEP_ALIVE_TIME_MINS_ALLOWED, TimeUnit.MINUTES)
            .addService(new Impl())
            .build();

        channel = ChannelBuilder
            .forAddress(config.getServerAddress().getHost(), config.getServerAddress().getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        kharon = LzyKharonGrpc.newStub(channel);
        server = LzyServerGrpc.newBlockingStub(channel);
    }

    @Override
    protected LzyContext context() {
        if (context == null) {
            throw new RuntimeException("Context is not yet defined (who knows what that mean!)");
        }
        return context;
    }

    @Override
    protected URI serverUri() {
        return agentUri;
    }

    @Override
    protected Server server() {
        return agentServer;
    }

    @Override
    protected void onStartUp() {
        commandHandler = new CommandHandler();
        status.set(AgentStatus.PREPARING_EXECUTION);

        context = new LzyContext(config.getServantId(), lzyFs.getSlotsManager(), lzyFs.getSlotConnectionManager(),
            lzyFs.getMountPoint().toString());
        status.set(AgentStatus.EXECUTING);

        Context.current().addListener(context -> {
            LOG.info("Terminal session terminated from server ");
            close();
        }, Runnable::run);

        context.onProgress(progress -> {
            LOG.info("LzyTerminal::progress {} {}", agentUri, JsonUtils.printRequest(progress));
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
                LOG.info("Skipping to send progress from terminal to server :" + JsonUtils
                    .printRequest(progress));
            }

            if (progress.hasExit()) {
                LOG.info("LzyTerminal::exit {}", agentUri);
                commandHandler.onCompleted();
            }
        });
    }

    @Override
    protected LzyServerGrpc.LzyServerBlockingStub serverApi() {
        return server;
    }

    @Override
    public void close() {
        context.slots().forEach(LzySlot::suspend);
        context.close();
        commandHandler.onCompleted();
        channel.shutdown();
        agentServer.shutdown();
        super.close();
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        channel.awaitTermination(10, TimeUnit.SECONDS);
        super.awaitTermination();
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
                        CommandHandler.this.onNext(TerminalState.newBuilder()
                            .setCommandId(commandId)
                            .setSlotStatus(Servant.SlotCommandStatus.newBuilder()
                                .setRc(Servant.SlotCommandStatus.RC.newBuilder()
                                    .setCodeValue(1)
                                    .setDescription("Invalid terminal command")
                                    .build())
                                .build())
                            .build());
                        return;
                    }

                    final Servant.SlotCommand slotCommand = terminalCommand.getSlotCommand();
                    try {
                        // TODO: find out if we need namespaces here
                        final LzySlot slot = context.slot(slotCommand.getTid(), slotCommand.getSlot());
                        if (slot == null) {
                            if (slotCommand.hasDestroy() || slotCommand.hasDisconnect()) {
                                CommandHandler.this.onNext(TerminalState.newBuilder()
                                    .setCommandId(commandId)
                                    .setSlotStatus(Servant.SlotCommandStatus.newBuilder()
                                        .setRc(Servant.SlotCommandStatus.RC.newBuilder()
                                            .setCodeValue(0)
                                            .build())
                                        .build())
                                    .build());
                                return;
                            } else {
                                CommandHandler.this.onNext(TerminalState.newBuilder()
                                    .setCommandId(commandId)
                                    .setSlotStatus(Servant.SlotCommandStatus.newBuilder()
                                        .setRc(Servant.SlotCommandStatus.RC.newBuilder()
                                            .setCodeValue(1)
                                            .setDescription("Slot " + slotCommand.getSlot()
                                                + " not found in ns: " + slotCommand.getTid())
                                            .build())
                                        .build())
                                    .build());
                                return;
                            }
                        }
                        if (slotCommand.hasConnect()) {
                            final URI slotUri = URI.create(slotCommand.getConnect().getSlotUri());
                            ForkJoinPool.commonPool().execute(() -> {
                                if (slot instanceof LzyOutputSlot) {
                                    slotSender.connect((LzyOutputSlot) slot, slotUri);
                                } else if (slot instanceof LzyInputSlot) {
                                    if (slotUri.getScheme().equals("s3") || slotUri.getScheme().equals("azure")) {
                                        ((LzyInputSlot) slot).connect(slotUri,
                                            lzyFs.getSlotConnectionManager().connectToS3(slotUri, 0));
                                    } else {
                                        ((LzyInputSlot) slot).connect(slotUri,
                                            lzyFs.getSlotConnectionManager().connectToSlot(slotUri, 0));
                                    }
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

                        final Servant.SlotCommandStatus slotCommandStatus = lzyFs.configureSlot(slotCommand);
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
                    close();
                }

                @Override
                public void onCompleted() {
                    LOG.warn("Terminal was detached from server");
                    close();
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
            try {
                responseObserver.onCompleted();
            } catch (IllegalStateException e) {
                LOG.warn("Terminal command handler already completed");
            }
        }
    }

    private class Impl extends LzyServantGrpc.LzyServantImplBase {

        @Override
        public void update(IAM.Auth request, StreamObserver<IAM.Empty> responseObserver) {
            LzyTerminal.this.update(request, responseObserver);
        }

        @Override
        public void status(IAM.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            LzyTerminal.this.status(request, responseObserver);
        }
    }
}
