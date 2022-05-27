package ru.yandex.cloud.ml.platform.lzy.servant.agents;

import static ru.yandex.cloud.ml.platform.lzy.model.UriScheme.LzyTerminal;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.StatusException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.fs.LzyInputSlot;
import ru.yandex.cloud.ml.platform.lzy.fs.LzyOutputSlot;
import ru.yandex.cloud.ml.platform.lzy.fs.LzySlot;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import ru.yandex.cloud.ml.platform.lzy.model.UriScheme;
import ru.yandex.cloud.ml.platform.lzy.model.grpc.ChannelBuilder;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.ServerCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalCommand;
import yandex.cloud.priv.datasphere.v2.lzy.LzyFsApi;
import yandex.cloud.priv.datasphere.v2.lzy.LzyFsApi.SlotCommandStatus.RC;
import yandex.cloud.priv.datasphere.v2.lzy.LzyKharonGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServantGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

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

        agentUri = new URI(LzyTerminal.scheme(), null, config.getAgentHost(), config.getAgentPort(), null, null, null);

        agentServer = NettyServerBuilder
            .forAddress(new InetSocketAddress(config.getAgentHost(), config.getAgentPort()))
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

        context = new LzyContext(config.getServantId(), lzyFs.getSlotsManager(), lzyFs.getMountPoint().toString());
        status.set(AgentStatus.EXECUTING);

        Context.current().addListener(context -> {
            LOG.info("Terminal session terminated from server ");
            close();
        }, Runnable::run);

        context.onProgress(progress -> {
            LOG.info("LzyTerminal::progress {} {}", agentUri, JsonUtils.printRequest(progress));
            if (progress.hasAttach()) {
                final ServerCommand terminalState = ServerCommand.newBuilder()
                    .setAttach(progress.getAttach())
                    .build();
                commandHandler.onNext(terminalState);
            } else if (progress.hasDetach()) {
                final ServerCommand terminalState = ServerCommand.newBuilder()
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
        LOG.info("Close terminal...");
        context.slots().forEach(slot -> {
            LOG.info("  suspending slot {} ({})...", slot.name(), slot.status().getState());
            slot.suspend();
        });
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
        private final StreamObserver<ServerCommand> responseObserver;
        private final TerminalSlotSender slotSender = new TerminalSlotSender(kharon);

        CommandHandler() {
            StreamObserver<TerminalCommand> supplier = new StreamObserver<>() {
                @Override
                public void onNext(TerminalCommand terminalCommand) {
                    LOG.info("TerminalCommand::onNext " + JsonUtils.printRequest(terminalCommand));

                    final String commandId = terminalCommand.getCommandId();
                    if (terminalCommand.getCommandCase() != TerminalCommand.CommandCase.SLOTCOMMAND) {
                        CommandHandler.this.onNext(ServerCommand.newBuilder()
                            .setTerminalResponse(
                                Kharon.TerminalResponse.newBuilder()
                                .setCommandId(commandId)
                                .setSlotStatus(LzyFsApi.SlotCommandStatus.newBuilder()
                                    .setRc(RC.newBuilder()
                                        .setCodeValue(1)
                                        .setDescription("Invalid terminal command")
                                        .build())
                                    .build())
                                .build()
                            ).build());
                        return;
                    }

                    final LzyFsApi.SlotCommand slotCommand = terminalCommand.getSlotCommand();
                    try {
                        // TODO: find out if we need namespaces here
                        final LzySlot slot = context.slot(slotCommand.getTid(), slotCommand.getSlot());

                        if (slot == null) {
                            if (slotCommand.hasDestroy() || slotCommand.hasDisconnect()) {
                                CommandHandler.this.onNext(ServerCommand.newBuilder()
                                    .setTerminalResponse(
                                        Kharon.TerminalResponse.newBuilder()
                                        .setCommandId(commandId)
                                        .setSlotStatus(LzyFsApi.SlotCommandStatus.newBuilder()
                                            .setRc(RC.newBuilder()
                                                .setCode(RC.Code.SUCCESS)
                                                .build())
                                            .build())
                                        .build())
                                    .build());
                            } else {
                                CommandHandler.this.onNext(ServerCommand.newBuilder()
                                    .setTerminalResponse(
                                        Kharon.TerminalResponse.newBuilder()
                                        .setCommandId(commandId)
                                        .setSlotStatus(LzyFsApi.SlotCommandStatus.newBuilder()
                                            .setRc(RC.newBuilder()
                                                .setCode(RC.Code.ERROR)
                                                .setDescription("Slot " + slotCommand.getSlot()
                                                    + " not found in ns: " + slotCommand.getTid())
                                                .build())
                                            .build())
                                        .build())
                                    .build());
                            }
                            return;
                        }

                        if (slotCommand.hasConnect()) {
                            final URI slotUri = URI.create(slotCommand.getConnect().getSlotUri());

                            ForkJoinPool.commonPool().execute(() -> {
                                try {
                                    if (slot instanceof LzyOutputSlot) {
                                        slotSender.connect((LzyOutputSlot) slot, slotUri);
                                    } else if (slot instanceof LzyInputSlot) {
                                        final var inputSlot = (LzyInputSlot) slot;
                                        if (UriScheme.SlotS3.match(slotUri) || UriScheme.SlotAzure.match(slotUri)) {
                                            inputSlot.connect(slotUri,
                                                lzyFs.getSlotConnectionManager().connectToS3(slotUri, 0));
                                        } else {
                                            inputSlot.connect(slotUri,
                                                lzyFs.getSlotConnectionManager().connectToSlot(slotUri, 0));
                                        }
                                    }

                                    CommandHandler.this.onNext(ServerCommand.newBuilder()
                                        .setTerminalResponse(Kharon.TerminalResponse.newBuilder()
                                            .setCommandId(commandId)
                                            .setSlotStatus(LzyFsApi.SlotCommandStatus.newBuilder()
                                                .setRc(RC.newBuilder()
                                                    .setCode(RC.Code.SUCCESS)
                                                    .build())
                                                .build())
                                            .build())
                                        .build());
                                } catch (Exception e) {
                                    LOG.error("Can't connect to slot {}: {}", slotUri, e.getMessage(), e);
                                    CommandHandler.this.onError(e);
                                }
                            });

                            return;
                        }

                        final LzyFsApi.SlotCommandStatus slotCommandStatus = lzyFs.configureSlot(slotCommand);
                        final ServerCommand terminalState = ServerCommand.newBuilder()
                            .setTerminalResponse(Kharon.TerminalResponse.newBuilder()
                                .setCommandId(commandId)
                                .setSlotStatus(slotCommandStatus)
                                .build()
                            ).build();
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
            responseObserver.onNext(ServerCommand.newBuilder()
                .setAttachTerminal(AttachTerminal.newBuilder()
                    .setAuth(auth.getUser())
                    .build())
                .build());
            status.set(AgentStatus.REGISTERED);
        }

        public synchronized void onNext(ServerCommand serverCommand) {
            responseObserver.onNext(serverCommand);
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
