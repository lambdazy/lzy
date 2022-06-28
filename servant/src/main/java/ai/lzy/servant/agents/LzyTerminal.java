package ai.lzy.servant.agents;

import static ai.lzy.model.UriScheme.LzyTerminal;

import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.Server;
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
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.JsonUtils;
import ai.lzy.model.UriScheme;
import ai.lzy.model.grpc.ChannelBuilder;
import ai.lzy.priv.v2.IAM;
import ai.lzy.priv.v2.Kharon;
import ai.lzy.priv.v2.Kharon.AttachTerminal;
import ai.lzy.priv.v2.Kharon.ServerCommand;
import ai.lzy.priv.v2.Kharon.TerminalCommand;
import ai.lzy.priv.v2.LzyFsApi;
import ai.lzy.priv.v2.LzyFsApi.SlotCommandStatus.RC;
import ai.lzy.priv.v2.LzyKharonGrpc;
import ai.lzy.priv.v2.LzyServantGrpc;
import ai.lzy.priv.v2.LzyServerGrpc;
import ai.lzy.priv.v2.Servant;

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

            if (progress.hasConcluded()) {
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
        private boolean invalidated = false;

        CommandHandler() {
            StreamObserver<TerminalCommand> supplier = new StreamObserver<>() {
                @Override
                public void onNext(TerminalCommand terminalCommand) {
                    LOG.info("TerminalCommand::onNext " + JsonUtils.printRequest(terminalCommand));

                    final String commandId = terminalCommand.getCommandId();

                    switch (terminalCommand.getCommandCase()) {
                        case CONNECTSLOT -> {
                            var cmd = terminalCommand.getConnectSlot();
                            LOG.info("ConnectSlot command received: {}", cmd);

                            final LzySlot slot = context.slot(cmd.getTaskId(), cmd.getSlotName());
                            if (slot == null) {
                                reply(commandId, RC.Code.ERROR,
                                        "Slot " + cmd.getSlotName() + " not found in ns: " + cmd.getTaskId());
                                return;
                            }

                            final URI slotUri = URI.create(cmd.getSlotUri());

                            ForkJoinPool.commonPool().execute(() -> {
                                try {
                                    if (slot instanceof LzyOutputSlot) {
                                        slotSender.connect((LzyOutputSlot) slot, slotUri);
                                    } else if (slot instanceof LzyInputSlot inputSlot) {
                                        if (UriScheme.SlotS3.match(slotUri) || UriScheme.SlotAzure.match(slotUri)) {
                                            inputSlot.connect(slotUri,
                                                lzyFs.getSlotConnectionManager().connectToS3(slotUri, 0));
                                        } else {
                                            inputSlot.connect(slotUri,
                                                lzyFs.getSlotConnectionManager().connectToSlot(slotUri, 0));
                                        }
                                    }

                                    reply(commandId, RC.Code.SUCCESS, "");
                                } catch (Exception e) {
                                    LOG.error("Can't connect to slot {}: {}", slotUri, e.getMessage(), e);
                                    CommandHandler.this.onError(e);
                                }
                            });
                        }

                        case DISCONNECTSLOT -> {
                            var cmd = terminalCommand.getDisconnectSlot();
                            LOG.info("DisconnectSlot command received: {}", cmd);

                            final LzySlot slot = context.slot(cmd.getTaskId(), cmd.getSlotName());
                            if (slot == null) {
                                reply(commandId, RC.Code.SUCCESS, "");
                                return;
                            }

                            reply(commandId, lzyFs.disconnectSlot(cmd));
                        }

                        case STATUSSLOT -> {
                            var cmd = terminalCommand.getStatusSlot();
                            LOG.info("StatusSlot command received: {}", cmd);

                            final LzySlot slot = context.slot(cmd.getTaskId(), cmd.getSlotName());
                            if (slot == null) {
                                reply(commandId, RC.Code.ERROR,
                                        "Slot " + cmd.getSlotName() + " not found in ns: " + cmd.getTaskId());
                                return;
                            }

                            reply(commandId, lzyFs.statusSlot(cmd));
                        }

                        case DESTROYSLOT -> {
                            var cmd = terminalCommand.getDestroySlot();
                            LOG.info("DestroySlot command received: {}", cmd);

                            final LzySlot slot = context.slot(cmd.getTaskId(), cmd.getSlotName());
                            if (slot == null) {
                                reply(commandId, RC.Code.SUCCESS, "");
                                return;
                            }

                            reply(commandId, lzyFs.destroySlot(cmd));
                        }

                        default -> reply(commandId, RC.Code.ERROR,
                                         "Invalid terminal command: " + terminalCommand.getCommandCase());
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

                private void reply(String commandId, RC.Code rc, String message) {
                    reply(commandId, LzyFsApi.SlotCommandStatus.newBuilder()
                        .setRc(RC.newBuilder()
                            .setCode(rc)
                            .setDescription(message)
                            .build())
                        .build());
                }

                private void reply(String commandId, LzyFsApi.SlotCommandStatus status) {
                    var terminalState = ServerCommand.newBuilder()
                        .setTerminalResponse(Kharon.TerminalResponse.newBuilder()
                            .setCommandId(commandId)
                            .setSlotStatus(status)
                            .build())
                        .build();
                    LOG.info("CommandHandler::onNext " + JsonUtils.printRequest(terminalState));
                    CommandHandler.this.onNext(terminalState);
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
            if (invalidated) {
                throw new IllegalStateException("Command handler was invalidated, but got onNext command");
            }
            responseObserver.onNext(serverCommand);
        }

        public synchronized void onError(Throwable th) {
            if (!invalidated) {
                responseObserver.onError(th);
                invalidated = true;
            }
        }

        public synchronized void onCompleted() {
            if (!invalidated) {
                responseObserver.onCompleted();
                invalidated = true;
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
