package ai.lzy.servant.agents;

import ai.lzy.fs.LzyFsServerLegacy;
import ai.lzy.fs.SlotConnectionManager;
import ai.lzy.fs.fs.LzyInputSlot;
import ai.lzy.fs.fs.LzyOutputSlot;
import ai.lzy.fs.fs.LzySlot;
import ai.lzy.model.UriScheme;
import ai.lzy.model.grpc.ProtoConverter;
import ai.lzy.model.slot.SlotInstance;
import ai.lzy.util.grpc.ChannelBuilder;
import ai.lzy.util.grpc.JsonUtils;
import ai.lzy.v1.deprecated.*;
import ai.lzy.v1.deprecated.Kharon.Attach;
import ai.lzy.v1.deprecated.Kharon.TerminalCommand;
import ai.lzy.v1.deprecated.Kharon.TerminalProgress;
import ai.lzy.v1.deprecated.Kharon.TerminalResponse;
import ai.lzy.v1.fs.LzyFsApi;
import ai.lzy.v1.fs.LzyFsApi.SlotCommandStatus.RC;
import io.grpc.Context;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

public class LzyTerminal implements Closeable {

    private static final Logger LOG = LogManager.getLogger(LzyTerminal.class);

    private final LzyAgent agent;
    private final LzyFsServerLegacy lzyFs;
    private final LzyContext context;
    private final ManagedChannel channel;
    private final LzyKharonGrpc.LzyKharonStub kharon;
    private final LzyServerGrpc.LzyServerBlockingStub server;
    private final CommandHandler commandHandler;

    public LzyTerminal(LzyAgentConfig config)
        throws URISyntaxException, IOException, InvocationTargetException, NoSuchMethodException,
        InstantiationException, IllegalAccessException
    {
        channel = ChannelBuilder
            .forAddress(config.getServerAddress().getHost(), config.getServerAddress().getPort())
            .usePlaintext()
            .enableRetry(LzyKharonGrpc.SERVICE_NAME)
            .build();
        kharon = LzyKharonGrpc.newStub(channel);
        server = LzyServerGrpc.newBlockingStub(channel);

        commandHandler = new CommandHandler();
        config = LzyAgentConfig.updateAgentId(config, commandHandler.workflowId());
        agent = new LzyAgent(config, "LzyTerminal", new Impl());
        agent.updateStatus(AgentStatus.REGISTERED);
        agent.publishTools(server.zygotes(agent.auth()));
        lzyFs = agent.fs();
        context = agent.context();
        agent.updateStatus(AgentStatus.EXECUTING);
        commandHandler.start();

        Context.current().addListener(context -> {
            LOG.info("Terminal session terminated from server ");
            close();
        }, Runnable::run);

        context.onProgress(progress -> {
            LOG.info("LzyTerminal::progress {} {}", agent.uri(), JsonUtils.printRequest(progress));
            if (progress.hasConcluded()) {
                LOG.info("LzyTerminal::exit {}", agent.uri());
                commandHandler.onCompleted();
            }
        });
    }

    @Override
    public void close() {
        LOG.info("Close terminal...");
        try {
            commandHandler.onCompleted();
            channel.shutdown();
            agent.shutdown();
        } finally {
            agent.close();
        }
    }

    public void awaitTermination() throws InterruptedException, IOException {
        agent.awaitTermination();
    }

    private class CommandHandler {

        private final StreamObserver<TerminalProgress> responseObserver;
        private final TerminalSlotSender slotSender = new TerminalSlotSender(kharon);
        private boolean invalidated = false;
        private final CompletableFuture<String> workflowId = new CompletableFuture<>();

        CommandHandler() {
            StreamObserver<TerminalCommand> supplier = new StreamObserver<>() {
                @Override
                public void onNext(TerminalCommand terminalCommand) {
                    LOG.info("TerminalCommand::onNext " + JsonUtils.printRequest(terminalCommand));

                    final String commandId = terminalCommand.getCommandId();

                    switch (terminalCommand.getCommandCase()) {
                        case TERMINALATTACHRESPONSE -> {
                            var terminalAttachResponse = terminalCommand.getTerminalAttachResponse();
                            LOG.info("TerminalAttachResponse received: {}", terminalAttachResponse);
                            workflowId.complete(terminalAttachResponse.getWorkflowId());
                        }

                        case CONNECTSLOT -> {
                            var connectSlotRequest = terminalCommand.getConnectSlot();
                            LOG.info("ConnectSlot command received: {}", connectSlotRequest);

                            final SlotInstance fromSlot = ProtoConverter.fromProto(connectSlotRequest.getFrom());
                            final LzySlot slot = context.slot(fromSlot.taskId(), fromSlot.name());
                            if (slot == null) {
                                reply(commandId, RC.Code.ERROR,
                                    "Slot " + fromSlot.name() + " not found in ns: " + fromSlot.taskId());
                                return;
                            }

                            final SlotInstance toSlot = ProtoConverter.fromProto(connectSlotRequest.getTo());
                            final URI slotUri = toSlot.uri();

                            ForkJoinPool.commonPool().execute(() -> {
                                try {
                                    if (slot instanceof LzyOutputSlot) {
                                        slotSender.connect((LzyOutputSlot) slot, toSlot);
                                    } else if (slot instanceof LzyInputSlot inputSlot) {
                                        if (UriScheme.SlotS3.match(slotUri) || UriScheme.SlotAzure.match(slotUri)) {
                                            inputSlot.connect(slotUri,
                                                lzyFs.getSlotConnectionManager().connectToS3(slotUri, 0));
                                        } else {
                                            inputSlot.connect(slotUri,
                                                SlotConnectionManager.connectToSlot(toSlot, 0));
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

                            final SlotInstance slotInstance = ProtoConverter.fromProto(cmd.getSlotInstance());

                            final LzySlot slot = context.slot(slotInstance.taskId(), slotInstance.name());
                            if (slot == null) {
                                reply(commandId, RC.Code.SUCCESS, "");
                                return;
                            }

                            reply(commandId, lzyFs.disconnectSlot(cmd));
                        }

                        case STATUSSLOT -> {
                            var cmd = terminalCommand.getStatusSlot();
                            LOG.info("StatusSlot command received: {}", cmd);
                            final SlotInstance slotInstance = ProtoConverter.fromProto(cmd.getSlotInstance());

                            final LzySlot slot = context.slot(slotInstance.taskId(), slotInstance.name());
                            if (slot == null) {
                                reply(commandId, RC.Code.ERROR,
                                    "Slot " + slotInstance.name() + " not found in ns: " + slotInstance.taskId());
                                return;
                            }

                            reply(commandId, lzyFs.statusSlot(cmd));
                        }

                        case DESTROYSLOT -> {
                            var cmd = terminalCommand.getDestroySlot();
                            LOG.info("DestroySlot command received: {}", cmd);
                            final SlotInstance slotInstance = ProtoConverter.fromProto(cmd.getSlotInstance());

                            final LzySlot slot = context.slot(slotInstance.taskId(), slotInstance.name());
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
                    var terminalState = TerminalProgress.newBuilder()
                        .setAuth(agent.auth().getUser())
                        .setTerminalResponse(TerminalResponse.newBuilder()
                            .setCommandId(commandId)
                            .setSlotStatus(status)
                            .build())
                        .build();
                    LOG.info("CommandHandler::onNext " + JsonUtils.printRequest(terminalState));
                    CommandHandler.this.onNext(terminalState);
                }
            };

            responseObserver = kharon.attachTerminal(supplier);
        }

        void start() {
            responseObserver.onNext(
                TerminalProgress.newBuilder().setAuth(agent.auth().getUser()).setAttach(Attach.newBuilder().build())
                    .build());
        }

        public String workflowId() {
            try {
                return workflowId.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IllegalStateException("Unable to get workflowId from kharon", e);
            }
        }

        public synchronized void onNext(Kharon.TerminalProgress serverCommand) {
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
        public void update(LzyAuth.Auth request, StreamObserver<LzyAuth.Empty> responseObserver) {
            agent.update(server.zygotes(request), responseObserver);
        }

        @Override
        public void status(LzyAuth.Empty request, StreamObserver<Servant.ServantStatus> responseObserver) {
            agent.status(request, responseObserver);
        }
    }
}
