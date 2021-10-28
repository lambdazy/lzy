package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.*;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

public class TerminalSession {
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private StreamObserver<Servant.ExecutionProgress> executionProgress;
    private final StreamObserver<Kharon.TerminalState> terminalStateObserver;
    private final StreamObserver<Kharon.TerminalCommand> terminalController;

    private final DataCarrier dataCarrier = new DataCarrier();

    private String user;
    private final UUID sessionId = UUID.randomUUID();
    private final URI kharonAddress;
    private final URI kharonServantAddress;

    private final Map<UUID, CompletableFuture<Kharon.TerminalState>> tasks = new ConcurrentHashMap<>();

    public TerminalSession(
        LzyServerGrpc.LzyServerBlockingStub lzyServer,
        StreamObserver<Kharon.TerminalCommand> terminalCommandObserver,
        URI kharonAddress,
        URI kharonServantAddress
    ) {
        this.kharonAddress = kharonAddress;
        this.kharonServantAddress = kharonServantAddress;
        terminalController = terminalCommandObserver;
        terminalStateObserver = new StreamObserver<>() {
            @Override
            public void onNext(Kharon.TerminalState terminalState) {
                LOG.info("Kharon::TerminalSession session_id:" + sessionId + " request:" + JsonUtils.printRequest(terminalState));
                switch (terminalState.getProgressCase()) {
                    case ATTACHTERMINAL: {
                        final Kharon.AttachTerminal attachTerminal = terminalState.getAttachTerminal();
                        if (user != null) {
                            throw new IllegalStateException("Double attach to terminal from user " + user);
                        }
                        final IAM.UserCredentials auth = attachTerminal.getAuth();
                        user = auth.getUserId();

                        final IAM.UserCredentials userCredentials = IAM.UserCredentials.newBuilder()
                            .setUserId(user)
                            .setToken(auth.getToken())
                            .setTokenSign(auth.getTokenSign())
                            .setSessionId(sessionId.toString())
                            .build();

                        //noinspection ResultOfMethodCallIgnored
                        lzyServer.registerServant(Lzy.AttachServant.newBuilder()
                            .setAuth(IAM.Auth.newBuilder()
                                .setUser(userCredentials)
                                .build())
                            .setServantURI(kharonServantAddress.toString())
                            .build());
                        break;
                    }
                    case ATTACH: {
                        checkConsistency();
                        final Servant.SlotAttach attach = terminalState.getAttach();
                        executionProgress.onNext(Servant.ExecutionProgress.newBuilder()
                            .setAttach(Servant.SlotAttach.newBuilder()
                                .setSlot(attach.getSlot())
                                .setUri(convertToKharonUri(attach.getUri()))
                                .setChannel(attach.getChannel())
                                .build())
                            .build());
                        break;
                    }
                    case DETACH: {
                        checkConsistency();
                        final Servant.SlotDetach detach = terminalState.getDetach();
                        executionProgress.onNext(Servant.ExecutionProgress.newBuilder()
                            .setDetach(Servant.SlotDetach.newBuilder()
                                .setSlot(detach.getSlot())
                                .setUri(convertToKharonUri(detach.getUri()))
                                .build())
                            .build());
                        break;
                    }
                    case SLOTSTATUS: {
                        checkConsistency();
                        final CompletableFuture<Kharon.TerminalState> future = tasks.get(UUID.fromString(terminalState.getCommandId()));
                        if (future == null) {
                            throw new IllegalStateException("No such task " + terminalState.getCommandId());
                        }
                        future.complete(terminalState);
                        break;
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Terminal execution terminated ", throwable);
            }

            @Override
            public void onCompleted() {
                LOG.info("Terminal for " + user + " disconnected; sessionId = " + sessionId);
            }
        };
    }

    public StreamObserver<Kharon.TerminalState> getTerminalStateObserver() {
        return terminalStateObserver;
    }

    public UUID getSessionId() {
        return sessionId;
    }

    public void setExecutionProgress(StreamObserver<Servant.ExecutionProgress> executionProgress) {
        this.executionProgress = executionProgress;
    }

    @Nullable
    public Servant.SlotCommandStatus configureSlot(Servant.SlotCommand request) {
        final CompletableFuture<Kharon.TerminalState> future = new CompletableFuture<>();
        final String commandId = generateCommandId(future);
        terminalController.onNext(Kharon.TerminalCommand.newBuilder()
            .setCommandId(commandId)
            .setSlotCommand(request)
            .build());
        try {
            return future.get().getSlotStatus();
        } catch (InterruptedException | ExecutionException e) {
            LOG.error("Failed while configure slot in bidirectional stream");
        }
        return Servant.SlotCommandStatus.newBuilder().build();
    }

    private void checkConsistency() {
        if (user == null) {
            throw new IllegalStateException("Got terminal state before attachTerminal");
        }
        if (executionProgress == null) {
            throw new IllegalStateException("Got terminal state before execute from server");
        }
    }

    private String generateCommandId(CompletableFuture<Kharon.TerminalState> future) {
        final UUID taskId = UUID.randomUUID();
        tasks.put(taskId, future);
        return taskId.toString();
    }

    private String convertToKharonUri(String slotStrUri) {
        try {
            final URI slotUri = URI.create(slotStrUri);
            return new URI(
                slotUri.getScheme(),
                kharonAddress.getHost(),
                slotUri.getPath(),
                sessionId.toString()
            ).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void carryTerminalSlotContent(Servant.SlotRequest request, StreamObserver<Servant.Message> responseObserver) {
        final String slot = request.getSlot();
        configureSlot(Servant.SlotCommand.newBuilder()
            .setSlot(slot)
            .setConnect(Servant.ConnectSlotCommand.newBuilder().build())
            .build());
        dataCarrier.openServantConnection(slot, responseObserver);
    }

    public StreamObserver<Kharon.SendSlotDataMessage> initDataTransfer(StreamObserver<Kharon.ReceivedDataStatus> responseObserver) {
        return dataCarrier.connectTerminalConnection(responseObserver);
    }
}
