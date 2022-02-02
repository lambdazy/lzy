package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.StatusRuntimeException;
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
import java.util.concurrent.*;

public class TerminalSession {
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);
    public static final String SESSION_ID_KEY = "kharon_session_id";

    private final CompletableFuture<StreamObserver<Servant.ContextProgress>> executeFromServerFuture = new CompletableFuture<>();
    private StreamObserver<Servant.ContextProgress> executionProgress;
    private final StreamObserver<Kharon.TerminalState> terminalStateObserver;
    private final StreamObserver<Kharon.TerminalCommand> terminalController;

    private final DataCarrier dataCarrier = new DataCarrier();

    private String user;
    private final UUID sessionId = UUID.randomUUID();
    private final URI kharonServantProxyAddress;

    private final Map<UUID, CompletableFuture<Kharon.TerminalState>> tasks = new ConcurrentHashMap<>();

    public TerminalSession(
        LzyServerGrpc.LzyServerBlockingStub lzyServer,
        StreamObserver<Kharon.TerminalCommand> terminalCommandObserver,
        URI kharonServantAddress
    ) {
        this.kharonServantProxyAddress = kharonServantAddress;
        terminalController = terminalCommandObserver;
        terminalStateObserver = new StreamObserver<>() {
            @Override
            public void onNext(Kharon.TerminalState terminalState) {
                if (!terminalState.hasSlotStatus()) {
                    LOG.info("Kharon::TerminalSession session_id:" + sessionId + " request:" + JsonUtils.printRequest(terminalState));
                }
                if (terminalState.getProgressCase() != Kharon.TerminalState.ProgressCase.ATTACHTERMINAL) {
                    checkConsistency();
                }
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
                            .build();

                        String servantAddr = kharonServantAddress.toString();
                        if (servantAddr.contains("host.docker.internal")) {
                            servantAddr = servantAddr.replace("host.docker.internal", "localhost");
                        }
                        try {
                            //noinspection ResultOfMethodCallIgnored
                            lzyServer.registerServant(Lzy.AttachServant.newBuilder()
                                    .setAuth(IAM.Auth.newBuilder()
                                            .setUser(userCredentials)
                                            .build())
                                    .setServantURI(servantAddr)
                                    .setSessionId(sessionId.toString())
                                    .build());
                        } catch (StatusRuntimeException e) {
                            LOG.error("registerServant failed. " + e);
                            terminalController.onError(e);
                        }
                        break;
                    }
                    case ATTACH: {
                        final Servant.SlotAttach attach = terminalState.getAttach();
                        executionProgress.onNext(Servant.ContextProgress.newBuilder()
                            .setAttach(Servant.SlotAttach.newBuilder()
                                .setSlot(attach.getSlot())
                                .setUri(convertToKharonServantUri(attach.getUri()))
                                .setChannel(attach.getChannel())
                                .build())
                            .build());
                        break;
                    }
                    case DETACH: {
                        final Servant.SlotDetach detach = terminalState.getDetach();
                        executionProgress.onNext(Servant.ContextProgress.newBuilder()
                            .setDetach(Servant.SlotDetach.newBuilder()
                                .setSlot(detach.getSlot())
                                .setUri(convertToKharonServantUri(detach.getUri()))
                                .build())
                            .build());
                        break;
                    }
                    case SLOTSTATUS: {
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

    public void setExecutionProgress(StreamObserver<Servant.ContextProgress> executionProgress) {
        executeFromServerFuture.complete(executionProgress);
    }

    @Nullable
    public Servant.SlotCommandStatus configureSlot(Servant.SlotCommand request) {
        LOG.info("Kharon sessionId " + sessionId + " ::configureSlot " + JsonUtils.printRequest(request));
        final CompletableFuture<Kharon.TerminalState> future = new CompletableFuture<>();
        final String commandId = generateCommandId(future);
        final Kharon.TerminalCommand sendingRequest = Kharon.TerminalCommand.newBuilder()
            .setCommandId(commandId)
            .setSlotCommand(request)
            .build();
        LOG.info("terminalController send request " + JsonUtils.printRequest(sendingRequest));
        synchronized (terminalController) {
            terminalController.onNext(sendingRequest);
        }
        try {
            return future.get(10, TimeUnit.SECONDS).getSlotStatus();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed while configure slot in bidirectional stream " + e);
        }
        return Servant.SlotCommandStatus.newBuilder().build();
    }

    private void checkConsistency() {
        if (user == null) {
            throw new IllegalStateException("Got terminal state before attachTerminal");
        }
        if (executionProgress == null) {
            try {
                executionProgress = executeFromServerFuture.get(10, TimeUnit.SECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                throw new IllegalStateException("Got terminal state before execute from server. " + e);
            }
        }
    }

    private String generateCommandId(CompletableFuture<Kharon.TerminalState> future) {
        final UUID taskId = UUID.randomUUID();
        tasks.put(taskId, future);
        return taskId.toString();
    }

    private String convertToKharonServantUri(String slotStrUri) {
        try {
            final URI slotUri = URI.create(slotStrUri);
            return new URI(
                slotUri.getScheme(),
                null,
                kharonServantProxyAddress.getHost(),
                kharonServantProxyAddress.getPort(),
                slotUri.getPath(),
                SESSION_ID_KEY + "=" + sessionId,
                null
            ).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        terminalController.onCompleted();
    }
}
