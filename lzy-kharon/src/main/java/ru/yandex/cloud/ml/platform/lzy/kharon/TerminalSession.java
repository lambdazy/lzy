package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.IAM.Auth;
import yandex.cloud.priv.datasphere.v2.lzy.IAM.UserCredentials;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalState;
import yandex.cloud.priv.datasphere.v2.lzy.Lzy.AttachServant;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.SlotCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Servant.SlotCommandStatus.RC;

import javax.annotation.Nullable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class TerminalSession {
    public static final String SESSION_ID_KEY = "kharon_session_id";
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private final CompletableFuture<StreamObserver<Servant.ServantProgress>> executeFromServerFuture =
        new CompletableFuture<>();
    private final StreamObserver<Kharon.TerminalState> terminalStateObserver;
    private final StreamObserver<Kharon.TerminalCommand> terminalController;
    private final AtomicBoolean invalid = new AtomicBoolean(false);
    private final UUID servantId = UUID.randomUUID();
    private final URI kharonFsProxyAddress;
    private final Map<UUID, CompletableFuture<Kharon.TerminalState>> tasks = new ConcurrentHashMap<>();
    private StreamObserver<Servant.ServantProgress> executionProgress;
    private String user;

    public TerminalSession(
        LzyServerGrpc.LzyServerBlockingStub lzyServer,
        StreamObserver<Kharon.TerminalCommand> terminalCommandObserver,
        URI kharonServantAddress, URI kharonFsAddress
    ) {
        this.kharonFsProxyAddress = kharonFsAddress;
        terminalController = terminalCommandObserver;
        terminalStateObserver = new StreamObserver<>() {
            @Override
            public void onNext(Kharon.TerminalState terminalState) {
                if (!terminalState.hasSlotStatus()) {
                    LOG.info("Kharon::TerminalSession session_id:" + servantId + " request:"
                        + JsonUtils.printRequest(terminalState));
                }
                if (terminalState.getProgressCase() != Kharon.TerminalState.ProgressCase.ATTACHTERMINAL) {
                    checkTerminalAndServantState();
                }
                switch (terminalState.getProgressCase()) {
                    case ATTACHTERMINAL: {
                        final AttachTerminal attachTerminal = terminalState.getAttachTerminal();
                        if (user != null) {
                            throw new IllegalStateException(
                                "Double attach to terminal from user " + user);
                        }
                        final UserCredentials auth = attachTerminal.getAuth();
                        user = auth.getUserId();

                        final UserCredentials userCredentials = UserCredentials.newBuilder()
                            .setUserId(user)
                            .setToken(auth.getToken())
                            .build();

                        String servantAddr = kharonServantAddress.toString();
                        if (servantAddr.contains("host.docker.internal")) {
                            servantAddr = servantAddr.replace("host.docker.internal", "localhost");
                        }
                        String fsAddr = kharonFsAddress.toString();
                        if (fsAddr.contains("host.docker.internal")) {
                            fsAddr = fsAddr.replace("host.docker.internal", "localhost");
                        }

                        try {
                            //noinspection ResultOfMethodCallIgnored
                            lzyServer.registerServant(AttachServant.newBuilder()
                                .setAuth(Auth.newBuilder()
                                    .setUser(userCredentials)
                                    .build())
                                .setServantURI(servantAddr)
                                .setFsURI(fsAddr)
                                .setServantId(servantId.toString())
                                .build());
                        } catch (StatusRuntimeException e) {
                            LOG.error("registerServant failed. " + e);
                            terminalController.onError(e);
                        }
                        break;
                    }
                    case ATTACH: {
                        final Servant.SlotAttach attach = terminalState.getAttach();
                        executionProgress.onNext(Servant.ServantProgress.newBuilder()
                            .setAttach(Servant.SlotAttach.newBuilder()
                                .setSlot(attach.getSlot())
                                .setUri(convertToKharonServantFsUri(attach.getUri()))
                                .setChannel(attach.getChannel())
                                .build())
                            .build());
                        break;
                    }
                    case DETACH: {
                        final Servant.SlotDetach detach = terminalState.getDetach();
                        executionProgress.onNext(Servant.ServantProgress.newBuilder()
                            .setDetach(Servant.SlotDetach.newBuilder()
                                .setSlot(detach.getSlot())
                                .setUri(convertToKharonServantFsUri(detach.getUri()))
                                .build())
                            .build());
                        break;
                    }
                    case SLOTSTATUS: {
                        final CompletableFuture<TerminalState> future = tasks.get(
                            UUID.fromString(terminalState.getCommandId()));
                        if (future == null) {
                            throw new IllegalStateException(
                                "No such task " + terminalState.getCommandId());
                        }
                        future.complete(terminalState);
                        break;
                    }
                    default:
                        throw new IllegalStateException("Unexpected value: " + terminalState.getProgressCase());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                LOG.error("Terminal execution terminated ", throwable);
                invalidate();
                if (executionProgress != null) {
                    executionProgress.onError(throwable);
                }
            }

            @Override
            public void onCompleted() {
                LOG.info("Terminal for " + user + " disconnected; sessionId = " + servantId);
                invalidate();
                if (executionProgress != null) {
                    executionProgress.onCompleted();
                }
            }
        };
    }

    public StreamObserver<Kharon.TerminalState> terminalStateObserver() {
        return terminalStateObserver;
    }

    public UUID sessionId() {
        return servantId;
    }

    public void setServantProgress(StreamObserver<Servant.ServantProgress> executionProgress) {
        executeFromServerFuture.complete(executionProgress);
    }

    public boolean isAlive() {
        return !invalid.get();
    }

    @Nullable
    public Servant.SlotCommandStatus configureSlot(SlotCommand request) {
        LOG.info("Kharon sessionId " + servantId + " ::configureSlot " + JsonUtils.printRequest(
            request));
        final CompletableFuture<Kharon.TerminalState> future = new CompletableFuture<>();
        final String commandId = generateCommandId(future);
        final Kharon.TerminalCommand sendingRequest = Kharon.TerminalCommand.newBuilder()
            .setCommandId(commandId)
            .setSlotCommand(request)
            .build();
        LOG.info("terminalController send request " + JsonUtils.printRequest(sendingRequest));
        synchronized (terminalController) {
            try {
                terminalController.onNext(sendingRequest);
            } catch (RuntimeException e) { // FIXME(d-kruchinin): why cannot catch StatusRuntimeException?
                LOG.warn(
                    "Kharon session={} was cancelled, but got configureSlot return -1;\n Cause: {}",
                    servantId, e);
                return Servant.SlotCommandStatus.newBuilder()
                    .setRc(RC.newBuilder().setCodeValue(-1)).build();
            }
        }
        try {
            return future.get(10, TimeUnit.SECONDS).getSlotStatus();
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LOG.error("Failed while configure slot in bidirectional stream " + e);
        }
        return Servant.SlotCommandStatus.newBuilder().build();
    }

    private void checkTerminalAndServantState() {
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

    private String convertToKharonServantFsUri(String slotStrUri) {
        try {
            final URI slotUri = URI.create(slotStrUri);
            return new URI(
                slotUri.getScheme(),
                null,
                kharonFsProxyAddress.getHost(),
                kharonFsProxyAddress.getPort(),
                slotUri.getPath(),
                SESSION_ID_KEY + "=" + servantId,
                null
            ).toString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        try {
            invalidate();
            terminalController.onCompleted();
        } catch (Exception e) {
            LOG.error("Failed to close stream with Terminal session_id {}. Already closed: ", servantId, e);
        }
    }

    private void invalidate() {
        invalid.set(true);
    }
}
