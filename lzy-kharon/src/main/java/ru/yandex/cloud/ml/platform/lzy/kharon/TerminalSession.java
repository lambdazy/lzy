package ru.yandex.cloud.ml.platform.lzy.kharon;

import static ru.yandex.cloud.ml.platform.lzy.kharon.TerminalSessionState.COMPLETED;
import static ru.yandex.cloud.ml.platform.lzy.kharon.TerminalSessionState.ERRORED;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import java.net.URI;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalState.ProgressCase;

public class TerminalSession {
    public static final String SESSION_ID_KEY = "kharon_session_id";
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private TerminalSessionState state = TerminalSessionState.UNBOUND;

    private final TerminalConnection terminalConnection;
    private ServerConnection serverConnection;

    private final UUID sessionId = UUID.randomUUID();
    private final URI kharonServantProxyAddress;
    private final URI kharonFsProxyAddress;

    public TerminalSession(
        TerminalConnection terminalConnection,
        URI kharonServantAddress,
        URI kharonFsAddress
    ) {
        this.kharonServantProxyAddress = kharonServantAddress;
        this.kharonFsProxyAddress = kharonFsAddress;
        this.terminalConnection = terminalConnection;
        StreamObserver<Kharon.TerminalState> terminalStateObserver = new StreamObserver<>() {
            @Override
            public void onNext(Kharon.TerminalState terminalState) {
                LOG.info("Kharon::TerminalSession session_id:" + sessionId + " request:"
                    + JsonUtils.printRequest(terminalState));

                final ProgressCase progressCase = terminalState.getProgressCase();
                if (progressCase == ProgressCase.ATTACH || progressCase == ProgressCase.DETACH) {
                    waitForState(TerminalSessionState.CONNECTION_ESTABLISHED);
                }

                switch (terminalState.getProgressCase()) {
                    case ATTACHTERMINAL: {
                        final AttachTerminal attachTerminal = terminalState.getAttachTerminal();
                        if (state != TerminalSessionState.UNBOUND) {
                            throw new IllegalStateException("Double attach to terminal from user "
                                    + attachTerminal.getAuth().getUserId());
                        }
                        updateState(TerminalSessionState.TERMINAL_ATTACHED);

                        try {
                            serverConnection.register(attachTerminal.getAuth(), kharonServantAddress, kharonFsAddress, sessionId);
                        } catch (StatusRuntimeException e) {
                            LOG.error("registerServant failed. " + e);
                            terminalController.onError(e);
                        }
                        break;
                    }
                    case ATTACH: {
                        serverConnection.attach(terminalState.getAttach());
                        break;
                    }
                    case DETACH: {
                        serverConnection.detach(terminalState.getDetach());
                        break;
                    }
                    case SLOTSTATUS: {
                        terminalConnection.slotStatus(
                            UUID.fromString(terminalState.getCommandId()),
                            terminalState.getSlotStatus()
                        );
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
                serverConnection.terminate(throwable);
            }

            @Override
            public void onCompleted() {
                LOG.info("Terminal for " + user + " disconnected; sessionId = " + sessionId);
                invalidate();
                serverConnection.complete();
            }
        };
    }

    public StreamObserver<Kharon.TerminalState> terminalStateObserver() {
        return terminalStateObserver;
    }

    public UUID sessionId() {
        return sessionId;
    }

    public TerminalSessionState state() {
        return state;
    }

    private synchronized void updateState(TerminalSessionState state) {
        this.state = state;
        notifyAll();
    }

    private synchronized void waitForState(TerminalSessionState state) {
        while (state != this.state && this.state != ERRORED && this.state != COMPLETED) {
            try {
                wait();
            } catch (InterruptedException ignore) {
                // Ignored exception
            }
        }
    }

    public boolean isAlive() {
        return !invalid.get();
    }

    public void close() {
        try {
            invalidate();
            terminalController.onCompleted();
        } catch (Exception e) {
            LOG.error("Failed to close stream with Terminal session_id {}. Already closed: ", sessionId, e);
        }
    }

    private void invalidate() {
        invalid.set(true);
    }
}
