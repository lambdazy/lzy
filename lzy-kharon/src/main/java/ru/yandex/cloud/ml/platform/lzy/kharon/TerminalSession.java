package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;

import java.util.UUID;

import static ru.yandex.cloud.ml.platform.lzy.kharon.TerminalSessionState.CONNECTION_ESTABLISHED;

public class TerminalSession {
    public static final String SESSION_ID_KEY = "kharon_session_id";
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private TerminalSessionState state = TerminalSessionState.UNBOUND;

    private final TerminalController terminalController;
    private final TerminalStateHandler terminalStateHandler;
    private final ServerController serverController;

    private final UUID sessionId = UUID.randomUUID();

    public TerminalSession(
        TerminalController terminalController
    ) {
        this.terminalController = terminalController;
        this.terminalStateHandler = new TerminalStateHandler();
    }

    public class TerminalStateHandler implements StreamObserver<Kharon.TerminalState> {
        @Override
        public void onNext(Kharon.TerminalState terminalState) {
            LOG.info("Kharon::TerminalSession session_id:" + sessionId + " request:"
                    + JsonUtils.printRequest(terminalState));

            switch (terminalState.getProgressCase()) {
                case ATTACHTERMINAL: {
                    final AttachTerminal attachTerminal = terminalState.getAttachTerminal();
                    if (state != TerminalSessionState.UNBOUND) {
                        throw new IllegalStateException("Double attach to terminal from user "
                                + attachTerminal.getAuth().getUserId());
                    }
                    updateState(TerminalSessionState.TERMINAL_ATTACHED);

                    try {
                        serverController.register(attachTerminal.getAuth());
                    } catch (StatusRuntimeException e) {
                        LOG.error("registerServant failed. " + e);
                        terminalController.onError(e);
                    }
                    break;
                }
                case ATTACH: {
                    serverController.attach(terminalState.getAttach());
                    break;
                }
                case DETACH: {
                    serverController.detach(terminalState.getDetach());
                    break;
                }
                case TERMINALRESPONSE: {
                    terminalController.handleTerminalResponse(terminalState.getTerminalResponse());
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
            serverController.terminate(throwable);
        }

        @Override
        public void onCompleted() {
            LOG.info("Terminal for " + user + " disconnected; sessionId = " + sessionId);
            invalidate();
            serverController.complete();
        }
    }

    public UUID sessionId() {
        return sessionId;
    }

    public TerminalSessionState state() {
        return state;
    }

    public StreamObserver<Kharon.TerminalState> terminalStateHandler() {
        return terminalStateHandler;
    }

    public TerminalController terminalController() {
        return terminalController;
    }

    public void setServerController(ServerController serverController) {
        if (state().ordinal() >= CONNECTION_ESTABLISHED.ordinal()) {
            throw new IllegalStateException("Server controller set twice");
        }
        this.serverController = serverController;
        updateState(CONNECTION_ESTABLISHED);
    }

    public void close() {
        try {
            invalidate();
            terminalController.onCompleted();
        } catch (Exception e) {
            LOG.error("Failed to close stream with Terminal session_id {}. Already closed: ", sessionId, e);
        }
    }
}
