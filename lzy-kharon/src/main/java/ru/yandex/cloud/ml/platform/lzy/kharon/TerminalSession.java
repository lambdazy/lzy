package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;

import java.util.UUID;

public class TerminalSession {
    public static final String SESSION_ID_KEY = "kharon_session_id";
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private TerminalSessionState state = TerminalSessionState.UNBOUND;

    private final TerminalController terminalController;
    private final TerminalStateHandler terminalStateHandler;
    private final ServerController serverController;

    private final UUID sessionId;

    public TerminalSession(
        UUID sessionId,
        TerminalController terminalController,
        ServerController serverController
    ) {
        this.sessionId = sessionId;
        this.terminalController = terminalController;
        this.serverController = serverController;
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
            LOG.info("Terminal with sessionId = {} disconnected", sessionId);
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

    public void close() {
        try {
            invalidate();
            terminalController.onCompleted();
        } catch (Exception e) {
            LOG.error("Failed to close stream with Terminal session_id {}. Already closed: ", sessionId, e);
        }
    }
}
