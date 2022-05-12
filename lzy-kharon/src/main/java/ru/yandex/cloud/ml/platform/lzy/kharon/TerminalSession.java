package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.kharon.ServerController.ServerControllerResetException;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;

import java.util.UUID;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public class TerminalSession {
    public static final String SESSION_ID_KEY = "kharon_session_id";
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private TerminalSessionState state = TerminalSessionState.UNBOUND;

    private final TerminalController terminalController;
    private final ServerCommandHandler serverCommandHandler;
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
        this.serverCommandHandler = new ServerCommandHandler();
    }

    public class ServerCommandHandler implements StreamObserver<Kharon.ServerCommand> {
        @Override
        public void onNext(Kharon.ServerCommand terminalState) {
            LOG.info("Kharon::TerminalSession session_id:" + sessionId + " request:"
                    + JsonUtils.printRequest(terminalState));

            try {
                switch (terminalState.getProgressCase()) {
                    case ATTACHTERMINAL: {
                        final AttachTerminal attachTerminal = terminalState.getAttachTerminal();
                        if (state != TerminalSessionState.UNBOUND) {
                            throw new IllegalStateException("Double attach to terminal from user "
                                + attachTerminal.getAuth().getUserId());
                        }
                        updateState(TerminalSessionState.TERMINAL_ATTACHED);
                        serverController.register(attachTerminal.getAuth());
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
            } catch (ServerControllerResetException e) {
                LOG.error(e);
                updateState(TerminalSessionState.ERRORED);
                terminalController.terminate(e);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            LOG.error("Terminal connection with sessionId={} terminated, exception = {} ", sessionId, throwable);
            updateState(TerminalSessionState.ERRORED);
            serverController.terminate(throwable);
        }

        @Override
        public void onCompleted() {
            LOG.info("Terminal connection with sessionId={} completed", sessionId);
            updateState(TerminalSessionState.COMPLETED);
            serverController.complete();
        }
    }

    public UUID sessionId() {
        return sessionId;
    }

    public TerminalSessionState state() {
        return state;
    }

    private synchronized void updateState(TerminalSessionState state) {
        this.state = state;
    }

    public StreamObserver<Kharon.ServerCommand> serverCommandHandler() {
        return serverCommandHandler;
    }

    public TerminalController terminalController() {
        return terminalController;
    }

    public void setServerStream(StreamObserver<Servant.ServantProgress> responseObserver)
        throws ServerControllerResetException {
        serverController.setProgress(responseObserver);
        updateState(TerminalSessionState.CONNECTION_ESTABLISHED);
    }

    public void onServerDisconnect() {
        LOG.info("Server DISCONNECTED for sessionId = {}", sessionId);
        updateState(TerminalSessionState.COMPLETED);
        terminalController.complete();
    }

    public void onTerminalDisconnect() {
        LOG.info("Terminal DISCONNECTED for sessionId = {}", sessionId);
        updateState(TerminalSessionState.COMPLETED);
        serverController.complete();
    }
}
