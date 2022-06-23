package ai.lzy.kharon;

import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.model.JsonUtils;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.AttachTerminal;
import yandex.cloud.priv.datasphere.v2.lzy.Servant;

public class TerminalSession {

    public static final String SESSION_ID_KEY = "kharon_session_id";
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private TerminalSessionState state = TerminalSessionState.UNBOUND;

    private final TerminalController terminalController;
    private final ServerCommandHandler serverCommandHandler;
    private final ServerControllerFactory serverControllerFactory;
    private ServerController serverController;

    private final String sessionId;

    public TerminalSession(
        String sessionId,
        TerminalController terminalController,
        ServerControllerFactory serverControllerFactory
    ) {
        this.sessionId = sessionId;
        this.terminalController = terminalController;
        this.serverControllerFactory = serverControllerFactory;
        this.serverCommandHandler = new ServerCommandHandler();
    }

    public class ServerCommandHandler implements StreamObserver<Kharon.ServerCommand> {

        @Override
        public void onNext(Kharon.ServerCommand terminalState) {
            LOG.info("Kharon::TerminalSession session_id:" + sessionId + " request:"
                + JsonUtils.printRequest(terminalState));

            try {
                switch (terminalState.getProgressCase()) {
                    case ATTACHTERMINAL -> {
                        //synchronize this block to avoid race between serverController instance building
                        // and setServerStream call (because server can call start before serverController
                        // instance is built)
                        synchronized (ServerCommandHandler.this) {
                            final AttachTerminal attachTerminal = terminalState.getAttachTerminal();
                            if (state != TerminalSessionState.UNBOUND) {
                                throw new IllegalStateException("Double attach to terminal from user "
                                    + attachTerminal.getAuth().getUserId());
                            }
                            updateState(TerminalSessionState.TERMINAL_ATTACHED);
                            serverController = serverControllerFactory.createInstance(attachTerminal.getAuth(),
                                sessionId);
                        }
                    }
                    case ATTACH -> serverController.attach(terminalState.getAttach());
                    case DETACH -> serverController.detach(terminalState.getDetach());
                    case TERMINALRESPONSE -> terminalController.handleTerminalResponse(
                        terminalState.getTerminalResponse());
                    default -> throw new IllegalStateException("Unexpected value: " + terminalState.getProgressCase());
                }
            } catch (ServerController.ServerControllerResetException e) {
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

    public String sessionId() {
        return sessionId;
    }

    public synchronized TerminalSessionState state() {
        return state;
    }

    private synchronized void updateState(TerminalSessionState state) {
        if (this.state == TerminalSessionState.ERRORED || this.state == TerminalSessionState.COMPLETED) {
            LOG.warn(
                "TerminalSession sessionId={} attempt to change final state {} to {}",
                sessionId,
                this.state,
                state
            );
            return;
        }
        this.state = state;
    }

    public StreamObserver<Kharon.ServerCommand> serverCommandHandler() {
        return serverCommandHandler;
    }

    public TerminalController terminalController() {
        return terminalController;
    }

    public synchronized void setServerStream(StreamObserver<Servant.ServantProgress> responseObserver)
        throws ServerController.ServerControllerResetException {
        serverController.setProgress(responseObserver);
        updateState(TerminalSessionState.CONNECTION_ESTABLISHED);
    }

    public synchronized void onServerDisconnect() {
        LOG.info("Server DISCONNECTED for sessionId = {}", sessionId);
        updateState(TerminalSessionState.COMPLETED);
        serverController.onDisconnect();
        terminalController.complete();
    }

    public synchronized void onTerminalDisconnect() {
        LOG.info("Terminal DISCONNECTED for sessionId = {}", sessionId);
        updateState(TerminalSessionState.COMPLETED);
        terminalController.onDisconnect();
        serverController.complete();
    }
}