package ai.lzy.kharon;

import ai.lzy.model.JsonUtils;
import ai.lzy.v1.Kharon;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TerminalSession {
    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private TerminalSessionState state = TerminalSessionState.UNBOUND;

    private final TerminalController terminalController;
    private final TerminalProgressHandler terminalProgressHandler;

    private final String sessionId;

    public TerminalSession(
        String sessionId,
        TerminalController terminalController
    ) {
        this.sessionId = sessionId;
        this.terminalController = terminalController;
        this.terminalProgressHandler = new TerminalProgressHandler();
    }

    public class TerminalProgressHandler implements StreamObserver<Kharon.TerminalProgress> {
        @Override
        public void onNext(Kharon.TerminalProgress terminalState) {
            LOG.info("Kharon::TerminalSession session_id:" + sessionId + " request:"
                + JsonUtils.printRequest(terminalState));

            if (terminalState.getProgressCase() == Kharon.TerminalProgress.ProgressCase.TERMINALRESPONSE) {
                terminalController.handleTerminalResponse(terminalState.getTerminalResponse());
            } else {
                throw new IllegalStateException("Unexpected value: " + terminalState.getProgressCase());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            LOG.error("Terminal connection with sessionId={} terminated, exception = {} ", sessionId, throwable);
            updateState(TerminalSessionState.ERRORED);
        }

        @Override
        public void onCompleted() {
            LOG.info("Terminal connection with sessionId={} completed", sessionId);
            updateState(TerminalSessionState.COMPLETED);
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

    public StreamObserver<Kharon.TerminalProgress> terminalProgressHandler() {
        return terminalProgressHandler;
    }

    public TerminalController terminalController() {
        return terminalController;
    }

    public synchronized void onTerminalDisconnect() {
        LOG.info("Terminal DISCONNECTED for sessionId = {}", sessionId);
        updateState(TerminalSessionState.COMPLETED);
        terminalController.onDisconnect();
    }
}