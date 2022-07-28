package ai.lzy.kharon;

import ai.lzy.model.JsonUtils;
import ai.lzy.v1.IAM.UserCredentials;
import ai.lzy.v1.Kharon;
import ai.lzy.v1.Kharon.TerminalProgress.ProgressCase;
import ai.lzy.v1.Lzy.RegisterSessionRequest;
import ai.lzy.v1.Lzy.UnregisterSessionRequest;
import ai.lzy.v1.LzyServerGrpc;
import ai.lzy.v1.LzyServerGrpc.LzyServerBlockingStub;
import io.grpc.stub.StreamObserver;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TerminalSession {

    private static final Logger LOG = LogManager.getLogger(TerminalSession.class);

    private TerminalSessionState state = TerminalSessionState.UNBOUND;

    private final TerminalController terminalController;
    private final LzyServerBlockingStub server;
    private final TerminalProgressHandler terminalProgressHandler;
    private final AtomicReference<UserCredentials> creds = new AtomicReference<>();

    private final String sessionId;

    public TerminalSession(
        String sessionId,
        TerminalController terminalController,
        LzyServerGrpc.LzyServerBlockingStub server
    ) {
        this.sessionId = sessionId;
        this.terminalController = terminalController;
        this.server = server;
        this.terminalProgressHandler = new TerminalProgressHandler();
    }

    public class TerminalProgressHandler implements StreamObserver<Kharon.TerminalProgress> {

        @Override
        public void onNext(Kharon.TerminalProgress terminalState) {
            LOG.info("Kharon::TerminalSession session_id:" + sessionId + " request:"
                + JsonUtils.printRequest(terminalState));
            if (terminalState.getProgressCase() == ProgressCase.ATTACH) {
                creds.set(terminalState.getAuth());
                //noinspection ResultOfMethodCallIgnored
                server.registerSession(
                    RegisterSessionRequest.newBuilder().setAuth(terminalState.getAuth().toBuilder().build())
                        .setSessionId(sessionId)
                        .build());
            } else if (terminalState.getProgressCase() == Kharon.TerminalProgress.ProgressCase.TERMINALRESPONSE) {
                terminalController.handleTerminalResponse(terminalState.getTerminalResponse());
            } else {
                throw new IllegalStateException("Unexpected value: " + terminalState.getProgressCase());
            }
        }

        @Override
        public void onError(Throwable throwable) {
            LOG.error("Terminal connection with sessionId={} terminated, exception = {} ", sessionId, throwable);
            updateState(TerminalSessionState.ERRORED);
            terminalController.terminate(throwable);
        }

        @Override
        public void onCompleted() {
            LOG.info("Terminal connection with sessionId={} completed", sessionId);
            updateState(TerminalSessionState.COMPLETED);
            terminalController.complete();
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

    public synchronized void onTerminalDisconnect(@Nullable Throwable cause) {
        try {
            LOG.info("Terminal DISCONNECTED for sessionId = {}, cause = {}", sessionId, cause);
            if (cause == null) {
                updateState(TerminalSessionState.COMPLETED);
            } else {
                updateState(TerminalSessionState.ERRORED);
            }
        } finally {
            final UserCredentials userCredentials = creds.get();
            if (userCredentials != null) {
                //noinspection ResultOfMethodCallIgnored
                server.unregisterSession(
                    UnregisterSessionRequest.newBuilder().setAuth(userCredentials).setSessionId(sessionId).build());
            }
        }
    }
}