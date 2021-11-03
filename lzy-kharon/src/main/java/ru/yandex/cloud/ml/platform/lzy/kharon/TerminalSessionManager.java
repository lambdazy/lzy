package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalCommand;
import yandex.cloud.priv.datasphere.v2.lzy.Kharon.TerminalState;
import yandex.cloud.priv.datasphere.v2.lzy.LzyServerGrpc.LzyServerBlockingStub;

import java.net.URI;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TerminalSessionManager {
    private final Map<UUID, TerminalSession> sessions = new ConcurrentHashMap<>();
    private final LzyServerBlockingStub server;
    private final URI kharonAddress;
    private final URI kharonServantAddress;

    public TerminalSessionManager(LzyServerBlockingStub server, URI kharonAddress, URI kharonServantAddress) {
        this.server = server;
        this.kharonAddress = kharonAddress;
        this.kharonServantAddress = kharonServantAddress;
    }

    public StreamObserver<TerminalState> createSession(StreamObserver<TerminalCommand> terminalCommandObserver) {
        final TerminalSession terminalSession = new TerminalSession(server, terminalCommandObserver, kharonAddress, kharonServantAddress);
        sessions.put(terminalSession.getSessionId(), terminalSession);
        return terminalSession.getTerminalStateObserver();
    }

    public TerminalSession getSession(UUID sessionId) {
        return sessions.values().stream().findFirst().get();
        // TODO(d-kruchinin): multiple sessions
        // return sessions.get(sessionId);
    }
}
