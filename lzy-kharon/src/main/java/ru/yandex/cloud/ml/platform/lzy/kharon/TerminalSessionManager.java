package ru.yandex.cloud.ml.platform.lzy.kharon;

import io.grpc.stub.StreamObserver;
import ru.yandex.cloud.ml.platform.lzy.model.GrpcConstant;
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
    private final URI kharonServantAddress;

    public TerminalSessionManager(LzyServerBlockingStub server, URI kharonServantAddress) {
        this.server = server;
        this.kharonServantAddress = kharonServantAddress;
    }

    public StreamObserver<TerminalState> createSession(StreamObserver<TerminalCommand> terminalCommandObserver) {
        final TerminalSession terminalSession = new TerminalSession(server, terminalCommandObserver, kharonServantAddress);
        sessions.put(terminalSession.getSessionId(), terminalSession);
        return terminalSession.getTerminalStateObserver();
    }

    public TerminalSession getTerminalSessionFromGrpcContext() {
        final UUID sessionId = UUID.fromString(GrpcConstant.SESSION_ID_CTX_KEY.get());
        final TerminalSession session = sessions.get(sessionId);
        if (session == null) {
            throw new IllegalStateException("Failed to parse sessionId from Grpc Context: Unknown sessionId " + sessionId);
        }
        return session;
    }

    public TerminalSession getTerminalSessionFromSlotUri(String slotUri) {
        final UUID sessionIdFromUri = parseSessionIdFromUri(slotUri);
        final TerminalSession session = sessions.get(sessionIdFromUri);
        if (session == null) {
            throw new IllegalStateException(
                String.format("Failed to parse sessionId from slot uri %s: Unknown sessionId %s", slotUri, sessionIdFromUri)
            );
        }
        return session;
    }

    private UUID parseSessionIdFromUri(String slotUri) {
        final URI uri = URI.create(slotUri);
        for (String queryPart: uri.getQuery().split("\\?")) {
            final int equalPos = queryPart.indexOf('=');
            final String key = queryPart.substring(0, equalPos);
            final String value = queryPart.substring(equalPos + 1);
            if (key.equals(TerminalSession.SESSION_ID_KEY)) {
                return UUID.fromString(value);
            }
        }
        return null;
    }
}
