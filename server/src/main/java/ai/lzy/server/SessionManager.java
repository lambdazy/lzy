package ai.lzy.server;

import java.util.stream.Stream;
import javax.annotation.Nullable;

public interface SessionManager {
    Session registerSession(String userId, String sessionId, String bucket);
    void deleteSession(String sessionId);

    Stream<Session> sessions(String userId);
    @Nullable
    Session get(String sessionId);
    @Nullable
    Session byServant(String servantId);

    /** [TODO] move this logic to kharon or even terminal */
    Session userSession(String user);

    interface Session {
        String id();
        String owner();
        String[] servants();
        String bucket();
    }
}
