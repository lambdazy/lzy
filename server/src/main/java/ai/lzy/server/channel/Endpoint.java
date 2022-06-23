package ai.lzy.server.channel;

import ai.lzy.model.Slot;
import ai.lzy.model.SlotStatus;

import java.net.URI;

public interface Endpoint {
    URI uri();

    Slot slot();

    String sessionId();

    int connect(URI endpoint);

    SlotStatus status();

    int disconnect();

    int destroy();

    void invalidate();

    boolean isInvalid();
}
