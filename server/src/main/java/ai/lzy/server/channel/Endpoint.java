package ai.lzy.server.channel;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;

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
