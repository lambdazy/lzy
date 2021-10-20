package ru.yandex.cloud.ml.platform.lzy.server.local;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import ru.yandex.cloud.ml.platform.lzy.model.SlotStatus;
import ru.yandex.cloud.ml.platform.lzy.server.TerminalController;
import ru.yandex.cloud.ml.platform.lzy.server.channel.Endpoint;

import javax.annotation.Nullable;
import java.net.URI;
import java.util.UUID;

public class TerminalEndpoint extends BaseEndpoint {
    private final TerminalController terminalController;

    public TerminalEndpoint(
        Slot slot,
        URI uri,
        UUID sessionId,
        TerminalController terminalController
    ) {
        super(slot, uri, sessionId);
        this.terminalController = terminalController;
    }

    @Override
    public int connect(Endpoint endpoint) {
        return terminalController.connect(this, endpoint);
    }

    @Override
    @Nullable
    public SlotStatus status() {
        return terminalController.status(this);
    }

    @Override
    public int disconnect() {
        return terminalController.disconnect(this);
    }

    @Override
    public int destroy() {
        return terminalController.destroy(this);
    }
}
