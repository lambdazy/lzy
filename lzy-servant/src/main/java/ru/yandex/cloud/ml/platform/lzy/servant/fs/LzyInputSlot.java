package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.net.URI;
import ru.yandex.cloud.ml.platform.lzy.servant.slots.SlotConnectionManager.SlotController;

public interface LzyInputSlot extends LzySlot {
    void connect(URI slotUri, SlotController slotController);

    void disconnect();

    void destroy();
}
