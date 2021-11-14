package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import ru.yandex.cloud.ml.platform.lzy.servant.slots.SlotConnectionManager.SlotController;

import java.net.URI;

public interface LzyInputSlot extends LzySlot {
    void connect(URI slotUri, SlotController slotController);
    void disconnect();
    void destroy();
}
