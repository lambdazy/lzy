package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import java.net.URI;

public interface LzyInputSlot extends LzySlot {
    void connect(URI slotUri);
    void disconnect();
    void close();
}
