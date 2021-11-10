package ru.yandex.cloud.ml.platform.lzy.servant.fs;

import javax.annotation.Nullable;
import java.net.URI;

public interface LzyInputSlot extends LzySlot {
    void connect(URI slotUri);
    void connectPersistent(URI slotUri);
    @Nullable String getLinkToStorage();
    void disconnect();
    void destroy();
}
