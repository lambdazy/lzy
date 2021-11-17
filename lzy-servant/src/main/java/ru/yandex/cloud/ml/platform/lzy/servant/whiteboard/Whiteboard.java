package ru.yandex.cloud.ml.platform.lzy.servant.whiteboard;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;

import java.net.URI;

public interface Whiteboard {
    enum status {
        IN_PROGRESS,
        FINISHED
    }
    // create entry in the database and set status IN_PROGRESS
    void prepareToSaveData(Slot slot, URI uri);

    // set status FINISHED
    void commit(Slot slot);
}
