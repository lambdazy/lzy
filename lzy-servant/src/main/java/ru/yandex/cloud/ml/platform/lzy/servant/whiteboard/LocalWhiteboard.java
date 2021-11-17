package ru.yandex.cloud.ml.platform.lzy.servant.whiteboard;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;

public class LocalWhiteboard implements Whiteboard {
    @Override
    public void prepareToSaveData(Slot slot, String key) {
        // create entry in database
        // set status to IN_PROGRESS
    }

    @Override
    public void commit(Slot slot) {
        // set status to FINISHED
    }
}
