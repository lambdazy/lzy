package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import ru.yandex.cloud.ml.platform.lzy.model.Slot;
import yandex.cloud.priv.datasphere.v2.lzy.IAM;

import java.net.URI;
import java.util.ArrayList;
import java.util.UUID;

public abstract class WhiteboardManager {
    // IN_PROGRESS --> started saving data
    // FINISHED --> finished saving data
    enum status {
        IN_PROGRESS,
        FINISHED
    }

    // create entry in the database and set status IN_PROGRESS
    public abstract void prepareToSaveData(UUID wbId, String operationName, Slot slot, URI uri);

    // set status FINISHED
    public abstract void commit(UUID wbId, String operationName, Slot slot);
    public abstract void addDependency(UUID wbId, String from, String to);
    public abstract void addDependencies(UUID wbId, ArrayList<String> from, String to);
    public abstract void getWhiteboardById(UUID wbId, IAM.UserCredentials auth);
}
