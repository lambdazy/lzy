package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import yandex.cloud.priv.datasphere.v2.lzy.Operations;
import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.ArrayList;
import java.util.UUID;

public class WhiteboardMeta {
    private final UUID wbId;
    private final ArrayList<String> dependencies;
    private final String operationName;

    WhiteboardMeta(UUID wbId, ArrayList<String> dependencies, String operationName) {
        this.wbId = wbId;
        this.dependencies = dependencies;
        this.operationName = operationName;
    }

    public static WhiteboardMeta from(Tasks.WhiteboardMeta meta) {
        ArrayList<String> dependencies = new ArrayList<>();
        return new WhiteboardMeta(UUID.fromString(meta.getWhiteboardId()), dependencies, meta.getOpName());
    }

    public UUID getWbId() {
        return wbId;
    }

    public ArrayList<String> getDependencies() {
        return dependencies;
    }

    public String getOperationName() {
        return operationName;
    }
}
