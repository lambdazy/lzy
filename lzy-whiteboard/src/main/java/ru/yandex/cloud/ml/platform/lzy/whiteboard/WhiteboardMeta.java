package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.util.List;
import java.util.UUID;

public class WhiteboardMeta {
    private final UUID wbId;
    private final List<String> dependencies;
    private final String fieldName;

    WhiteboardMeta(UUID wbId, List<String> dependencies, String fieldName) {
        this.wbId = wbId;
        this.dependencies = dependencies;
        this.fieldName = fieldName;
    }

    public static WhiteboardMeta from(Tasks.WhiteboardMeta meta) {
        return new WhiteboardMeta(UUID.fromString(meta.getWhiteboardId()), meta.getDependenciesList(), meta.getFieldName());
    }

    public static Tasks.WhiteboardMeta to(WhiteboardMeta meta) {
        return Tasks.WhiteboardMeta.newBuilder()
                .setWhiteboardId(meta.getWbId().toString())
                .addAllDependencies(meta.getDependencies())
                .setFieldName(meta.getFieldName())
                .build();
    }

    public UUID getWbId() {
        return wbId;
    }

    public List<String> getDependencies() {
        return dependencies;
    }

    public String getFieldName() {
        return fieldName;
    }
}
