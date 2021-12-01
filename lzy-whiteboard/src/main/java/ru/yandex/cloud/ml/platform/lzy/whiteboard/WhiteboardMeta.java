package ru.yandex.cloud.ml.platform.lzy.whiteboard;

import yandex.cloud.priv.datasphere.v2.lzy.Tasks;

import java.net.URI;
import java.util.List;
import java.util.UUID;

public class WhiteboardMeta {
    private final URI wbId;
    private final List<String> dependencies;
    private final String fieldName;

    WhiteboardMeta(URI wbId, List<String> dependencies, String fieldName) {
        this.wbId = wbId;
        this.dependencies = dependencies;
        this.fieldName = fieldName;
    }

    public static WhiteboardMeta from(Tasks.WhiteboardMeta meta) {
        return new WhiteboardMeta(URI.create(meta.getWhiteboardId()), meta.getDependenciesList(), meta.getFieldName());
    }

    public static Tasks.WhiteboardMeta to(WhiteboardMeta meta) {
        return Tasks.WhiteboardMeta.newBuilder()
                .setWhiteboardId(meta.getWbId().toString())
                .addAllDependencies(meta.getDependencies())
                .setFieldName(meta.getFieldName())
                .build();
    }

    public URI getWbId() {
        return wbId;
    }

    public List<String> getDependencies() {
        return dependencies;
    }

    public String getFieldName() {
        return fieldName;
    }
}
