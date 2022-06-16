package ru.yandex.cloud.ml.platform.lzy.graph.model;

public record QueueEvent(String id, String workflowId, String graphId, Type type, String description) {

    public enum Type {
        START,
        STOP
    }
}
