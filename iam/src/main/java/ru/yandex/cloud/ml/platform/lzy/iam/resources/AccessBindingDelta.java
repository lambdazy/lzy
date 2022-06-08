package ru.yandex.cloud.ml.platform.lzy.iam.resources;

public record AccessBindingDelta(
        AccessBindingAction action,
        AccessBinding binding) {

    public enum AccessBindingAction {
        ADD,
        REMOVE,
    }

}
