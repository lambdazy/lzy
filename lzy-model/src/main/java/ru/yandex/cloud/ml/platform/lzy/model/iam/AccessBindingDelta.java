package ru.yandex.cloud.ml.platform.lzy.model.iam;

public class AccessBindingDelta {

    private final AccessBindingAction action;
    private final AccessBinding binding;

    public AccessBindingDelta(AccessBindingAction action, AccessBinding binding) {
        this.action = action;
        this.binding = binding;
    }

    public AccessBindingAction action() {
        return action;
    }

    public AccessBinding binding() {
        return binding;
    }

    public enum AccessBindingAction {
        ADD,
        REMOVE,
    }

}
