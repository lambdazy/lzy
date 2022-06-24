package ai.lzy.iam.resources;

public record AccessBindingDelta(
        AccessBindingAction action,
        AccessBinding binding) {

    public enum AccessBindingAction {
        ADD,
        REMOVE,
    }

}
