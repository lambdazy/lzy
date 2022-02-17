package ru.yandex.cloud.ml.platform.lzy.iam.authorization;

import java.util.List;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResources;

public interface AccessBindingClient {

    List<AccessBinding> listAccessBindings(AuthResources resource);

    void setAccessBindings(AuthResources resource, List<AccessBinding> accessBinding);

    void updateAccessBindings(AuthResources resource, List<AccessBindingDelta> accessBindingDeltas);

    class AccessBinding {

        private final String role;
        private final String userId;

        public AccessBinding(String role, String subject) {
            this.role = role;
            this.userId = subject;
        }

        public String role() {
            return role;
        }

        public String subject() {
            return userId;
        }
    }

    class AccessBindingDelta {

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

}
