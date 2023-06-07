package ai.lzy.iam.resources;

import ai.lzy.iam.resources.subjects.Subject;

public record AccessBinding(
    Role role,
    Subject subject
) {
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AccessBinding that = (AccessBinding) o;
        return this.role() == that.role() && this.subject().id().equals(that.subject().id());
    }
}
