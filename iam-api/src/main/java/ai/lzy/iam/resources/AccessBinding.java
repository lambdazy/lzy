package ai.lzy.iam.resources;

import ai.lzy.iam.resources.subjects.Subject;

public record AccessBinding(String role,
                            Subject subject) {

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AccessBinding that = (AccessBinding) o;
        return this.role().equals(that.role()) && this.subject().id().equals(that.subject().id());
    }
}
