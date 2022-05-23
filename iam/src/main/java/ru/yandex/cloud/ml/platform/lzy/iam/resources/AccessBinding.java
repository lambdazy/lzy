package ru.yandex.cloud.ml.platform.lzy.iam.resources;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;

public class AccessBinding {

    private final String role;
    private final Subject subject;

    public AccessBinding(String role, Subject subject) {
        this.role = role;
        this.subject = subject;
    }

    public String role() {
        return role;
    }

    public Subject subject() {
        return subject;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final AccessBinding that = (AccessBinding) o;
        return this.role.equals(that.role) && this.subject.id().equals(that.subject().id());
    }
}
