package ru.yandex.cloud.ml.platform.lzy.iam.resources;

public class AccessBinding {

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
