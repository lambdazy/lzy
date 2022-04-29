package ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects;

public class User implements Subject {
    private final String id;

    public User(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }
}
