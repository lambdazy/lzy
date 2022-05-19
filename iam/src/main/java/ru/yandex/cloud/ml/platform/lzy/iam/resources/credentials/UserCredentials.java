package ru.yandex.cloud.ml.platform.lzy.iam.resources.credentials;

public class UserCredentials {

    private final String name;
    private final String value;
    private final String type;

    public UserCredentials(String name, String value, String type) {
        this.name = name;
        this.value = value;
        this.type = type;
    }

    public String name() {
        return name;
    }

    public String value() {
        return value;
    }

    public String type() {
        return type;
    }
}
