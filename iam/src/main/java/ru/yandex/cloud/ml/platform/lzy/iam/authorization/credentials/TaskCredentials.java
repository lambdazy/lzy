package ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials;

public class TaskCredentials implements Credentials {
    private final String token;

    public TaskCredentials(String token) {
        this.token = token;
    }

    public String token() {
        return token;
    }

    @Override
    public String type() {
        return "ott";
    }
}
