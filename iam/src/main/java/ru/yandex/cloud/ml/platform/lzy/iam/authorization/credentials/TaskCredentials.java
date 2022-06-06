package ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials;

public record TaskCredentials(String token) implements Credentials {

    @Override
    public String type() {
        return "ott";
    }
}
