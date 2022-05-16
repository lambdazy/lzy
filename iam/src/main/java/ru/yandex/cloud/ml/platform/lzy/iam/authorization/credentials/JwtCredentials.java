package ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials;

public class JwtCredentials implements Credentials {
    private final String token;

    public JwtCredentials(String token) {
        this.token = token;
    }

    public String token() {
        return token;
    }

    @Override
    public String type() {
        return "public_key";
    }
}
