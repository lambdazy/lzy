package ai.lzy.util.auth.credentials;

public record YcIamCredentials(String token) implements Credentials {
    @Override
    public String type() {
        return "YC_IAM";
    }
}
