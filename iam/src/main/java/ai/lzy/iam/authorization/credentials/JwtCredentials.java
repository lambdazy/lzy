package ai.lzy.iam.authorization.credentials;

public record JwtCredentials(String token) implements Credentials {

    @Override
    public String type() {
        return "public_key";
    }
}
