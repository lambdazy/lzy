package ai.lzy.util.auth.credentials;

public record OttCredentials(String token) implements Credentials {
    @Override
    public String type() {
        return "OTT";
    }
}
