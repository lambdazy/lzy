package ai.lzy.util.auth.credentials;

public record TaskCredentials(String token) implements Credentials {

    @Override
    public String type() {
        return "OTT";
    }
}
