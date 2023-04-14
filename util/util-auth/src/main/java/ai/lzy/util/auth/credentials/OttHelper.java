package ai.lzy.util.auth.credentials;

import ai.lzy.util.grpc.ClientHeaderInterceptor;

import java.util.Base64;

public final class OttHelper {
    private OttHelper() {
    }

    public static ClientHeaderInterceptor<String> createOttClientInterceptor(String subjectId, String ott) {
        var auth = Base64.getEncoder().encodeToString((subjectId + '/' + ott).getBytes());
        return ClientHeaderInterceptor.authorization(() -> auth);
    }

    public record DecodedOtt(
        String subjectId,
        String ott
    ) {
        public String toStringSafe() {
            return subjectId + "/" + ott.substring(4);
        }
    }

    public static DecodedOtt decodeOtt(OttCredentials credentials) {
        var decoded = new String(Base64.getDecoder().decode(credentials.token()));
        int separatorIdx = decoded.indexOf('/');

        var subjectId = decoded.substring(0, separatorIdx);
        var token = decoded.substring(separatorIdx + 1);

        return new DecodedOtt(subjectId, token);
    }
}
