package ai.lzy.iam.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Base64;
import java.util.Objects;
import java.util.regex.Pattern;

public final class TokenParser {
    private static final Logger LOG = LogManager.getLogger(TokenParser.class);

    private static final Pattern JWT_TOKEN_PATTERN = Pattern.compile(
            "^[A-Za-z0-9-_]+\\.[A-Za-z0-9-_]+\\.[A-Za-z0-9-_]*$");

    // format: "<provider_login>/<uuid>"
    private static final Pattern OTT_TOKEN_PATTERN = Pattern.compile(
            "^[A-Za-z0-9-_]+/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

    // https://cloud.yandex.ru/docs/iam/concepts/authorization/iam-token#iam-token-format
    private static final Pattern YC_IAM_TOKEN_PATTERN = Pattern.compile(
        "^t1\\.[A-Z0-9a-z_-]+[=]{0,2}\\.[A-Z0-9a-z_-]{86}[=]{0,2}$");

    public TokenParser() {
    }

    public static Token parse(String header) throws IllegalArgumentException {
        Objects.requireNonNull(header, "header is null");

        if (header.startsWith("Bearer ")) {
            var token = header.substring("Bearer ".length()).trim();

            var ycIamMatcher = YC_IAM_TOKEN_PATTERN.matcher(token);
            if (ycIamMatcher.find()) {
                return new Token(Token.Kind.YC_IAM, token);
            }

            var jwtMatcher = JWT_TOKEN_PATTERN.matcher(token);
            if (jwtMatcher.find()) {
                return new Token(Token.Kind.JWT, token);
            }

            try {
                // base64("<provider_subject_id>/<token>")
                var decodedToken = new String(Base64.getDecoder().decode(token.getBytes()));
                var ottMatcher = OTT_TOKEN_PATTERN.matcher(decodedToken);
                if (ottMatcher.find()) {
                    return new Token(Token.Kind.OTT, token);
                }
            } catch (IllegalArgumentException e) {
                // ignored
            }

            LOG.error("Cannot parse token {}", token);
        }

        throw new IllegalArgumentException("Authorization header is invalid: it MUST start with \"Bearer\" " +
            "authentication scheme and token divided by space(s) (RFC 6750).");
    }

    public static class Token {
        Kind kind;
        String token;

        public Token(Kind kind, String token) {
            this.kind = kind;
            this.token = token;
        }

        public Kind kind() {
            return kind;
        }

        public String token() {
            return token;
        }

        public enum Kind {
            JWT,
            OTT,
            YC_IAM
        }
    }
}
