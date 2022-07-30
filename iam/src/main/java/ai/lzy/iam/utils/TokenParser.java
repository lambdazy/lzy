package ai.lzy.iam.utils;

import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TokenParser {

    private static final Pattern JWT_HEADER_PATTERN = Pattern.compile("^Bearer +[^ ]");
    private static final Pattern JWT_TOKEN_PATTERN = Pattern.compile(
            "^[A-Za-z0-9-_]+\\.[A-Za-z0-9-_]+\\.[A-Za-z0-9-_]*");
    private static final Pattern INTERNAL_HEADER_PATTERN = Pattern.compile("^Internal +[^ ]");
    private static final Pattern INTERNAL_TOKEN_PATTERN = Pattern.compile(
            "^[A-Za-z0-9-._~+\\/]+");

    public TokenParser() {
    }

    private static String parseJWTToken(String token) {
        Matcher tokenMatcher = JWT_TOKEN_PATTERN.matcher(token);
        int illegalCharPos;
        if (tokenMatcher.find()) {
            if (tokenMatcher.hitEnd()) {
                return token;
            }

            illegalCharPos = tokenMatcher.end();
            tokenMatcher.reset(token.substring(illegalCharPos));
            if (tokenMatcher.find()) {
                throw new IllegalArgumentException(
                        "JWT token encoding violates PS256. Token may contain a padding character '.' only two times");
            }
        } else {
            illegalCharPos = token.charAt(0) == '.' ? -1 : 0;
        }

        if (illegalCharPos < 0) {
            throw new IllegalArgumentException(
                    "JWT token encoding violates PS256. Token may not start with a padding character '.'.");
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "JWT token encoding violates PS256."
                                    + " Token contains an illegal character '%s' in position %s.",
                            token.charAt(illegalCharPos), illegalCharPos + 1));
        }
    }

    private static String parseInternalToken(String token) {
        Matcher tokenMatcher = INTERNAL_TOKEN_PATTERN.matcher(token);
        int illegalCharPos;
        if (tokenMatcher.find()) {
            if (tokenMatcher.hitEnd()) {
                return token;
            }

            illegalCharPos = tokenMatcher.end();
        } else {
            illegalCharPos = -1;
        }

        if (illegalCharPos < 0) {
            throw new IllegalArgumentException(
                    "Internal token encoding violates.");
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Internal token encoding violates."
                                    + " Token contains an illegal character '%s' in position %s.",
                            token.charAt(illegalCharPos), illegalCharPos + 1));
        }
    }

    public static Token parse(String header) throws IllegalArgumentException {
        Objects.requireNonNull(header, "header is null");
        Matcher jwtTokenMatcher = JWT_HEADER_PATTERN.matcher(header);
        Matcher internalTokenMatcher = INTERNAL_HEADER_PATTERN.matcher(header);
        if (jwtTokenMatcher.find()) {
            return new Token(Token.Kind.JWT_TOKEN, parseJWTToken(header.substring(jwtTokenMatcher.end() - 1)));
        } else if (internalTokenMatcher.find()) {
            return new Token(
                    Token.Kind.INTERNAL_TOKEN,
                    parseInternalToken(header.substring(jwtTokenMatcher.end() - 1))
            );
        } else {
            throw new IllegalArgumentException("Authorization header is invalid: it MUST start with \"Bearer\" "
                    + "authentication scheme and token divided by space(s) (RFC 6750).");
        }
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
            JWT_TOKEN,
            INTERNAL_TOKEN
        }
    }
}
