package ai.lzy.model.utils;

import ai.lzy.v1.deprecated.BackOffice;

public enum AuthProvider {
    GITHUB("github");

    public final String name;

    AuthProvider(String name) {
        this.name = name;
    }

    public static AuthProvider fromProto(BackOffice.AuthUserSessionRequest.OAuthProviders provider) {
        return switch (provider) {
            case GITHUB -> GITHUB;
            default -> throw new RuntimeException("Unrecognized option");
        };
    }

    public static AuthProvider fromString(String s) {
        if (s == null) {
            return null;
        }
        return switch (s) {
            case "github" -> GITHUB;
            default -> null;
        };
    }

    public BackOffice.AuthUserSessionRequest.OAuthProviders toProto() {
        return switch (this) {
            case GITHUB -> BackOffice.AuthUserSessionRequest.OAuthProviders.GITHUB;
            default -> BackOffice.AuthUserSessionRequest.OAuthProviders.UNRECOGNIZED;
        };
    }

    public String toString() {
        return name;
    }

}
