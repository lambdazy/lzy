package ai.lzy.model.utils;

import ai.lzy.v1.BackOffice;

public enum AuthProviders {
    GITHUB("github");

    public final String name;

    AuthProviders(String name) {
        this.name = name;
    }

    public static AuthProviders fromGrpcMessage(BackOffice.AuthUserSessionRequest.OAuthProviders provider) {
        switch (provider) {
            case GITHUB:
                return GITHUB;
            default:
                throw new RuntimeException("Unrecognized option");
        }
    }

    public static AuthProviders fromString(String s) {
        if (s == null) {
            return null;
        }
        switch (s) {
            case "github":
                return GITHUB;
            default:
                return null;
        }
    }

    public BackOffice.AuthUserSessionRequest.OAuthProviders toGrpcMessage() {
        switch (this) {
            case GITHUB:
                return BackOffice.AuthUserSessionRequest.OAuthProviders.GITHUB;
            default:
                return BackOffice.AuthUserSessionRequest.OAuthProviders.UNRECOGNIZED;
        }
    }

    public String toString() {
        return name;
    }

}
