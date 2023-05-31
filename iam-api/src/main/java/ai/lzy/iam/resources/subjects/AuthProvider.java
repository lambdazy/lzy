package ai.lzy.iam.resources.subjects;

import ai.lzy.v1.iam.IAM;

public enum AuthProvider {
    INTERNAL,
    GITHUB,
    YCDS;

    public boolean isInternal() {
        return this == INTERNAL;
    }

    public boolean allowMetadata() {
        return this == YCDS;
    }

    public static AuthProvider fromProto(IAM.AuthProvider authProvider) {
        return switch (authProvider) {
            case INTERNAL -> AuthProvider.INTERNAL;
            case GITHUB -> AuthProvider.GITHUB;
            case YCDS -> AuthProvider.YCDS;
            default -> throw new IllegalArgumentException(authProvider.name());
        };
    }

    public IAM.AuthProvider toProto() {
        return switch (this) {
            case INTERNAL -> IAM.AuthProvider.INTERNAL;
            case GITHUB -> IAM.AuthProvider.GITHUB;
            case YCDS -> IAM.AuthProvider.YCDS;
        };
    }
}
