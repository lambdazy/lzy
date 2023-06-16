package ai.lzy.iam.resources.subjects;

import ai.lzy.v1.iam.IAM;

public enum AuthProvider {
    INTERNAL,
    GITHUB,
    EXTERNAL;

    public boolean isInternal() {
        return this == INTERNAL;
    }

    public boolean isExternal() {
        return this == EXTERNAL;
    }

    public boolean allowMetadata() {
        return this == EXTERNAL;
    }

    public static AuthProvider fromProto(IAM.AuthProvider authProvider) {
        return switch (authProvider) {
            case INTERNAL -> AuthProvider.INTERNAL;
            case GITHUB -> AuthProvider.GITHUB;
            case EXTERNAL -> AuthProvider.EXTERNAL;
            default -> throw new IllegalArgumentException(authProvider.name());
        };
    }

    public IAM.AuthProvider toProto() {
        return switch (this) {
            case INTERNAL -> IAM.AuthProvider.INTERNAL;
            case GITHUB -> IAM.AuthProvider.GITHUB;
            case EXTERNAL -> IAM.AuthProvider.EXTERNAL;
        };
    }
}
