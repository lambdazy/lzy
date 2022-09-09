package ai.lzy.iam.resources.subjects;

import ai.lzy.v1.iam.IAM;

public enum AuthProvider {
    INTERNAL,
    GITHUB;

    public boolean isInternal() {
        return this == INTERNAL;
    }

    public boolean isPublic() {
        return this != INTERNAL;
    }

    public static AuthProvider fromProto(IAM.AuthProvider authProvider) {
        return switch (authProvider) {
            case INTERNAL -> AuthProvider.INTERNAL;
            case GITHUB -> AuthProvider.GITHUB;
            default -> throw new IllegalArgumentException(authProvider.name());
        };
    }

    public IAM.AuthProvider toProto() {
        return switch (this) {
            case INTERNAL -> IAM.AuthProvider.INTERNAL;
            case GITHUB -> IAM.AuthProvider.GITHUB;
        };
    }
}
