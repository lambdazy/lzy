package ai.lzy.iam.resources.subjects;

import ai.lzy.v1.iam.IAM;

public enum CredentialsType {
    PUBLIC_KEY;

    public IAM.Credentials.Type toProto() {
        return switch (this) {
            case PUBLIC_KEY -> IAM.Credentials.Type.PUBLIC_KEY;
        };
    }

    public static CredentialsType fromProto(IAM.Credentials.Type type) {
        return switch (type) {
            case PUBLIC_KEY -> CredentialsType.PUBLIC_KEY;
            default -> throw new IllegalArgumentException(type.name());
        };
    }
}
