package ai.lzy.iam.resources.subjects;

import ai.lzy.v1.iam.IAM;

public enum CredentialsType {
    PUBLIC_KEY,
    OTT;

    public IAM.Credentials.Type toProto() {
        return switch (this) {
            case PUBLIC_KEY -> IAM.Credentials.Type.PUBLIC_KEY;
            case OTT -> IAM.Credentials.Type.OTT;
        };
    }

    public static CredentialsType fromProto(IAM.Credentials.Type type) {
        return switch (type) {
            case PUBLIC_KEY -> CredentialsType.PUBLIC_KEY;
            case OTT -> CredentialsType.OTT;
            default -> throw new IllegalArgumentException(type.name());
        };
    }
}
