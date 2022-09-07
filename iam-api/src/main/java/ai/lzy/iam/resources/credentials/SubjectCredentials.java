package ai.lzy.iam.resources.credentials;

import ai.lzy.iam.resources.subjects.CredentialsType;

public record SubjectCredentials(
    String name,
    String value,
    CredentialsType type
) {}
