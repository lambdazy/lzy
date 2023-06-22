package ai.lzy.iam.utils;

import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.resources.impl.Whiteboard;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.v1.iam.IAM;

public class ProtoConverter {

    public static AuthResource to(IAM.Resource resource) {
        return switch (resource.getType()) {
            case Workflow.TYPE -> new Workflow(resource.getId());
            case Whiteboard.TYPE -> new Whiteboard(resource.getId());
            case Root.TYPE -> Root.INSTANCE;
            default -> throw new RuntimeException("Unknown Resource type::" + resource.getType());
        };
    }

    public static AccessBinding to(IAM.AccessBinding accessBinding) {
        return new AccessBinding(Role.of(accessBinding.getRole()), to(accessBinding.getSubject()));
    }

    public static AccessBindingDelta to(IAM.AccessBindingDelta accessBindingDelta) {
        return new AccessBindingDelta(
                AccessBindingDelta.AccessBindingAction.valueOf(accessBindingDelta.getAction().name()),
                to(accessBindingDelta.getBinding())
        );
    }

    public static Subject to(IAM.Subject subject) {
        var subjectType = SubjectType.valueOf(subject.getType());
        var authProvider = AuthProvider.fromProto(subject.getAuthProvider());
        return Subject.of(subject.getId(), subjectType, authProvider, subject.getProviderId(), subject.getMetaMap());
    }

    public static SubjectCredentials to(IAM.Credentials credentials) {
        return new SubjectCredentials(
                credentials.getName(),
                credentials.getCredentials(),
                CredentialsType.fromProto(credentials.getType()),
                credentials.hasExpiredAt()
                    ? ai.lzy.util.grpc.ProtoConverter.fromProto(credentials.getExpiredAt())
                    : null
        );
    }

    public static IAM.Resource from(AuthResource resource) {
        return IAM.Resource.newBuilder()
                .setId(resource.resourceId())
                .setType(resource.type())
                .build();
    }

    public static IAM.AccessBinding from(AccessBinding accessBinding) {
        return IAM.AccessBinding.newBuilder()
                .setRole(accessBinding.role().value())
                .setSubject(from(accessBinding.subject()))
                .build();
    }
    public static IAM.AccessBindingDelta from(AccessBindingDelta accessBinding) {
        return IAM.AccessBindingDelta.newBuilder()
                .setAction(IAM.AccessBindingAction.valueOf(accessBinding.action().name()))
                .setBinding(from(accessBinding.binding()))
                .build();
    }

    public static IAM.Subject from(Subject subject) {
        return IAM.Subject.newBuilder()
            .setId(subject.id())
            .setType(subject.type().name())
            .setAuthProvider(subject.provider().toProto())
            .setProviderId(subject.providerId())
            .putAllMeta(subject.meta())
            .build();
    }

    public static IAM.Credentials from(SubjectCredentials credentials) {
        var builder = IAM.Credentials.newBuilder()
                .setName(credentials.name())
                .setCredentials(credentials.value())
                .setType(credentials.type().toProto());

        if (credentials.expiredAt() != null) {
            builder.setExpiredAt(ai.lzy.util.grpc.ProtoConverter.toProto(credentials.expiredAt()));
        }

        return builder.build();
    }
}
