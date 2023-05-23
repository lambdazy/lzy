package ai.lzy.iam.utils;

import ai.lzy.iam.resources.AccessBinding;
import ai.lzy.iam.resources.AccessBindingDelta;
import ai.lzy.iam.resources.AuthResource;
import ai.lzy.iam.resources.Role;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.impl.Whiteboard;
import ai.lzy.iam.resources.impl.Workflow;
import ai.lzy.iam.resources.subjects.*;
import ai.lzy.v1.iam.IAM;
import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import jakarta.annotation.Nullable;
import yandex.cloud.priv.accessservice.v2.PAS;

import static com.google.common.base.Strings.emptyToNull;

public class ProtoConverter {

    public static AuthResource to(IAM.Resource resource) {
        return switch (resource.getType()) {
            case Workflow.TYPE -> new Workflow(resource.getId());
            case Whiteboard.TYPE -> new Whiteboard(resource.getId());
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
        return switch (subjectType) {
            case USER -> new User(subject.getId());
            case WORKER -> new Worker(subject.getId());
            case EXTERNAL_YC -> {
                try {
                    yield new YcSubject(to(subject.getExternal().unpack(PAS.Subject.class)));
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static yandex.cloud.auth.api.Subject to(PAS.Subject subject) {
        if (subject.hasServiceAccount()) {
            var serviceAccount = subject.getServiceAccount();
            return new YcServiceAccountImpl(serviceAccount.getId(), serviceAccount.getFolderId());
        }

        if (subject.hasUserAccount()) {
            var userAccount = subject.getUserAccount();
            return new YcUserAccountImpl(userAccount.getId(), emptyToNull(userAccount.getFederationId()));
        }

        throw new UnsupportedOperationException(
            "The subject must have either YcServiceAccount or YcUserAccount");
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
        SubjectType subjectType;
        Any external = null;
        if (subject instanceof User) {
            subjectType = SubjectType.USER;
        } else if (subject instanceof Worker) {
            subjectType = SubjectType.WORKER;
        } else if (subject instanceof YcSubject ycSubject) {
            subjectType = SubjectType.EXTERNAL_YC;
            external = Any.pack(from(ycSubject.provided()));
        } else {
            throw new RuntimeException("Unknown subject type " + subject.getClass());
        }
        var builder = IAM.Subject.newBuilder()
                .setId(subject.id())
                .setType(subjectType.name());
        if (external != null) {
            builder.setExternal(external);
        }
        return builder.build();
    }

    public static PAS.Subject from(yandex.cloud.auth.api.Subject subject) {
        if (subject.toId() instanceof yandex.cloud.auth.api.Subject.ServiceAccount.Id serviceAccount) {
            return PAS.Subject.newBuilder()
                .setServiceAccount(
                    PAS.Subject.ServiceAccount.newBuilder()
                        .setId(serviceAccount.getId())
                        .build())
                .build();
        } else if (subject.toId() instanceof yandex.cloud.auth.api.Subject.UserAccount.Id userAccount) {
            return PAS.Subject.newBuilder()
                .setUserAccount(
                    PAS.Subject.UserAccount.newBuilder()
                        .setId(userAccount.getId())
                        .build())
                .build();
        } else {
            throw new UnsupportedOperationException(
                "Unsupported subjectId type " + subject.toId().getClass());
        }
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


    private static final class YcServiceAccountImpl extends yandex.cloud.auth.api.Subject.ServiceAccount.Id
        implements yandex.cloud.auth.api.Subject.ServiceAccount
    {
        private final String folderId;

        private YcServiceAccountImpl(String id, String folderId) {
            super(id);
            this.folderId = folderId;
        }

        public String getFolderId() {
            return folderId;
        }

        @Override
        public Id toId() {
            return this;
        }

        @Override
        public String toString() {
            return "YcServiceAccount{id=" + getId() + ", folderId=" + getFolderId() + "}";
        }
    }

    private static final class YcUserAccountImpl extends yandex.cloud.auth.api.Subject.UserAccount.Id
        implements yandex.cloud.auth.api.Subject.UserAccount
    {
        @Nullable
        private final String federationId;

        private YcUserAccountImpl(String id, @Nullable String federationId) {
            super(id);
            this.federationId = federationId;
        }

        @Override
        public Id toId() {
            return this;
        }

        @Nullable
        @Override
        public String getFederationId() {
            return federationId;
        }

        @Override
        public String toString() {
            return "YcUserAccount{id=" + getId() +
                (getFederationId() != null ? ", federationId=" + getFederationId() : "") +
                "}";
        }
    }

    public static yandex.cloud.auth.api.Subject newYcServiceAccount(String id, String folderId) {
        return new YcServiceAccountImpl(id, folderId);
    }

    public static yandex.cloud.auth.api.Subject newYcUserAccount(String id, @Nullable String federationId) {
        return new YcUserAccountImpl(id, federationId);
    }
}
