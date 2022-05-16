package ru.yandex.cloud.ml.platform.lzy.iam.utils;

import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBinding;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AccessBindingDelta;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthResource;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Whiteboard;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Workflow;
import yandex.cloud.lzy.v1.IAM;

public class GrpcConverter {

    public static AuthResource to(IAM.Resource resource) {
        switch (resource.getType()) {
            case Workflow.TYPE:
                return new Workflow(resource.getId());
            case Whiteboard.TYPE:
                return new Whiteboard(resource.getId());
            default:
                throw new RuntimeException("Unknown Resource type::" + resource.getType());
        }
    }

    public static AccessBinding to(IAM.AccessBinding accessBinding) {
        return new AccessBinding(accessBinding.getRole(), accessBinding.getSubject().getId());
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public static IAM.AccessBinding from(AccessBinding accessBinding) {
        return IAM.AccessBinding.newBuilder()
                .setRole(accessBinding.role())
                .setSubject(
                        IAM.Subject.newBuilder().setId(accessBinding.subject()).build()
                )
                .build();
    }

    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public static AccessBindingDelta to(IAM.AccessBindingDelta accessBindingDelta) {
        return new AccessBindingDelta(
                AccessBindingDelta.AccessBindingAction.valueOf(accessBindingDelta.getAction().name()),
                to(accessBindingDelta.getBinding())
        );
    }

}
