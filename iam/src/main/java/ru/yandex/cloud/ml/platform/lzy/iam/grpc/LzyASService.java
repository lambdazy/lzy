package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthPermissionDeniedException;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.context.AuthenticationContext;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.impl.Root;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import yandex.cloud.lzy.v1.IAM.Subject;
import yandex.cloud.priv.lzy.v1.LAS.AuthorizeRequest;
import yandex.cloud.priv.lzy.v1.LzyAccessServiceGrpc;

import java.util.Objects;

@Requires(beans = AccessClient.class)
public class LzyASService extends LzyAccessServiceGrpc.LzyAccessServiceImplBase {

    public static final Logger LOG = LogManager.getLogger(LzyASService.class);

    @Inject
    private AccessClient accessClient;

    @Override
    public void authorize(AuthorizeRequest request, StreamObserver<Subject> responseObserver) {
        LOG.info("Authorize user::{}  to resource:: {}", request.getSubject().getId(), request.getResource().getId());
        try {
            var currentSubject = Objects.requireNonNull(AuthenticationContext.current()).getSubject();
            if (notInternalAccess(currentSubject)) {
                LOG.error("Not INTERNAL user::{} try authorize something::{}",
                        currentSubject.id(), request.getSubject().getId());
                throw new AuthPermissionDeniedException("");
            }
            if (accessClient.hasResourcePermission(
                    GrpcConverter.to(request.getSubject()), request.getResource().getId(),
                    AuthPermission.fromString(request.getPermission()))) {
                responseObserver.onNext(Subject.newBuilder()
                        .setId(request.getSubject().getId())
                        .build());
                responseObserver.onCompleted();
            } else {
                LOG.error("Access denied to resource::{}", request.getResource().getId());
                responseObserver.onError(Status.PERMISSION_DENIED
                        .withDescription("Access denied to resource:: " + request.getResource().getId())
                        .asException());
            }
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        } catch (Exception e) {
            LOG.error("Internal exception::", e);
            responseObserver.onError(Status.INTERNAL.asException());
        }
    }

    private boolean notInternalAccess(ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject currentSubject) {
        return !accessClient.hasResourcePermission(currentSubject,
                new Root().resourceId(),
                AuthPermission.INTERNAL_AUTHORIZE);
    }

}
