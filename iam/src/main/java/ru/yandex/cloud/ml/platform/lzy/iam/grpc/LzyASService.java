package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.AuthPermission;
import yandex.cloud.lzy.v1.IAM.Subject;
import yandex.cloud.priv.lzy.v1.LAS.AuthorizeRequest;
import yandex.cloud.priv.lzy.v1.LzyASGrpc;

@Requires(beans = AccessClient.class)
public class LzyASService extends LzyASGrpc.LzyASImplBase {

    public static final Logger LOG = LogManager.getLogger(LzyASService.class);

    @Inject
    AccessClient accessClient;

    @Override
    public void authorize(AuthorizeRequest request, StreamObserver<Subject> responseObserver) {
        LOG.info("Authorize user:: " + request.getSubjectId() + " to resource:: " + request.getResource().getId());
        try {
            if (accessClient.hasResourcePermission(
                request.getSubjectId(), request.getResource().getId(),
                AuthPermission.fromString(request.getPermission()))) {
                responseObserver.onNext(Subject.newBuilder()
                        .setId(request.getSubjectId())
                    .build());
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(Status.PERMISSION_DENIED
                    .withDescription("Access denied to resource:: " + request.getResource().getId())
                    .asException());
            }
        } catch (AuthException e) {
            responseObserver.onError(e.status().asException());
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL.asException());
        }
    }
}
