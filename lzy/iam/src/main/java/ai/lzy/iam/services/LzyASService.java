package ai.lzy.iam.services;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.storage.impl.DbAccessClient;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import ai.lzy.v1.iam.IAM.Subject;
import ai.lzy.v1.iam.LACS.AuthorizeRequest;
import ai.lzy.v1.iam.LzyAccessServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.micronaut.context.annotation.Requires;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

@Singleton
@Requires(beans = DbAccessClient.class)
public class LzyASService extends LzyAccessServiceGrpc.LzyAccessServiceImplBase {

    public static final Logger LOG = LogManager.getLogger(LzyASService.class);

    @Inject
    private DbAccessClient accessClient;

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
                    ProtoConverter.to(request.getSubject()), request.getResource().getId(),
                    AuthPermission.fromString(request.getPermission())))
            {
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
            LOG.error("Auth exception:: {}", e.getInternalDetails());
            responseObserver.onError(e.status().asException());
        } catch (Exception e) {
            LOG.error("Internal exception:: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.asException());
        }
    }

    private boolean notInternalAccess(ai.lzy.iam.resources.subjects.Subject currentSubject) {
        return !accessClient.hasResourcePermission(currentSubject,
                Root.INSTANCE.resourceId(),
                AuthPermission.INTERNAL_AUTHORIZE);
    }

}
