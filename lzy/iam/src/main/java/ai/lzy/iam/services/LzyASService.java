package ai.lzy.iam.services;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.AuthResource;
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
        var subject = ProtoConverter.to(request.getSubject());
        var resource = ProtoConverter.to(request.getResource());
        var permission = AuthPermission.fromString(request.getPermission());

        LOG.info("Authorize {} to {} for {}", subject, resource, permission);

        try {
            var requester = Objects.requireNonNull(AuthenticationContext.current()).getSubject();

            if (!hasInternalAccess(resource, requester)) {
                LOG.error("Not INTERNAL {} try authorize something::{}", requester, subject);
                throw new AuthPermissionDeniedException("");
            }

            if (accessClient.hasResourcePermission(subject, resource.resourceId(), permission)) {
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

    private boolean hasInternalAccess(AuthResource resource, ai.lzy.iam.resources.subjects.Subject requester) {
        try {
            return accessClient.hasResourcePermission(
                requester, resource.resourceId(), AuthPermission.INTERNAL_AUTHORIZE);
        } catch (AuthException e) {
            LOG.error("Failed to check permission {} to {}", requester, resource, e);
            return false;
        }
    }
}
