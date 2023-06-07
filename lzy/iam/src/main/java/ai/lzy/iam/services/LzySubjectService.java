package ai.lzy.iam.services;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.storage.impl.DbAccessClient;
import ai.lzy.iam.storage.impl.DbSubjectService;
import ai.lzy.iam.utils.IdempotencyUtils;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthPermissionDeniedException;
import ai.lzy.v1.iam.IAM;
import ai.lzy.v1.iam.LSS;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

@Singleton
public class LzySubjectService extends LzySubjectServiceGrpc.LzySubjectServiceImplBase {
    public static final Logger LOG = LogManager.getLogger(LzySubjectService.class);

    @Inject
    private DbSubjectService subjectService;
    @Inject
    private DbAccessClient accessClient;

    @Override
    public void createSubject(LSS.CreateSubjectRequest request, StreamObserver<IAM.Subject> response) {
        AuthProvider authProvider;
        try {
            authProvider = AuthProvider.fromProto(request.getAuthProvider());
        } catch (Exception e) {
            LOG.error("Invalid auth provider {}", request.getAuthProvider());
            response.onError(Status.INVALID_ARGUMENT.withDescription("Invalid Auth Provider").asException());
            return;
        }

        try {
            if (internalAccess()) {
                Subject subject = subjectService.createSubject(
                    authProvider,
                    request.getProviderSubjectId(),
                    SubjectType.valueOf(request.getType()),
                    request.getCredentialsList().stream()
                        .map(ProtoConverter::to)
                        .toList(),
                    IdempotencyUtils.md5(request)
                );

                response.onNext(ProtoConverter.from(subject));
                response.onCompleted();
                return;
            }
            response.onError(Status.UNAUTHENTICATED.asException());
        } catch (AuthException e) {
            LOG.error("LzySubjectService::createSubject exception {}: {}",
                e.getClass().getSimpleName(), e.getInternalDetails());
            response.onError(e.status().asException());
        }
    }

    @Override
    public void removeSubject(LSS.RemoveSubjectRequest request, StreamObserver<LSS.RemoveSubjectResponse> response) {
        try {
            if (internalAccess()) {
                subjectService.removeSubject(request.getSubjectId());
                response.onNext(LSS.RemoveSubjectResponse.getDefaultInstance());
                response.onCompleted();
                return;
            }
            response.onError(Status.UNAUTHENTICATED.asException());
        } catch (AuthException e) {
            LOG.error("LzySubjectService::removeSubject exception {}: {}",
                e.getClass().getSimpleName(), e.getInternalDetails());
            response.onError(e.status().asException());
        }
    }

    @Override
    public void getSubject(LSS.GetSubjectRequest request, StreamObserver<IAM.Subject> response) {
        try {
            if (internalAccess()) {
                Subject subject = subjectService.subject(request.getSubjectId());
                response.onNext(ProtoConverter.from(subject));
                response.onCompleted();
            }
        } catch (AuthException e) {
            LOG.error("LzySubjectService::getSubject exception {}: {}",
                e.getClass().getSimpleName(), e.getInternalDetails());
            response.onError(e.status().asException());
        }
    }

    @Override
    public void addCredentials(LSS.AddCredentialsRequest request, StreamObserver<LSS.AddCredentialsResponse> response) {
        try {
            if (internalAccess()) {
                subjectService.addCredentials(
                    request.getSubjectId(),
                    ProtoConverter.to(request.getCredentials()));
                response.onNext(LSS.AddCredentialsResponse.getDefaultInstance());
                response.onCompleted();
                return;
            }
            response.onError(Status.UNAUTHENTICATED.asException());
        } catch (AuthException e) {
            LOG.error("LzySubjectService::addCredentials exception {}: {}",
                e.getClass().getSimpleName(), e.getInternalDetails());
            response.onError(e.status().asException());
        }
    }

    @Override
    public void removeCredentials(LSS.RemoveCredentialsRequest request,
                                  StreamObserver<LSS.RemoveCredentialsResponse> response)
    {
        try {
            if (internalAccess()) {
                subjectService.removeCredentials(request.getSubjectId(), request.getCredentialsName());
                response.onNext(LSS.RemoveCredentialsResponse.getDefaultInstance());
                response.onCompleted();
                return;
            }
            response.onError(Status.UNAUTHENTICATED.asException());
        } catch (AuthException e) {
            LOG.error("LzySubjectService::removeCredentials exception {}: {}",
                e.getClass().getSimpleName(), e.getInternalDetails());
            response.onError(e.status().asException());
        }
    }

    @Override
    public void listCredentials(LSS.ListCredentialsRequest request,
                                StreamObserver<LSS.ListCredentialsResponse> response)
    {
        try {
            if (internalAccess()) {
                var subjectCredentials = subjectService.listCredentials(request.getSubjectId());

                response.onNext(
                    LSS.ListCredentialsResponse.newBuilder()
                        .addAllCredentialsList(
                            subjectCredentials.stream()
                                .map(ProtoConverter::from)
                                .toList())
                        .build());
                response.onCompleted();
            }
        } catch (AuthException e) {
            LOG.error("LzySubjectService::listCredentials exception {}: {}",
                e.getClass().getSimpleName(), e.getInternalDetails());
            response.onError(e.status().asException());
        }
    }

    @Override
    public void findSubject(LSS.FindSubjectRequest request, StreamObserver<LSS.FindSubjectResponse> responseObserver) {
        LOG.info("Finding subject with provider_user_id=<{}>, auth_provider=<{}>, subject_type=<{}>",
            request.getProviderUserId(), request.getAuthProvider(), request.getSubjectType());
        try {
            if (internalAccess()) {
                var subject = subjectService.findSubject(
                    request.getProviderUserId(),
                    AuthProvider.fromProto(request.getAuthProvider()),
                    SubjectType.valueOf(request.getSubjectType()));

                if (subject == null) {
                    responseObserver.onError(Status.NOT_FOUND.withDescription("Subject not found").asException());
                    return;
                }

                responseObserver.onNext(
                    LSS.FindSubjectResponse.newBuilder()
                        .setSubject(ProtoConverter.from(subject))
                        .build()
                );
                responseObserver.onCompleted();
            }
        } catch (AuthException e) {
            LOG.error("LzySubjectService::listCredentials exception {}: {}",
                e.getClass().getSimpleName(), e.getInternalDetails());
            responseObserver.onError(e.status().asException());
        }
    }

    private boolean internalAccess() {
        var currentSubject = Objects.requireNonNull(AuthenticationContext.current()).getSubject();

        if (!accessClient.hasResourcePermission(
                currentSubject, Root.INSTANCE.resourceId(), AuthPermission.INTERNAL_AUTHORIZE))
        {
            LOG.error("Not INTERNAL user::{} try to create subjects", currentSubject.id());
            throw new AuthPermissionDeniedException("");
        }

        return true;
    }
}
