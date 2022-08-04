package ai.lzy.iam.grpc.service;

import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.authorization.exceptions.AuthPermissionDeniedException;
import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.resources.impl.Root;
import ai.lzy.iam.resources.subjects.SubjectType;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.iam.resources.AuthPermission;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.storage.impl.DbAccessClient;
import ai.lzy.iam.storage.impl.DbSubjectService;
import ai.lzy.iam.utils.GrpcConverter;
import ai.lzy.v1.iam.IAM;
import ai.lzy.v1.iam.LSS;
import ai.lzy.v1.iam.LzySubjectServiceGrpc;

import java.util.Objects;

@Singleton
public class LzySubjectService extends LzySubjectServiceGrpc.LzySubjectServiceImplBase {
    public static final Logger LOG = LogManager.getLogger(LzySubjectService.class);

    @Inject
    private DbSubjectService subjectService;
    @Inject
    private DbAccessClient accessClient;

    @Override
    public void createSubject(LSS.CreateSubjectRequest request, StreamObserver<IAM.Subject> responseObserver) {
        try {
            if (internalAccess()) {
                Subject subject = subjectService.createSubject(
                        request.getName(),
                        request.getAuthProvider(),
                        request.getProviderSubjectId(),
                        SubjectType.valueOf(request.getUserType())
                );
                responseObserver.onNext(GrpcConverter.from(subject));
                responseObserver.onCompleted();
            }
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        }
    }

    @Override
    public void removeSubject(
            LSS.RemoveSubjectRequest request,
            StreamObserver<LSS.RemoveSubjectResponse> responseObserver) {
        try {
            if (internalAccess()) {
                subjectService.removeSubject(GrpcConverter.to(request.getSubject()));
                responseObserver.onCompleted();
            }
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        }
    }

    @Override
    public void addCredentials(
            LSS.AddCredentialsRequest request,
            StreamObserver<LSS.AddCredentialsResponse> responseObserver) {
        try {
            if (internalAccess()) {
                subjectService.addCredentials(
                        GrpcConverter.to(request.getSubject()),
                        request.getCredentials().getName(),
                        request.getCredentials().getCredentials(),
                        request.getCredentials().getType()
                );
                responseObserver.onCompleted();
            }
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        }
    }

    @Override
    public void removeCredentials(
            LSS.RemoveCredentialsRequest request,
            StreamObserver<LSS.RemoveCredentialsResponse> responseObserver) {
        try {
            if (internalAccess()) {
                subjectService.removeCredentials(
                        GrpcConverter.to(request.getSubject()),
                        request.getCredentialsName()
                );
                responseObserver.onCompleted();
            }
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        }
    }

    private boolean internalAccess() {
        var currentSubject = Objects.requireNonNull(AuthenticationContext.current()).getSubject();
        if (!accessClient.hasResourcePermission(currentSubject,
                new Root().resourceId(),
                AuthPermission.INTERNAL_AUTHORIZE)) {
            LOG.error("Not INTERNAL user::{} try to create subjects", currentSubject.id());
            throw new AuthPermissionDeniedException("");
        }
        return true;
    }
}
