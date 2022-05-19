package ru.yandex.cloud.ml.platform.lzy.iam.grpc;

import io.grpc.stub.StreamObserver;
import jakarta.inject.Inject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AccessClient;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.SubjectService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import yandex.cloud.lzy.v1.IAM;
import yandex.cloud.priv.lzy.v1.LSS;
import yandex.cloud.priv.lzy.v1.LzySubjectServiceGrpc;

public class LzySubjectService extends LzySubjectServiceGrpc.LzySubjectServiceImplBase {
    public static final Logger LOG = LogManager.getLogger(LzySubjectService.class);


    @Inject
    private SubjectService subjectService;
    @Inject
    private AccessClient accessClient;

    @Override
    public void createSubject(LSS.CreateSubjectRequest request, StreamObserver<IAM.Subject> responseObserver) {
        try {
            Subject subject = subjectService.createSubject(
                    request.getName(),
                    request.getAuthProvider(),
                    request.getProviderUserId()
            );
            responseObserver.onNext(GrpcConverter.from(subject));
            responseObserver.onCompleted();
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        }
    }

    @Override
    public void removeSubject(LSS.RemoveSubjectRequest request, StreamObserver<LSS.RemoveSubjectResponse> responseObserver) {
        try {
            subjectService.removeSubject(GrpcConverter.to(request.getSubject()));
            responseObserver.onCompleted();
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        }
    }

    @Override
    public void addCredentials(LSS.AddCredentialsRequest request, StreamObserver<LSS.AddCredentialsResponse> responseObserver) {
        try {
            subjectService.addCredentials(
                    GrpcConverter.to(request.getSubject()),
                    request.getCredentials().getName(),
                    request.getCredentials().getCredentials(),
                    request.getCredentials().getType()
            );
            responseObserver.onCompleted();
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        }
    }

    @Override
    public void removeCredentials(LSS.RemoveCredentialsRequest request, StreamObserver<LSS.RemoveCredentialsResponse> responseObserver) {
        try {
            subjectService.removeCredentials(
                    GrpcConverter.to(request.getSubject()),
                    request.getCredentialsName()
            );
            responseObserver.onCompleted();
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        }
    }
}
