package ru.yandex.cloud.ml.platform.lzy.iam.grpc.service;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.exceptions.AuthException;
import ru.yandex.cloud.ml.platform.lzy.iam.grpc.context.AuthenticationContext;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.GrpcConverter;
import yandex.cloud.lzy.v1.IAM.Subject;
import yandex.cloud.lzy.v1.LAS.AuthenticateRequest;
import yandex.cloud.lzy.v1.LzyAuthenticateServiceGrpc;

import java.util.Objects;

public class LzyAuthService extends LzyAuthenticateServiceGrpc.LzyAuthenticateServiceImplBase {

    public static final Logger LOG = LogManager.getLogger(LzyAuthService.class);

    @Override
    public void authenticate(AuthenticateRequest request, StreamObserver<Subject> responseObserver) {
        LOG.info("Authenticate user");
        try {
            var currentSubject = Objects.requireNonNull(AuthenticationContext.current()).getSubject();
            responseObserver.onNext(GrpcConverter.from(currentSubject));
            responseObserver.onCompleted();
        } catch (AuthException e) {
            LOG.error("Auth exception::", e);
            responseObserver.onError(e.status().asException());
        } catch (Exception e) {
            LOG.error("Internal exception::", e);
            responseObserver.onError(Status.INTERNAL.asException());
        }
    }

}
