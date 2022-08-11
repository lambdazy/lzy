package ai.lzy.iam.grpc.service;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import ai.lzy.iam.authorization.exceptions.AuthException;
import ai.lzy.iam.utils.GrpcConverter;
import ai.lzy.v1.iam.IAM.Subject;
import ai.lzy.v1.iam.LAS.AuthenticateRequest;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;

import java.util.Objects;

@Singleton
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
