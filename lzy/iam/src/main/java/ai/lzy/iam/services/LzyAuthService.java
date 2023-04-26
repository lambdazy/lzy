package ai.lzy.iam.services;

import ai.lzy.iam.grpc.context.AuthenticationContext;
import ai.lzy.iam.utils.ProtoConverter;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.v1.iam.IAM.Subject;
import ai.lzy.v1.iam.LAS.AuthenticateRequest;
import ai.lzy.v1.iam.LzyAuthenticateServiceGrpc;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Objects;

@Singleton
public class LzyAuthService extends LzyAuthenticateServiceGrpc.LzyAuthenticateServiceImplBase {

    public static final Logger LOG = LogManager.getLogger(LzyAuthService.class);

    @Override
    public void authenticate(AuthenticateRequest request, StreamObserver<Subject> responseObserver) {
        try {
            var currentSubject = Objects.requireNonNull(AuthenticationContext.current()).getSubject();
            LOG.debug("Authenticate subject " + currentSubject);
            responseObserver.onNext(ProtoConverter.from(currentSubject));
            responseObserver.onCompleted();
        } catch (AuthException e) {
            LOG.error("Auth exception:: {}", e.getInternalDetails());
            responseObserver.onError(e.status().asException());
        } catch (Exception e) {
            LOG.error("Internal exception:: {}", e.getMessage(), e);
            responseObserver.onError(Status.INTERNAL.asException());
        }
    }

}
