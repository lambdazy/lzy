package ai.lzy.iam.grpc.ycclient;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.YcSubject;
import ai.lzy.util.auth.credentials.Credentials;
import ai.lzy.util.auth.credentials.YcIamCredentials;
import ai.lzy.util.auth.exceptions.AuthException;
import ai.lzy.util.auth.exceptions.AuthInvalidArgumentException;
import io.grpc.StatusRuntimeException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import yandex.cloud.auth.api.CloudAuthClient;
import yandex.cloud.auth.api.credentials.IamToken;

public class YcAuthenticateServiceGrpcClient implements AuthenticateService {
    private static final Logger LOG = LogManager.getLogger(YcAuthenticateServiceGrpcClient.class);

    private final CloudAuthClient authClient;

    public YcAuthenticateServiceGrpcClient(CloudAuthClient authClient) {
        this.authClient = authClient;
    }

    @Override
    public Subject authenticate(Credentials credentials) throws AuthException {
        if (!(credentials instanceof YcIamCredentials)) {
            LOG.error("Unexpected credentials type {}", credentials.getClass().getName());
            throw new AuthInvalidArgumentException("Unexpected credentials type " + credentials.type());
        }

        try {
            var subject = authClient.authenticate(new IamToken(credentials.token()));
            return new YcSubject(subject);
        } catch (StatusRuntimeException e) {
            throw AuthException.fromStatusRuntimeException(e);
        }
    }
}
