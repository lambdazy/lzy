package ai.lzy.site;

import ai.lzy.iam.clients.AuthenticateService;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.util.auth.credentials.JwtCredentials;
import ai.lzy.util.auth.exceptions.AuthException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.exceptions.HttpStatusException;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

@Singleton
public class AuthUtils {
    private static final Logger LOG = LogManager.getLogger(AuthUtils.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    SubjectServiceGrpcClient subjectService;

    @Inject
    AuthenticateService authService;

    public Subject checkCookieAndGetSubject(String userSubjectId, String sessionId) {
        try {
            return authService.authenticate(new JwtCredentials(sessionId));
        } catch (AuthException e) {
            LOG.error("Cannot auth token for user {}: ", userSubjectId, e);
            throw new HttpStatusException(HttpStatus.FORBIDDEN, "Invalid token: " + e.getMessage());
        }
    }
}
