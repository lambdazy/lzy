package ai.lzy.site;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
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

    public Subject checkCookieAndGetSubject(String userSubjectId, String sessionId) {
        final Subject subject = subjectService.getSubject(userSubjectId);
        final SubjectCredentials credentials = subjectService.listCredentials(subject).stream()
            .filter(creds -> creds.type() == CredentialsType.COOKIE)
            .findFirst().orElse(null);
        if (credentials == null || !credentials.value().equals(sessionId)) {
            final String message = "Invalid cookie passed for user " + userSubjectId;
            LOG.error(message);
            throw new HttpStatusException(HttpStatus.FORBIDDEN, message);
        }
        return subject;
    }
}
