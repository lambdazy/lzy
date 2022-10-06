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

import java.io.IOException;

@Singleton
public class AuthUtils {
    private static final Logger LOG = LogManager.getLogger(AuthUtils.class);

    @Inject
    ObjectMapper objectMapper;

    @Inject
    SubjectServiceGrpcClient subjectService;

    public Subject checkCookieAndGetSubject(Cookie cookie) {
        final Subject subject = subjectService.getSubject(cookie.userId());
        final SubjectCredentials credentials = subjectService.listCredentials(subject).stream()
            .filter(creds -> creds.type() == CredentialsType.COOKIE)
            .findFirst().orElse(null);
        try {
            if (credentials == null || !objectMapper.readValue(credentials.value(), Cookie.class).equals(cookie)) {
                final String message = "Invalid cookie passed for user " + cookie.userId();
                LOG.error(message);
                throw new HttpStatusException(HttpStatus.FORBIDDEN, message);
            }
        } catch (IOException e) {
            final String message = "Failed via parsing cookie from credentials " + credentials.value();
            LOG.error(message, e);
            throw new HttpStatusException(HttpStatus.INTERNAL_SERVER_ERROR, message);
        }
        return subject;
    }
}
