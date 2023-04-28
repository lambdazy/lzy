package ai.lzy.site.routes;

import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.site.AuthUtils;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.CookieValue;
import io.micronaut.http.annotation.Post;
import jakarta.inject.Inject;
import jakarta.validation.Valid;

import java.util.List;
import java.util.stream.Collectors;

@Controller("key")
public class Keys {
    @Inject
    AuthUtils authUtils;

    @Inject
    SubjectServiceGrpcClient subjectService;

    @Post("add")
    public HttpResponse<?> add(@CookieValue String userSubjectId, @CookieValue String sessionId,
                               @Valid @Body AddPublicKeyRequest request)
    {
        final Subject subject = authUtils.checkCookieAndGetSubject(userSubjectId, sessionId);
        subjectService.addCredentials(subject, SubjectCredentials.publicKey(request.keyName, request.publicKey));
        return HttpResponse.ok();
    }

    @Post("delete")
    public HttpResponse<?> delete(@CookieValue String userSubjectId, @CookieValue String sessionId,
                                  @Valid @Body DeletePublicKeyRequest request)
    {
        final Subject subject = authUtils.checkCookieAndGetSubject(userSubjectId, sessionId);
        subjectService.removeCredentials(subject, request.keyName);
        return HttpResponse.ok();
    }

    @Post("list")
    public HttpResponse<ListKeysResponse> list(@CookieValue String userSubjectId, @CookieValue String sessionId) {
        final Subject subject = authUtils.checkCookieAndGetSubject(userSubjectId, sessionId);
        return HttpResponse.ok(
            new ListKeysResponse(
                subjectService.listCredentials(subject).stream()
                    .filter(creds -> creds.type().equals(CredentialsType.PUBLIC_KEY))
                    .map(creds -> new Key(creds.name(), creds.value()))
                    .collect(Collectors.toList())
            )
        );
    }

    @Introspected
    public record AddPublicKeyRequest(
        String keyName,
        String publicKey
    ) {}

    @Introspected
    public record DeletePublicKeyRequest(
        String keyName
    ) {}

    public record Key(String name, String value) {}

    @Introspected
    public record ListKeysResponse(List<Key> keys) {}
}
