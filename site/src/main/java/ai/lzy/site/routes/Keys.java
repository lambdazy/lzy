package ai.lzy.site.routes;

import ai.lzy.iam.grpc.client.AuthenticateServiceGrpcClient;
import ai.lzy.iam.grpc.client.SubjectServiceGrpcClient;
import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.CredentialsType;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.site.AuthUtils;
import ai.lzy.site.Cookie;
import io.micronaut.core.annotation.Introspected;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import jakarta.inject.Inject;

import java.util.List;
import java.util.stream.Collectors;
import javax.validation.Valid;

@Controller("key")
public class Keys {
    @Inject
    AuthUtils authUtils;

    @Inject
    SubjectServiceGrpcClient subjectService;
    @Inject
    AuthenticateServiceGrpcClient authenticateService;

    @Post("add")
    public HttpResponse<?> add(@Valid @Body AddPublicKeyRequest request) {
        final Subject subject = authUtils.checkCookieAndGetSubject(request.cookie);
        subjectService.addCredentials(subject, SubjectCredentials.publicKey(request.keyName, request.publicKey));
        return HttpResponse.ok();
    }

    @Post("delete")
    public HttpResponse<?> delete(@Valid @Body DeletePublicKeyRequest request) {
        final Subject subject = authUtils.checkCookieAndGetSubject(request.cookie);
        subjectService.removeCredentials(subject, request.keyName);
        return HttpResponse.ok();
    }

    @Post("list")
    public HttpResponse<ListKeysResponse> list(@Valid @Body ListKeysRequest request) {
        final Subject subject = authUtils.checkCookieAndGetSubject(request.cookie);
        return HttpResponse.ok(
            new ListKeysResponse(
                subjectService.listCredentials(subject).stream()
                    .filter(creds -> creds.type().equals(CredentialsType.PUBLIC_KEY))
                    .map(SubjectCredentials::name)
                    .collect(Collectors.toList())
            )
        );
    }

    @Introspected
    public record AddPublicKeyRequest(
        Cookie cookie,
        String keyName,
        String publicKey
    )
    {
    }

    @Introspected
    public record DeletePublicKeyRequest(
        Cookie cookie,
        String keyName
    )
    {
    }

    @Introspected
    public record ListKeysRequest(Cookie cookie) {
    }

    @Introspected
    public record ListKeysResponse(List<String> keyNames) {
    }
}
