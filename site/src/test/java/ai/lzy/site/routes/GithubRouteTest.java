package ai.lzy.site.routes;

import ai.lzy.site.ServiceConfig;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.Body;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Post;
import jakarta.inject.Inject;

import java.util.Objects;

@Controller("github")
public class GithubRouteTest {
    public static final String ACCESS_TOKEN = "accessToken";

    @Inject
    public ServiceConfig.GithubCredentials githubCredentials;

    @Post("/login/oauth/access_token")
    public HttpResponse<Auth.GithubAccessTokenResponse> accessToken(@Body Auth.GithubAccessTokenRequest request) {
        if (!Objects.equals(request.clientId(), githubCredentials.getClientId()) ||
            !Objects.equals(request.clientSecret(), githubCredentials.getClientSecret()))
        {
            return HttpResponse.unauthorized();
        }
        return HttpResponse.ok(new Auth.GithubAccessTokenResponse(ACCESS_TOKEN, "scope", "tokenType"));
    }
}
