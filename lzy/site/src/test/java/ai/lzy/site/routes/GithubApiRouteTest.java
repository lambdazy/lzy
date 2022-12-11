package ai.lzy.site.routes;

import io.micronaut.http.HttpResponse;
import io.micronaut.http.annotation.*;

@Controller("github-api")
public class GithubApiRouteTest {
    public static final String TEST_USER = "test-user";

    @Get("/user")
    public HttpResponse<Auth.GitHubGetUserResponse> user(@Header String authorization) {
        if (!authorization.equals("token " + GithubRouteTest.ACCESS_TOKEN)) {
            return HttpResponse.unauthorized();
        }
        return HttpResponse.ok(new Auth.GitHubGetUserResponse(TEST_USER, "id"));
    }
}
