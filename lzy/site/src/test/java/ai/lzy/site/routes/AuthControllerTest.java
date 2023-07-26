package ai.lzy.site.routes;

import ai.lzy.iam.resources.credentials.SubjectCredentials;
import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.test.IamContextImpl;
import ai.lzy.site.ServiceConfig;
import ai.lzy.site.routes.context.IamOnlySiteContextTests;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import jakarta.annotation.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AuthControllerTest extends IamOnlySiteContextTests {
    private ServiceConfig.GithubCredentials githubCredentials;

    @Before
    public void before() {
        githubCredentials = micronautContext().getBean(ServiceConfig.GithubCredentials.class);
    }

    @Test
    public void verifyLoginUrlTest() throws URISyntaxException {
        final String siteSignInUrl = "https://lzy.ai/login_url";
        final var response = auth.loginUrl(Auth.AuthType.GITHUB, siteSignInUrl, HttpRequest.GET("/auth/login"));
        Assert.assertEquals(HttpStatus.OK, response.getStatus());
        final Auth.LoginUrlResponse loginUrlResponse = response.body();
        Assert.assertNotNull(loginUrlResponse);

        Assert.assertTrue(loginUrlResponse.url().startsWith("https://github.com/login/oauth/authorize"));

        final URI loginUrl = URI.create(loginUrlResponse.url());
        final Map<String, String> query = Pattern.compile("&")
            .splitAsStream(loginUrl.getQuery())
            .map(s -> s.split("=", 2))
            .collect(Collectors.toMap(s -> s[0], s -> s[1]));
        Assert.assertEquals(3, query.size());
        Assert.assertEquals(githubCredentials.getClientId(), query.get("client_id"));
        Assert.assertEquals(siteSignInUrl, query.get("state"));
        Assert.assertTrue(query.get("redirect_uri").endsWith("/auth/code/github"));
    }

    @Test
    public void verifyGithubCodeUrlTest() {
        final String signInUrl = "https://host/signIn";
        final var response = auth.acceptGithubCode("code", signInUrl);
        Assert.assertEquals(HttpStatus.MOVED_PERMANENTLY.getCode(), response.code());
        final String location = response.header("location");
        Assert.assertNotNull(location);

        final Subject subject = getSubject(AuthProvider.GITHUB, GithubApiRouteTest.TEST_USER, SubjectType.USER);
        Assert.assertNotNull(subject);

        final Utils.ParsedCookies cookies = Utils.parseCookiesFromHeaders(response);
        Assert.assertEquals(GithubApiRouteTest.TEST_USER, cookies.userId());

        final var creds = listCredentials(subject.id());
        Assert.assertNotNull(creds);
    }

    @Nullable
    public Subject getSubject(AuthProvider provider, String providerSubjectId, SubjectType type) {
        return micronautContext().getBean(IamContextImpl.class).getSubject(provider, providerSubjectId, type);
    }

    @Nullable
    public List<SubjectCredentials> listCredentials(String subjectId) {
        return micronautContext().getBean(IamContextImpl.class).listCredentials(subjectId);
    }
}
