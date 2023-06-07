package ai.lzy.site.routes;

import ai.lzy.iam.resources.subjects.AuthProvider;
import ai.lzy.iam.resources.subjects.Subject;
import ai.lzy.iam.resources.subjects.SubjectType;
import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import ai.lzy.site.ServiceConfig;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.runtime.server.EmbeddedServer;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AuthControllerTest extends BaseTestWithIam {
    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });

    private ApplicationContext context;
    private EmbeddedServer server = ApplicationContext.run(EmbeddedServer.class);

    private Auth auth;
    private ServiceConfig.GithubCredentials githubCredentials;

    @Before
    public void before() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));
        context = server.getApplicationContext();
        var config = context.getBean(ServiceConfig.class);
        config.getIam().setAddress("localhost:" + super.getPort());
        auth = context.getBean(Auth.class);
        githubCredentials = context.getBean(ServiceConfig.GithubCredentials.class);
        server = context.getBean(EmbeddedServer.class);
    }

    @After
    public void after() {
        super.after();
        server.stop();
        context.stop();
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
}
