package ai.lzy.site.routes;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.env.PropertySource;
import io.micronaut.context.env.yaml.YamlPropertySourceLoader;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpStatus;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.annotation.Client;
import jakarta.inject.Inject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;

public class AuthControllerTest {
    Auth auth;

    @Client("/")
    @Inject
    HttpClient client;

    @Before
    public void before() throws IOException {
        var props = new YamlPropertySourceLoader()
            .read("iam", new FileInputStream("../site/src/main/resources/application.yml"));
        final var ctx = ApplicationContext.run(PropertySource.of(props));
        auth = ctx.getBean(Auth.class);
    }

    @Test
    public void verifyAuthUrlTest() {
        final var response = client.toBlocking().exchange(
            HttpRequest.GET("/auth/login_url?auth_type=" + Auth.AuthType.GITHUB),
            Auth.LoginUrlResponse.class
        );
        Assert.assertEquals(HttpStatus.OK, response.getStatus());
        final Auth.LoginUrlResponse loginUrlResponse = response.body();
        Assert.assertNotNull(loginUrlResponse);
        Assert.assertTrue(loginUrlResponse.url().startsWith("https://github.com/login/oauth/authorize"));
    }
}
