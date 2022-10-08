package ai.lzy.site.routes;

import ai.lzy.iam.test.BaseTestWithIam;
import ai.lzy.model.db.test.DatabaseTestUtils;
import io.micronaut.context.ApplicationContext;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import io.micronaut.runtime.server.EmbeddedServer;
import io.micronaut.test.extensions.junit5.annotation.MicronautTest;
import io.zonky.test.db.postgres.junit.EmbeddedPostgresRules;
import io.zonky.test.db.postgres.junit.PreparedDbRule;
import org.junit.*;

import java.io.IOException;

@MicronautTest
public class KeysControllerTest extends BaseTestWithIam {
    @Rule
    public PreparedDbRule iamDb = EmbeddedPostgresRules.preparedDatabase(ds -> {
    });

    private ApplicationContext context;
    private EmbeddedServer server = ApplicationContext.run(EmbeddedServer.class);

    private Auth auth;
    private Keys keys;

    @Before
    public void before() throws IOException {
        super.setUp(DatabaseTestUtils.preparePostgresConfig("iam", iamDb.getConnectionInfo()));
        context = server.getApplicationContext();
        auth = context.getBean(Auth.class);
        keys = context.getBean(Keys.class);
        server = context.getBean(EmbeddedServer.class);
    }

    @After
    public void after() {
        super.after();
        server.stop();
        context.stop();
    }

    @Test
    public void addDeleteListTest() {
        final String signInUrl = "https://host/signIn";
        final var response = auth.acceptGithubCode("code", signInUrl);
        Assert.assertEquals(HttpStatus.MOVED_PERMANENTLY.getCode(), response.code());
        final Utils.ParsedCookies cookies = Utils.parseCookiesFromHeaders(response);

        final String sessionId = cookies.sessionId();
        final String subjectId = cookies.userSubjectId();
        {
            final HttpResponse<Keys.ListKeysResponse> listKeys = keys.list(subjectId, sessionId);
            final Keys.ListKeysResponse body = listKeys.body();
            Assert.assertNotNull(body);
            Assert.assertEquals(0, body.keys().size());
        }

        {
            final String keyName = "keyName";
            final String publicKey = "publicKey";
            keys.add(subjectId, sessionId, new Keys.AddPublicKeyRequest(keyName, publicKey));
            HttpResponse<Keys.ListKeysResponse> listKeys = keys.list(subjectId, sessionId);
            Keys.ListKeysResponse body = listKeys.body();
            Assert.assertNotNull(body);
            Assert.assertEquals(1, body.keys().size());
            final Keys.Key key = body.keys().get(0);
            Assert.assertEquals(keyName, key.name());
            Assert.assertEquals(publicKey, key.value());

            keys.delete(subjectId, sessionId, new Keys.DeletePublicKeyRequest(keyName));
            listKeys = keys.list(subjectId, sessionId);
            body = listKeys.body();
            Assert.assertNotNull(body);
            Assert.assertEquals(0, body.keys().size());
        }
    }
}
