package ai.lzy.site.routes;

import ai.lzy.site.routes.context.IamOnlySiteContextTests;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.HttpStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class KeysControllerTest extends IamOnlySiteContextTests {
    private Keys keys;

    @Before
    public void before() {
        keys = micronautContext().getBean(Keys.class);
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
            Assert.assertEquals(1, body.keys().size());  // Contains session key
        }

        {
            final String keyName = "keyName";
            final String publicKey = "publicKey";
            keys.add(subjectId, sessionId, new Keys.AddPublicKeyRequest(keyName, publicKey));
            HttpResponse<Keys.ListKeysResponse> listKeys = keys.list(subjectId, sessionId);
            Keys.ListKeysResponse body = listKeys.body();
            Assert.assertNotNull(body);
            Assert.assertEquals(2, body.keys().size());
            final var key = body.keys().stream().filter(k -> k.name().equals(keyName)).findFirst();
            Assert.assertTrue(key.isPresent());
            Assert.assertEquals(keyName, key.get().name());
            Assert.assertEquals(publicKey, key.get().value());

            keys.delete(subjectId, sessionId, new Keys.DeletePublicKeyRequest(keyName));
            listKeys = keys.list(subjectId, sessionId);
            body = listKeys.body();
            Assert.assertNotNull(body);
            Assert.assertEquals(1, body.keys().size());
        }
    }
}
