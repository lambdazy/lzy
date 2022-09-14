package test;

import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import org.junit.Assert;
import org.junit.Test;

public class JwtTest {
    @Test
    public void test() throws Exception {
        var keys = RsaUtils.generateRsaKeys();
        var publicKey = CredentialsUtils.readPublicKey(keys.publicKeyPath());
        var privateKey = CredentialsUtils.readPrivateKey(keys.privateKeyPath());

        var jwt = JwtUtils.buildJWT("some_user", "provider", JwtUtils.afterDays(1), privateKey);

        Assert.assertTrue(JwtUtils.checkJWT(publicKey, jwt, "some_user", "provider"));
        Assert.assertFalse(JwtUtils.checkJWT(publicKey, jwt, "Superman", "provider"));
        Assert.assertFalse(JwtUtils.checkJWT(publicKey, jwt, "some_user", "another_provider"));
    }
}
