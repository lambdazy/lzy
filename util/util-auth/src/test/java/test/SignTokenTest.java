package test;

import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

public class SignTokenTest {
    @Test
    public void test() throws Exception {
        var keys = RsaUtils.generateRsaKeys();
        var publicKey = CredentialsUtils.readPublicKey(keys.publicKey());
        var privateKey = CredentialsUtils.readPrivateKey(keys.privateKey());

        var anyToken = UUID.randomUUID().toString();
        var tokenSignature = CredentialsUtils.signToken(anyToken, privateKey);

        Assert.assertTrue(CredentialsUtils.checkToken(publicKey, anyToken, tokenSignature));
        Assert.assertFalse(CredentialsUtils.checkToken(publicKey, anyToken + "x", tokenSignature));
        try {
            Assert.assertFalse(CredentialsUtils.checkToken(publicKey, anyToken, tokenSignature + "x"));
            Assert.fail();
        } catch (IllegalArgumentException e) {
            Assert.assertEquals("Input byte array has incorrect ending byte at 344", e.getMessage());
        }
    }
}
