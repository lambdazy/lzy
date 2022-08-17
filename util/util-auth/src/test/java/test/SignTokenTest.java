package test;

import static ai.lzy.util.auth.credentials.RsaUtils.generateRsaKeys;

import ai.lzy.util.auth.credentials.CredentialsUtils;
import ai.lzy.util.auth.credentials.RsaUtils;
import ai.lzy.util.auth.credentials.JwtUtils;
import java.io.FileReader;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class SignTokenTest {
    @Test
    public void test() throws Exception {
        final RsaUtils.Keys keys = generateRsaKeys();

        final UUID terminalToken = UUID.randomUUID();
        try (FileReader keyReader = new FileReader(keys.privateKeyPath().toFile())) {
            String tokenSignature = CredentialsUtils.signToken(terminalToken, keyReader);
            try (FileReader reader = new FileReader(keys.publicKeyPath().toFile())) {
                Assert.assertTrue(CredentialsUtils.checkToken(reader, terminalToken.toString(), tokenSignature));
            }
        }
        try (FileReader keyReader = new FileReader(keys.privateKeyPath().toFile())) {
            String jwt = JwtUtils.buildJWT("some_user", keyReader);
            try (FileReader reader = new FileReader(keys.publicKeyPath().toFile())) {
                Assert.assertTrue(JwtUtils.checkJWT(reader, jwt, "some_user"));
            }
        }
    }
}
