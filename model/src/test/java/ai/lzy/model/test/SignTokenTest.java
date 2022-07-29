package ai.lzy.model.test;

import static ai.lzy.model.utils.JwtCredentials.generateRsaKeys;

import ai.lzy.model.utils.Credentials;
import ai.lzy.model.utils.JwtCredentials;
import java.io.FileReader;
import java.util.UUID;
import org.junit.Assert;
import org.junit.Test;

public class SignTokenTest {
    @Test
    public void test() throws Exception {
        final JwtCredentials.Keys keys = generateRsaKeys();

        final UUID terminalToken = UUID.randomUUID();
        try (FileReader keyReader = new FileReader(keys.privateKeyPath().toFile())) {
            String tokenSignature = Credentials.signToken(terminalToken, keyReader);
            try (FileReader reader = new FileReader(keys.publicKeyPath().toFile())) {
                Assert.assertTrue(Credentials.checkToken(reader, terminalToken.toString(), tokenSignature));
            }
        }
        try (FileReader keyReader = new FileReader(keys.privateKeyPath().toFile())) {
            String jwt = JwtCredentials.buildJWT("some_user", keyReader);
            try (FileReader reader = new FileReader(keys.publicKeyPath().toFile())) {
                Assert.assertTrue(JwtCredentials.checkJWT(reader, jwt, "some_user"));
            }
        }
    }
}
