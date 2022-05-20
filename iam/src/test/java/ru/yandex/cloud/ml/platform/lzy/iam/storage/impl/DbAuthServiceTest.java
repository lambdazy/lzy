package ru.yandex.cloud.ml.platform.lzy.iam.storage.impl;

import io.micronaut.context.ApplicationContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.AuthenticateService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.SubjectService;
import ru.yandex.cloud.ml.platform.lzy.iam.authorization.credentials.JwtCredentials;
import ru.yandex.cloud.ml.platform.lzy.iam.resources.subjects.Subject;
import ru.yandex.cloud.ml.platform.lzy.iam.storage.Storage;
import ru.yandex.cloud.ml.platform.lzy.iam.utils.CredentialsHelper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static org.junit.Assert.fail;

public class DbAuthServiceTest {
    public static final Logger LOG = LogManager.getLogger(DbAuthServiceTest.class);


    private ApplicationContext ctx;
    private SubjectService subjectService;
    private Storage storage;
    private AuthenticateService authenticateService;

    @Before
    public void setUp() {
        ctx = ApplicationContext.run();
        storage = ctx.getBean(Storage.class);
        subjectService = ctx.getBean(DbSubjectService.class);
        authenticateService = ctx.getBean(DbAuthService.class);
    }

    @Test
    public void validAuth() {
        String userId = "user1";
        subjectService.createSubject(userId, "", "");
        TestCred cred = null;
        try {
             cred = initCreds();
        } catch (IOException | InterruptedException e) {
            LOG.error(e);
            fail();
        }
        final Subject user = subjectService.subject(userId);
        subjectService.addCredentials(user, "testCred", cred.publicPem, "public_key");

        try {
            authenticateService.authenticate(new JwtCredentials(CredentialsHelper.buildJWT(userId, cred.privatePem)));
        } catch (IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
            LOG.error(e);
            fail();
        }
    }

    @After
    public void tearDown() {
        try (PreparedStatement st =storage.connect().prepareStatement("DROP ALL OBJECTS DELETE FILES;")) {
            st.executeUpdate();
        } catch (SQLException e) {
            LOG.error(e);
        }
        ctx.stop();
    }

    private TestCred initCreds() throws IOException, InterruptedException {
        final Path tempDirectory = Files.createTempDirectory("test-rsa-keys");
        final Path publicKeyPath = tempDirectory.resolve("public.pem");
        final Path privateKeyPath = tempDirectory.resolve("private.pem");

        final Process exec = Runtime.getRuntime()
                .exec(String.format("openssl genrsa -out %s 2048", privateKeyPath));
        exec.waitFor();
        final Process exec1 = Runtime.getRuntime()
                .exec(String.format("openssl rsa -in %s -outform PEM -pubout -out %s", privateKeyPath, publicKeyPath));
        exec1.waitFor();
        return new TestCred(readFileAsString(publicKeyPath), readFileAsString(privateKeyPath));
    }

    private String readFileAsString(Path filePath) throws IOException {
        return Files.readString(filePath);
    }

    private static class TestCred {
        private final String publicPem;
        private final String privatePem;

        private TestCred(String publicPem, String privatePem) {
            this.publicPem = publicPem;
            this.privatePem = privatePem;
        }

        public String publicPem() {
            return publicPem;
        }

        public String privatePem() {
            return privatePem;
        }
    }
}